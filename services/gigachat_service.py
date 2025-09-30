import os
import json
import re
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class GigaChatService:
    def __init__(self):
        try:
            credentials = os.getenv("GIGACHAT_CREDENTIALS")

            if not credentials:
                raise ValueError("GIGACHAT_CREDENTIALS не установлен")

            from langchain_gigachat.chat_models import GigaChat
            from langchain_core.prompts import ChatPromptTemplate
            from langchain_core.output_parsers import StrOutputParser

            self.model = GigaChat(
                model="GigaChat-2-Max",
                verify_ssl_certs=False,
                credentials=credentials,
                timeout=120
            )
            self.parser = StrOutputParser()
            logger.info("✅ GigaChat initialized successfully")

        except Exception as e:
            logger.error(f"❌ Failed to initialize GigaChat: {e}")
            raise

    def analyze_contract(self, contract_text: str, law_articles: str, law_type: str) -> Dict[str, Any]:

        logger.info(f"🔧 Starting GigaChat analysis for {law_type}")

        prompt_template = """
        Ты - эксперт по государственным закупкам. Проанализируй контракт на соответствие законодательству {law_type}.

        ДОСТУПНЫЕ СТАТЬИ ЗАКОНА:
        {law_articles}

        КОНТРАКТ ДЛЯ АНАЛИЗА:
        {contract_text}

        ИНСТРУКЦИЯ ДЛЯ АНАЛИЗА:
        1. Внимательно изучи предоставленные статьи закона
        2. Проверь контракт на соответствие конкретным статьям
        3. При выявлении нарушений указывай ТОЧНЫЕ номера статей (например: "Статья 34", "часть 2 статьи 95")
        4. Формулируй конкретные рекомендации по исправлению
        5. Будь точным и избегай общих фраз

        ФОРМАТ ОТВЕТА (строго в JSON):
        {{
            "compliance_status": "соответствует|частично соответствует|не соответствует",
            "summary": "краткое общее заключение",
            "issues": [
                {{
                    "article": "конкретный номер статьи и части (например: 'Статья 34 часть 1')",
                    "issue": "конкретное описание нарушения с указанием пункта контракта",
                    "recommendation": "конкретная рекомендация по исправлению"
                }}
            ]
        }}

        ВАЖНО: 
        - Указывай только реальные номера статей из предоставленного закона
        - Не придумывай несуществующие статьи
        - Будь максимально конкретным в описании проблем
        """

        try:
            from langchain_core.prompts import ChatPromptTemplate
            from langchain_core.output_parsers import StrOutputParser

            prompt = ChatPromptTemplate.from_template(prompt_template)
            chain = prompt | self.model | self.parser

            response = chain.invoke({
                "law_type": law_type.upper(),
                "law_articles": law_articles[:12000],  # Увеличил лимит
                "contract_text": contract_text[:10000]  # Увеличил лимит
            })

            logger.info(f"🔧 GigaChat raw response: {response[:200]}...")
            return self._parse_response(response)

        except Exception as e:
            logger.error(f"❌ GigaChat analysis error: {e}")
            return {
                "compliance_status": "ошибка анализа",
                "issues": [{
                    "article": "системная ошибка",
                    "issue": f"Ошибка при анализе: {str(e)}",
                    "recommendation": "Повторите попытку или проверьте подключение"
                }],
                "summary": f"Ошибка анализа: {str(e)}"
            }

    def _parse_response(self, response: str) -> Dict[str, Any]:

        try:

            cleaned_response = response.strip()


            start = cleaned_response.find('{')
            end = cleaned_response.rfind('}') + 1

            if start != -1 and end != 0:
                json_str = cleaned_response[start:end]
                logger.info(f"🔧 Found JSON: {json_str[:100]}...")
                parsed = json.loads(json_str)


                if not isinstance(parsed, dict):
                    raise ValueError("Ответ не является словарем")

                if 'compliance_status' not in parsed:
                    parsed['compliance_status'] = 'не определен'

                if 'issues' not in parsed or not isinstance(parsed['issues'], list):
                    parsed['issues'] = []

                if 'summary' not in parsed:
                    parsed['summary'] = 'Заключение не предоставлено'


                for issue in parsed['issues']:
                    if 'article' in issue:

                        issue['article'] = re.sub(r'[Сс]татья\s*', 'Статья ', issue['article'])
                        issue['article'] = re.sub(r'\s+', ' ', issue['article']).strip()

                logger.info("✅ JSON parsed and validated successfully")
                return parsed
            else:
                logger.warning("❌ No JSON found in response, using fallback")
                raise ValueError("JSON не найден в ответе")

        except Exception as e:
            logger.error(f"❌ JSON parse error: {e}")


            fallback_issues = []
            if "статья" in response.lower():
                # Пытаемся найти упоминания статей
                article_matches = re.finditer(r'[Сс]татья\s+(\d+(?:\.\d+)*[\s\S]*?)(?=[Сс]татья|$)', response)
                for match in article_matches:
                    fallback_issues.append({
                        "article": f"Статья {match.group(1).strip()}",
                        "issue": "Нарушение выявлено при анализе",
                        "recommendation": "Требуется ручная проверка"
                    })

            return {
                "compliance_status": "требует ручной проверки",
                "issues": fallback_issues if fallback_issues else [{
                    "article": "формат ответа",
                    "issue": "Некорректный формат ответа от AI",
                    "recommendation": "Проверьте контракт вручную"
                }],
                "summary": response[:1000] if response else "Не удалось получить корректный ответ"
            }