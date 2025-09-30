from datetime import datetime

class LawArticle:
    def __init__(self, law_type, article_number, title, content, keywords):
        self.law_type = law_type  # '44_fz' или '223_fz'
        self.article_number = article_number
        self.title = title
        self.content = content
        self.keywords = keywords
        self.created_at = datetime.now()

class ContractAnalysis:
    def __init__(self, contract_text, law_type, compliance_result, issues_found, recommendations):
        self.contract_text = contract_text
        self.law_type = law_type
        self.compliance_result = compliance_result  # 'compliant', 'non_compliant', 'partial'
        self.issues_found = issues_found
        self.recommendations = recommendations
        self.analyzed_at = datetime.now()