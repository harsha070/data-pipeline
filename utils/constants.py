STATIC = "static"
DYNAMIC = "dynamic"
AWS = "aws"
S3_ACCESS_KEY = "s3_access_key"
S3_SECRET_KEY = "s3_secret_key"
FSYM = "fsym"
UPLOAD_TO_S3 = "upload_to_s3"
SA_COLUMNS = [
    'expected_date_range_begin', 
    'drug_brand_name', 
    'lead_company_name', 
    'partner_company_names', 
    'event_type', 
    'event_title',
    'lead_company_ticker',
    'partner_company_tickers',
]
BMT_COLUMNS = [
    'event_id', 
    'event_phase', 
    'event_title', 
    'event_type', 
    'event_status', 
    'expected_date_range_begin', 
    'expected_date_range_end', 
    'drug_brand_name', 
    'drug_generic_name', 
    'indication', 
    'lead_company_name', 
    'lead_company_ticker', 
    'partner_company_names', 
    'partner_company_tickers'
]
COLUMN_NAME_MAP = {
    'Date': 'expected_date_range_begin', 
    'Drug Name': 'drug_brand_name',
    'Manufacturer': 'lead_company_name',
    'Partners': 'partner_company_names',
    'Event Type': 'event_type',
    'Details': 'event_title',
}
