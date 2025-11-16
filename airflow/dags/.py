from google.cloud import bigquery
from google.oauth2 import service_account
# import db_dtypes

# Đường dẫn đến file JSON key
key_path = "../keys/gcp_key.json"

# Tạo credentials
credentials = service_account.Credentials.from_service_account_file(key_path)

# Tạo BigQuery client
client = bigquery.Client(credentials=credentials, project="japanese-food-shop-analysis")

print("✅ Đã kết nối BigQuery thành công!")