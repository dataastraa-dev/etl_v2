import requests

base_url = "http://127.0.0.1:5000/v1"

# ─── 1. Repair the Use Case Mapping (Aligns to Apr_to_Sep.csv headers) ───
repair_payload = {
  "client_id": "work",
  "column_mapping": {
    "CUSTOMER_TRX_ID": "transaction_id",
    "CUSTOMER_NAME": "customer_name",
    "PRODUCT_FAMILY": "product_id",      # FIXED: Was SKU
    "CATEGORY1": "product_category",
    "CATEGORY2": "product_subcategory",
    "CATEGORY3": "product_type",
    "COUNTRY": "region",
    "SALES_PERSON": "sales_rep_id",
    "STATE": "state_head",
    "ENTERED_AMOUNT": "total_sales_amount",
    "TAX_AMOUNT": "discount_amount",
    "SKU_CGS": "cost",                   # FIXED: Maps cost
    "GL_DATE": "transaction_date",
    "ORDERED_QUANTITY": "quantity",      # FIXED: Maps quantity
    "ORDER_TYPE": "transaction_type",
    "ENTERED_CURRENCY": "base_currency"
  }
}

print("1. Repairing the 'mer_sales' mapping...")
res1 = requests.post(f"{base_url}/use-cases/mer_sales/repair", json=repair_payload)
print(res1.json())

# ─── 2. Commit the Active Configuration with UT/UV Rules ───
config_payload = {
  "client_id": "work",
  "use_case_id": "mer_sales",
  "transformation_rules": [
    {
      "type": "UV",
      "rule": "numeric_range",
      "column": "cost",
      "parameters": {"min": 0},
      "source_file": "DK_SampleData_Apr_to_Sep.csv"
    },
    {
      "type": "UT",
      "rule": "concatenate_columns",
      "column": "transaction_id",
      "parameters": {
        "separator": "-",
        "source_columns": ["transaction_id", "product_id"]
      },
      "source_file": "DK_SampleData_Apr_to_Sep.csv"
    }
  ]
}

print("\n2. Locking in pipeline configuration...")
res2 = requests.post(f"{base_url}/configs", json=config_payload)
print(res2.json())