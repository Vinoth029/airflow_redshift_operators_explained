# **Redshift Data API – Multi‑Row & Multi‑Column Example (Markdown Guide)**

This document provides a **clear and visual explanation** of how Redshift **Data API** returns results when querying a table with **multiple rows and multiple columns**, and how to **extract those values** in Python using `RedshiftDataHook`.

You will learn:

* What the input table looks like
* The SQL executed
* The exact Data API JSON returned
* How to extract each field manually
* How to convert the response into Python lists and Pandas DataFrame

---

# 🧪 **1. Example Redshift Table (Input Data)**

Assume this table exists in Redshift:

**Table: `mart.orders_summary`**

| order_id | customer_id | amount |
| -------- | ----------- | ------ |
| 101      | 5001        | 100.00 |
| 102      | 5002        | 200.00 |
| 103      | 5003        | 150.00 |
| 104      | 5004        | 99.50  |
| 105      | 5005        | 250.00 |

5 rows × 3 columns.

---

# 📝 **2. SQL Query Used**

```sql
SELECT order_id, customer_id, amount
FROM mart.orders_summary
LIMIT 5;
```

---

# 📌 **3. Redshift Data API Raw Response**

Redshift Data API **does not return simple arrays**.
It returns a typed JSON structure.

Here is the **actual output structure** for our example:

```json
{
  "Records": [
    [ {"longValue": 101}, {"longValue": 5001}, {"doubleValue": 100.00} ],
    [ {"longValue": 102}, {"longValue": 5002}, {"doubleValue": 200.00} ],
    [ {"longValue": 103}, {"longValue": 5003}, {"doubleValue": 150.00} ],
    [ {"longValue": 104}, {"longValue": 5004}, {"doubleValue": 99.50} ],
    [ {"longValue": 105}, {"longValue": 5005}, {"doubleValue": 250.00} ]
  ],
  "ColumnMetadata": [
    {"name": "order_id", "typeName": "bigint"},
    {"name": "customer_id", "typeName": "bigint"},
    {"name": "amount", "typeName": "double"}
  ]
}
```

### 🔍 Notes

* `Records` = list of rows
* Each row = list of column objects
* Each column has a **typed key**:

  * `longValue` (BIGINT, INT)
  * `doubleValue` (DOUBLE)
  * `stringValue` (VARCHAR)

---

# 🧠 **4. Accessing Individual Values**

After running:

```python
result = hook.get_statement_result(resp["Id"])
records = result["Records"]
```

## ✔ Access the 1st row

```python
row1 = records[0]
```

This gives:

```python
[ {"longValue": 101}, {"longValue": 5001}, {"doubleValue": 100.00} ]
```

## ✔ Access the columns inside row 1

```python
order_id = records[0][0]["longValue"]      # 101
customer_id = records[0][1]["longValue"]   # 5001
amount = records[0][2]["doubleValue"]      # 100.00
```

---

# 🔄 **5. Loop Through All Rows**

```python
for row in records:
    order_id = row[0]["longValue"]
    customer_id = row[1]["longValue"]
    amount = row[2]["doubleValue"]
    print(order_id, customer_id, amount)
```

### Output:

```
101 5001 100.0
102 5002 200.0
103 5003 150.0
104 5004 99.5
105 5005 250.0
```

---

# 🧩 **6. Convert Data API Result to Clean Python Dicts**

```python
clean_rows = []

for row in records:
    clean_rows.append({
        "order_id": row[0]["longValue"],
        "customer_id": row[1]["longValue"],
        "amount": row[2]["doubleValue"]
    })
```

### Output (`clean_rows`):

```python
[
  {"order_id": 101, "customer_id": 5001, "amount": 100.0},
  {"order_id": 102, "customer_id": 5002, "amount": 200.0},
  {"order_id": 103, "customer_id": 5003, "amount": 150.0},
  {"order_id": 104, "customer_id": 5004, "amount": 99.5},
  {"order_id": 105, "customer_id": 5005, "amount": 250.0}
]
```

---

# 🐼 **7. Convert to Pandas DataFrame**

```python
import pandas as pd

df = pd.DataFrame(clean_rows)
print(df)
```

### Output:

| order_id | customer_id | amount |
| -------- | ----------- | ------ |
| 101      | 5001        | 100.0  |
| 102      | 5002        | 200.0  |
| 103      | 5003        | 150.0  |
| 104      | 5004        | 99.5   |
| 105      | 5005        | 250.0  |

---

# 🎉 **8. Summary**

* Redshift Data API returns **typed nested JSON**, not plain rows.
* Each row = array of column objects.
* Each column uses a specific type key like:

  * `longValue`, `doubleValue`, `stringValue`.
* You must extract values manually:

  ```python
  row[col]["longValue"]
  ```
* You can convert it into:

  * Python dicts
  * Pandas DataFrames
  * JSON for S3
  * Any other structure

This structured approach is needed because the Data API is schema-aware and type-safe.

---

# 🏗 **9. Full Airflow DAG Using Redshift Data API (Multi‑Row, Multi‑Column Example)**

Below is a production‑style Airflow DAG that:

* Runs a multi‑column query using **RedshiftDataHook** (Data API)
* Retrieves all rows + columns
* Extracts values programmatically
* Converts them to a clean Pandas DataFrame
* Prints / returns useful output

```python
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook

# -------------------------------------------------------
# Function: Query Redshift (Data API) and return Records
# -------------------------------------------------------
def fetch_redshift_data(**context):
    hook = RedshiftDataHook(
        cluster_identifier="{{ var.value.redshift_cluster_identifier }}",
        database="dev",
        db_user="awsuser",
        aws_conn_id="aws_default"
    )

    sql = """
        SELECT order_id, customer_id, amount
        FROM mart.orders_summary
        LIMIT 5;
    """

    # Step 1 — Execute query
    
    resp = hook.execute_statement(sql=sql, with_event=False)  # resp <- {"Id": "d3b21483-a2ed-4550-b508-998e9c9c3381"}

    # Step 2 — Retrieve results
    result = hook.get_statement_result(resp["Id"])
    records = result["Records"]

    # Step 3 — Parse into clean python list
    clean_rows = []
    for row in records:
        clean_rows.append({
            "order_id": row[0].get("longValue"),
            "customer_id": row[1].get("longValue"),
            "amount": row[2].get("doubleValue")
        })

    # Step 4 — Push as JSON for downstream tasks
    ti = context["ti"]
    ti.xcom_push(key="orders_clean", value=pd.DataFrame(clean_rows).to_json(orient="records"))

    print("Fetched & Parsed Records:")
    print(clean_rows)


# -------------------------------------------------------
# Function: Convert JSON → DataFrame and print
# -------------------------------------------------------
def convert_to_dataframe(**context):
    ti = context["ti"]

    df_json = ti.xcom_pull(task_ids="fetch_data", key="orders_clean")
    df = pd.read_json(df_json)

    print("DataFrame from XCom:")
    print(df)

    return "Converted to DataFrame"


# -------------------------------------------------------
# DAG Definition
# -------------------------------------------------------
default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="redshift_data_api_multicolumn_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["redshift", "data-api", "multicolumn"]
) as dag:

    fetch_data = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_redshift_data,
        provide_context=True,
    )

    convert_df = PythonOperator(
        task_id="convert_df",
        python_callable=convert_to_dataframe,
        provide_context=True,
    )

    fetch_data >> convert_df
```

---
