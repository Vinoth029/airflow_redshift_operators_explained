# **RedshiftSQLHook Examples: get_first(), get_records(), run()**

This markdown explains the three most important RedshiftSQLHook methods:

* **get_first()** → fetches **ONE** row
* **get_records()** → fetches **ALL** rows
* **run()** → executes SQL **without returning results**

All examples use **5 rows × 3 columns** from a sample Redshift table and include a **full Airflow DAG**.

---

# 🎯 Example Source Table (5 rows × 3 columns)

**Table: `mart.orders_summary`**

| order_id | customer_id | amount |
| -------- | ----------- | ------ |
| 101      | 5001        | 100.00 |
| 102      | 5002        | 200.00 |
| 103      | 5003        | 150.00 |
| 104      | 5004        | 99.50  |
| 105      | 5005        | 250.00 |

---

# 📌 1. **get_first() Example**

Fetches **only the first row** as a tuple.

### **Code**

```python
records = hook.get_first("SELECT order_id, customer_id, amount FROM mart.orders_summary ORDER BY order_id LIMIT 5;")
```

### **Output**

```python
(101, 5001, 100.0)
```

### **Accessing values**

```python
order_id = records[0]        # 101
customer_id = records[1]     # 5001
amount = records[2]          # 100.0
```

---

# 📌 2. **get_records() Example**

Fetches **all rows** as a list of tuples.

### **Code**

```python
records = hook.get_records("SELECT order_id, customer_id, amount FROM mart.orders_summary ORDER BY order_id LIMIT 5;")
```

### **Output**

```python
[
  (101, 5001, 100.00),
  (102, 5002, 200.00),
  (103, 5003, 150.00),
  (104, 5004, 99.50),
  (105, 5005, 250.00)
]
```

### **Accessing values**

```python
records[0][0]   # 101
records[2][2]   # 150.00
```

---

# 📌 3. **run() Example**

Executes SQL **without retrieving results**.

### **Use cases:**

* CREATE TABLE
* DROP TABLE
* INSERT data
* DELETE
* TRUNCATE
* UPDATE

### **Example Code**

```python
hook.run("CREATE TABLE IF NOT EXISTS audit.stats (load_date date, row_count int);")
```

### **Example INSERT using run()**

```python
hook.run("INSERT INTO audit.stats VALUES ('2025-02-20', 5);")
```

run() does **not** return row data.

---

# 🏗 **Full Airflow DAG Demonstrating get_first(), get_records(), run()**

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.operators.python import PythonOperator

# ----------------------------------------------------
# Task 1 — get_first()
# ----------------------------------------------------
def demo_get_first(**context):
    hook = RedshiftSQLHook(redshift_conn_id="redshift_default")

    row = hook.get_first("""
        SELECT order_id, customer_id, amount
        FROM mart.orders_summary
        ORDER BY order_id
        LIMIT 5;
    """)

    print("get_first output:", row)
    # Expected: (101, 5001, 100.0)

    context["ti"].xcom_push(key="first_row", value=row)


# ----------------------------------------------------
# Task 2 — get_records()
# ----------------------------------------------------
def demo_get_records(**context):
    hook = RedshiftSQLHook(redshift_conn_id="redshift_default")

    rows = hook.get_records("""
        SELECT order_id, customer_id, amount
        FROM mart.orders_summary
        ORDER BY order_id
        LIMIT 5;
    """)

    print("get_records output:")
    for r in rows:
        print(r)

    context["ti"].xcom_push(key="all_rows", value=rows)


# ----------------------------------------------------
# Task 3 — run() (no result expected)
# ----------------------------------------------------
def demo_run(**context):
    hook = RedshiftSQLHook(redshift_conn_id="redshift_default")

    hook.run("""
        CREATE TABLE IF NOT EXISTS audit.order_stats (
            order_id bigint,
            amount double precision
        );
    """)

    print("Table created with run()")


# ----------------------------------------------------
# DAG Definition
# ----------------------------------------------------
default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="redshift_hook_examples_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["redshift", "hooks", "examples"],
) as dag:

    t1 = PythonOperator(
        task_id="example_get_first",
        python_callable=demo_get_first,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="example_get_records",
        python_callable=demo_get_records,
        provide_context=True,
    )

    t3 = PythonOperator(
        task_id="example_run",
        python_callable=demo_run,
        provide_context=True,
    )

    t1 >> t2 >> t3
```

---

# 🎉 Summary

This markdown explains how to use RedshiftSQLHook:

## ✔ get_first()

* Fetches **one** row
* Returns a tuple

## ✔ get_records()

* Fetches **multiple rows**
* Returns a list of tuples

## ✔ run()

* Executes SQL (DDL/DML)
* Returns no result

## ✔ Provided Airflow DAG

Demonstrates all three methods using a consistent 5×3 dataset example.

---

