try:
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.dummy_operator import DummyOperator
    from datetime import date, datetime, timedelta
    import requests
    from time import sleep
    import pandas as pd
    import json
    import psycopg2
    print("All modules were imported successfully")

except Exception as e:
    print("Error {} ".format(e))

def main():
    today = str(date.today())
    hours = datetime.now().strftime("%H:%M:%S")
    url = "https://api.exchangerate.host/convert?from=BTC&to=USD"
    response = requests.get(url)
    data = response.json()
    listToPg = ["BTC/USD", today + ' ' + hours, round(data['info'].get("rate"), 2)]
    print(listToPg)

    conn = psycopg2.connect(
        database="postgres",
        user='markermakov',
        password='admin',
        host='host.docker.internal',
        port='5432'
    )

    cursor = conn.cursor()
    cursor.execute("INSERT into public.""etl_test""(currency_pair, created_on, current_rate) VALUES (%s, %s, %s)", listToPg)
    conn.commit()
    my_table = pd.read_sql("select * from public.""etl_test""", conn)
    print(my_table.tail(5))
    conn.close()

with DAG (
    dag_id="ETL",
    schedule_interval="0 */3 * * *",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2022, 4, 28)
    },
    catchup=False
) as f:

    main = PythonOperator(
        task_id="main",
        python_callable=main,
    )