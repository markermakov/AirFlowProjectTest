import requests
import json
from datetime import date, datetime
import psycopg2

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
    password='Njvcr123',
    host='localhost',
    port='5432'
    )

    cursor = conn.cursor()
    cursor.execute("INSERT into public.""etl_test""(currency_pair, created_on, current_rate) VALUES (%s, %s, %s)", listToPg)
    conn.commit()
    conn.close()

main()