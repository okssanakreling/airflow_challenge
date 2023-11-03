from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.models import Variable
import pandas as pd


# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
## Do not change the code above this line-----------------------##


def extract_orders():
    df = SqliteHook("NorthwhindSqlite").get_pandas_df("SELECT * FROM 'Order'")
    df.to_csv("./data/output_orders.csv", index=False)


def merge_data():
    orders_df = pd.read_csv("./data/output_orders.csv")
    orders_df["OrderId"] = orders_df["Id"].astype("int64")
    order_details_df = SqliteHook("NorthwhindSqlite").get_pandas_df("SELECT * FROM 'OrderDetail'")
    order_details_df["OrderId"] = order_details_df["OrderId"].astype("int64")
    df = orders_df.merge(order_details_df, on=["OrderId"])
    df = df[df["ShipCity"] == "Rio de Janeiro"]
    total_orders = df["Quantity"].sum()
    with open("./count.txt", "w") as f:
        f.write(str(total_orders))
    with open("./data/count.txt", "w") as f:
        f.write(str(total_orders))


with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = "Esse é o desafio de Airflow da Indicium."
    extract_orders_task = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders,
    )

    merge_data_task = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
    )
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer
    )

    extract_orders_task >> merge_data_task >> export_final_output