from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# Event Handler: Logs execution status and errors directly into the DWH table
def log_execution(context):
    if context.get('exception'):
        status = 'FAILED'
        error_msg = str(context.get('exception')).replace("'", '"')[:250]
    else:
        status = 'SUCCESS'
        error_msg = 'DAG completed successfully'

    hook = PostgresHook(postgres_conn_id='pg_dwh_conn')
    sql = f"""
        INSERT INTO dwh.etl_logs (process_name, execution_time, status, error_message)
        VALUES ('airflow_mrr_stg_dwh_dag', CURRENT_TIMESTAMP, '{status}', '{error_msg}');
    """
    hook.run(sql)


def extract_and_load_mrr():
    source_hook = PostgresHook(postgres_conn_id='pg_source_conn')
    dwh_hook = PostgresHook(postgres_conn_id='pg_dwh_conn')
    
    dwh_hook.run("TRUNCATE TABLE mrr.mrr_fact_sales, mrr.mrr_dim_customers, mrr.mrr_dim_products CASCADE;")
    
    customers = source_hook.get_records("SELECT id, name, country FROM customers")
    dwh_hook.insert_rows(table="mrr.mrr_dim_customers", rows=customers, target_fields=['id', 'name', 'country'])
    
    products = source_hook.get_records("SELECT id, name, groupname FROM products")
    dwh_hook.insert_rows(table="mrr.mrr_dim_products", rows=products, target_fields=['id', 'name', 'groupname'])
    
    hwm_record = dwh_hook.get_first("SELECT COALESCE(MAX(last_update_date), '1900-01-01') FROM dwh.high_water_mark WHERE table_name = 'sales'")
    last_hwm = hwm_record[0] if hwm_record else '1900-01-01'
    
    query = f"SELECT id, customerid, productid, qty, updated_at FROM sales WHERE updated_at > '{last_hwm}'"
    sales_delta = source_hook.get_records(query)
    
    if sales_delta:
        dwh_hook.insert_rows(
            table="mrr.mrr_fact_sales", 
            rows=sales_delta, 
            target_fields=['id', 'customerid', 'productid', 'qty', 'updated_at']
        )

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_success_callback': log_execution,
    'on_failure_callback': log_execution
}

with DAG(
    'mrr_stg_dwh_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='ETL pipeline extracting data from Source to DWH'
) as dag:

    # From OperationalDB to MRR
    load_mrr_task = PythonOperator(
        task_id='source_to_mrr',
        python_callable=extract_and_load_mrr
    )

    # From MRR to STG
    load_stg_task = PostgresOperator(
        task_id='mrr_to_stg',
        postgres_conn_id='pg_dwh_conn',
        sql="""
            TRUNCATE TABLE stg.stg_fact_sales, stg.stg_dim_customers, stg.stg_dim_products, stg.rejected_sales CASCADE;
            
            INSERT INTO stg.stg_dim_customers SELECT id, TRIM(name), UPPER(country) FROM mrr.mrr_dim_customers;
            INSERT INTO stg.stg_dim_products SELECT * FROM mrr.mrr_dim_products;
            
            INSERT INTO stg.stg_fact_sales 
            SELECT * FROM mrr.mrr_fact_sales WHERE qty > 0;
            
            INSERT INTO stg.rejected_sales (id, customerId, productId, qty, updated_at, reject_reason)
            SELECT id, customerId, productId, qty, updated_at, 'Negative or zero quantity detected' 
            FROM mrr.mrr_fact_sales WHERE qty <= 0;
        """
    )

    # From STG to Final DWH with Delta processing
    load_dwh_task = PostgresOperator(
        task_id='stg_to_dwh',
        postgres_conn_id='pg_dwh_conn',
        sql="""
            INSERT INTO dwh.dwh_dim_customers SELECT * FROM stg.stg_dim_customers
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, country = EXCLUDED.country;
            
            INSERT INTO dwh.dwh_dim_products SELECT * FROM stg.stg_dim_products
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, groupname = EXCLUDED.groupname;
            
            INSERT INTO dwh.dwh_fact_sales (id, customerId, productId, qty, updated_at) 
            SELECT id, customerId, productId, qty, updated_at FROM stg.stg_fact_sales 
            WHERE updated_at > (SELECT last_update_date FROM dwh.high_water_mark WHERE table_name = 'sales')
            ON CONFLICT (id) DO UPDATE SET 
                customerId = EXCLUDED.customerId, 
                productId = EXCLUDED.productId, 
                qty = EXCLUDED.qty, 
                updated_at = EXCLUDED.updated_at;
                
            UPDATE dwh.high_water_mark 
            SET last_update_date = (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM dwh.dwh_fact_sales)
            WHERE table_name = 'sales';
        """
    )

    load_mrr_task >> load_stg_task >> load_dwh_task