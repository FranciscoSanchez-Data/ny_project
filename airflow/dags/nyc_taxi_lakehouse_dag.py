from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Ruta base de nuestro proyecto en WSL
PROJECT_ROOT = "/home/math2data/nyc-mobility-lakehouse"

# Configuraciones por defecto para los reintentos y alertas
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2), # Si un script falla, espera 2 mins y reintenta
}


# Definimos el DAG
with DAG(
    'nyc_mobility_lakehouse_pipeline',
    default_args=default_args,
    description='End-to-end Lakehouse pipeline for NYC Taxi data',
    schedule='@monthly', # <-- ¡AQUÍ ESTÁ EL CAMBIO!
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['nyc_mobility', 'lakehouse', 'dbt'],
) as dag:

    # Tarea 1: Descargar datos
    t1_download = BashOperator(
        task_id='download_taxi_data',
        bash_command=f'python {PROJECT_ROOT}/src/download_data.py',
        cwd=PROJECT_ROOT
    )

    # Tarea 2: Ingesta a Bronze
    t2_bronze = BashOperator(
        task_id='ingest_to_bronze',
        bash_command=f'python {PROJECT_ROOT}/src/bronze_ingestion.py',
        cwd=PROJECT_ROOT
    )

    # Tarea 3: Transformación a Silver
    t3_silver = BashOperator(
        task_id='transform_to_silver',
        bash_command=f'python {PROJECT_ROOT}/src/silver_transformations.py',
        cwd=PROJECT_ROOT
    )

    # Tarea 4: dbt run (Modelos Gold)
    t4_dbt_run = BashOperator(
        task_id='run_dbt_models',
        bash_command='dbt run --profiles-dir .',
        cwd=f'{PROJECT_ROOT}/dbt' # Ejecutamos desde la carpeta dbt/
    )

    # Tarea 5: dbt test (Calidad de datos)
    t5_dbt_test = BashOperator(
        task_id='test_dbt_models',
        bash_command='dbt test --profiles-dir .',
        cwd=f'{PROJECT_ROOT}/dbt'
    )

    # EL FLUJO DE EJECUCIÓN (Dependencies)
    # El operador ">>" significa "después de la tarea anterior, ejecuta esta"
    t1_download >> t2_bronze >> t3_silver >> t4_dbt_run >> t5_dbt_test