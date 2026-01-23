from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

username = "pauldefusco"

with DAG(
    dag_id='setup_dag',
    default_args={
        'depends_on_past': False,
        'retry_delay': timedelta(seconds=5),
        'schedule_interval':'@hourly',
        #'end_date': datetime(2024,9,30,8)
    },
    description='Dag with initial setup task that only runs on start_date',
    #start_date=datetime(2023, 12, 10),
    # Runs daily at 1 am
    #schedule_interval='0 1 * * *',
    # catchup must be true if start_date is before datetime.now()
    start_date= datetime(2023,12,9,0),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    def branch_fn(**kwargs):
        # Have to make sure start_date will equal data_interval_start on first run
        # This dag is daily but since the schedule_interval is set to 1 am data_interval_start would be
        # 2000-01-01 01:00:00 when it needs to be
        # 2000-01-01 00:00:00
        date = kwargs['data_interval_start'].replace(hour=0, minute=0, second=0, microsecond=0)
        #date = context.get("execution_date")
        print("THE EXECUTION DATE IS")
        print(date)
        print("THE START DATE IS")
        print(dag.start_date)
        if date == dag.start_date:
            return 'initial_task'
        else:
            return 'skip_initial_task'

    start = DummyOperator(
        task_id="start"
    )

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_fn,
        provide_context=True
    )

    iceberg_migration = CDEJobRunOperator(
        task_id="migrate_to_iceberg",
        job_name="IcebergMigration-"+username
    )

    step1 = CDEJobRunOperator(
          task_id='iceberg_etl',
          job_name='IcebergETL-'+username #job_name needs to match the name assigned to the Spark CDE Job in the CDE UI
        )

    skip_initial_task = DummyOperator(
        task_id="skip_initial_task"
    )

    step2 = CDEJobRunOperator(
        task_id='iceebrg_report',
        job_name='icebergReport-'+username #job_name needs to match the name assigned to the Spark CDE Job in the CDE UI
    )

    start >> branch_task >> [iceberg_migration, skip_initial_task] >> next_task
