from airflow.models import DAG, SkipMixin
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.utils.db import provide_session

class RunOnceBranchOperator(PythonOperator, SkipMixin):
    def __init__(
            self,
            run_once_task_id=None,
            skip_task_id=None,
            *args, **kwargs):
        kwargs['python_callable'] = lambda x: x
        super(RunOnceBranchOperator, self).__init__(*args, **kwargs)

        self.run_once_task_id = run_once_task_id
        self.skip_task_id = skip_task_id

    @provide_session
    def execute(self, context, session=None):
        print("execute runonce operator")
        logging.info("execute once run operator")
        #import IPython; IPython.embed()

        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.task_id == self.run_once_task_id,
            TI.dag_id ==  context['dag'].dag_id,
        ).all()

        previous_sucess = [ t for t in ti if t.state == State.SUCCESS ]

        if previous_sucess:
            logging.info('Found existing task run (%s) with state success. '
                         'Therefore skip the direct downstream task!',
                         previous_sucess)

            branch = self.skip_task_id
        else:
            logging.info('Found no existing task run with state success. '
                         'Therefore run the direct downstream task')
            branch = self.run_once_task_id

        logging.info("Following branch {}".format(branch))
        logging.info("Marking other directly downstream tasks as skipped")

        downstream_tasks = context['task'].downstream_list
        logging.debug("Downstream task_ids {}".format(downstream_tasks))

        skip_tasks = [t for t in downstream_tasks if t.task_id != branch]
        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, skip_tasks)

        logging.info("Done.")

dag = DAG(
    dag_id='example_runtaskonce',
    schedule_interval='@once',
    start_date=days_ago(1),
    is_paused_upon_creation=False
)

parent = DummyOperator(dag=dag, task_id='root')

folders = map(lambda x: 'folder-'+str(x), range(0,3))

for folder in folders:
    #runonce = RunOnceOperator(dag=dag, task_id='runonce_{}'.format(folder))
    task_id_read_folders_in = 'read_folders_in_{}'.format(folder)
    task_id_dummy_skip = 'dummy_skip_{}'.format(folder)
    runonce = RunOnceBranchOperator(
        dag=dag,
        task_id='runonce_{}'.format(folder),
        run_once_task_id=task_id_read_folders_in,
        skip_task_id=task_id_dummy_skip
    )
    runonce.set_upstream(parent)

    dummy_skip = DummyOperator(dag=dag, task_id=task_id_dummy_skip)
    dummy_skip.set_upstream(runonce)

    if folder == 'folder-1':
        # cmd will fail
        cmd = 'cat /tmp/not-there'
    else:
        cmd = 'echo 1'
    read_folders_in = BashOperator(
        task_id=task_id_read_folders_in, bash_command=cmd, dag=dag)
    read_folders_in.set_upstream(runonce)

    join = DummyOperator(
        task_id='join_{}'.format(folder),
        trigger_rule='one_success',
        dag=dag
    )
    join.set_upstream(dummy_skip)
    join.set_upstream(read_folders_in)

    parent = join
