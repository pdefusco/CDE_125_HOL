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
