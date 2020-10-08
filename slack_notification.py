# Create a DAG using context manager (with...as...)
# Benefits of context manager: all tasks within context manager is automatically assigned to the DAG so you don't have to explicitly set dag=dag for all tasks.
with DAG('DAG_NAME', default_args=default_args, schedule_interval='@once') as dag:
  
  # Create SQL task A using BigQueryOperator
  bq_sql_task_a = bigquery_operator.BigQueryOperator(
    task_id='demo_bq_sql_a',
    sql="INSERT INTO TABLE TABLE_B SELECT * FROM TABLE_A",
    use_legacy_sql=False)
  
  # Create SQL task B
  bq_sql_task_b = bigquery_operator.BigQueryOperator(
    task_id='demo_bq_sql_b',
    sql="INSERT INTO TABLE TABLE_C SELECT * FROM TABLE_B",
    use_legacy_sql=False)
  
  # Set dependency between A and B: B depends on A
  bq_sql_task_a >> bq_sql_task_b
  
  # Create Slack notification task using SlackAPIPostOperator
  slack_msg_task = SlackAPIPostOperator(
    task_id='slack_msg',
    channel='data_notifications',
    token=os.environ['SLACK_API_TOKEN'],
    text="""
    :white_check_mark: Workflow Succeeded
    *Dag*: {dag}
    *DAG Run ID*: {dag_run_id}
    """.format(dag=dag.dag_id, dag_run_id='{{ run_id }}'))
  
  # Slack task depends on both A and B
  [bq_sql_task_a, bq_sql_task_b] >> slack_msg_task
