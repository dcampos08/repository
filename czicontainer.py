from datetime import datetime, timedelta
import civis
import os
import pandas as pd
from civis.io import read_civis_sql, file_to_civis
client = civis.APIClient()
db_id = client.get_database_id('Chan Zuckerberg Initiative')
cred_id = client.default_credential
job_id = os.environ.get('CIVIS_JOB_ID')
run_id = os.environ.get('CIVIS_RUN_ID')

limit = os.environ['limit']
state = os.environ['state']
city = os.environ['city']


if state == 'NULL' and city == 'NULL':
    generate_table = """drop table if exists report.target_list; 
                       create table report.target_list as 
                       select
                       voterbase_id
                       ,city
                       ,state
                       ,zip
                       ,spiritual_health_score 
                       from report.list_cutter 
                       order by spiritual_health_score desc 
                       limit """ + limit
    sql_script = client.scripts.post_sql(
      sql= generate_table,
      remote_host_id=db_id,
      credential_id=cred_id,
      name="CZI Spiritual Health Top Scores")
    print("Created export script ID {script_id}".format(
      script_id=sql_script['id']))
    sql_script_run = client.scripts.post_sql_runs(sql_script.id)
    civis.polling.PollableResult(
    client.scripts.get_sql_runs,
    (sql_script.id, sql_script_run.id)
    ).result()
    table_id=client.get_table_id(
      table="report.target_list",
      database="Chan Zuckerberg Initiative")
    client.scripts.post_python3_runs_outputs(
      job_id,
      run_id,
      'Table',
      table_id)
    df = civis.io.read_civis_sql(
      sql = "select* from report.target_list",
      database = "Chan Zuckerberg Initiative",
      use_pandas = True)
    df.to_csv('target_list.csv')
    file_id = None
    expiration_date = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S%z")
    with open('target_list.csv', 'r') as f:
      file_id = civis.io.file_to_civis(f, 'target_list.csv', expires_at=expiration_date)
    file_info = client.files.get(file_id)
    print("File ID: ",file_id)
    print("File size: ",file_info.file_size)
    client.scripts.post_python3_runs_outputs(job_id, run_id, 'File', file_id)

elif state != 'NULL' and city == 'NULL':
    str1 = """drop table if exists report.target_list;
    create table report.target_list as
    select voterbase_id,city,state,zip,spiritual_health_score
    from report.list_cutter
    where state = """
    str2 = " order by spiritual_health_score desc limit "
    generate_table = str1+state+str2+limit
    sql_script = client.scripts.post_sql(
      sql= generate_table,
      remote_host_id=db_id,
      credential_id=cred_id,
      name="CZI Spiritual Health Top Scores")
    print("Created export script ID {script_id}".format(
      script_id=sql_script['id']))
    sql_script_run = client.scripts.post_sql_runs(sql_script.id)
    civis.polling.PollableResult(
      client.scripts.get_sql_runs,
      (sql_script.id, sql_script_run.id)
      ).result()
    table_id = client.get_table_id(
      table = "report.target_list",
      database = "Chan Zuckerberg Initiative")
    client.scripts.post_python3_runs_outputs(
      job_id,
      run_id,
      'Table',
      table_id)
    df = civis.io.read_civis_sql(
      sql = "select* from report.target_list",
      database = "Chan Zuckerberg Initiative",
      use_pandas = True)
    df.to_csv('target_list.csv')
    file_id = None
    expiration_date = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S%z")
    with open('target_list.csv', 'r') as f:
      file_id = civis.io.file_to_civis(f, 'target_list.csv', expires_at=expiration_date)
    file_info = client.files.get(file_id)
    print("File ID: ",file_id)
    print("File size: ",file_info.file_size)
    client.scripts.post_python3_runs_outputs(job_id, run_id, 'File', file_id)

elif state == 'NULL' and city != 'NULL':
    str1="""drop table if exists report.target_list;
    create table report.target_list as
    select voterbase_id,city,state,zip,spiritual_health_score
    from report.list_cutter
    where city = """
    str2 = " order by spiritual_health_score desc limit "
    generate_table = str1+city+str2+limit
    sql_script = client.scripts.post_sql(
      sql= generate_table,
      remote_host_id=db_id,
      credential_id=cred_id,
      name="CZI Spiritual Health Top Scores")
    print("Created export script ID {script_id}".format(
      script_id=sql_script['id']))
    sql_script_run = client.scripts.post_sql_runs(sql_script.id)
    civis.polling.PollableResult(
      client.scripts.get_sql_runs,
      (sql_script.id, sql_script_run.id)
      ).result()
    table_id = client.get_table_id(
      table = "report.target_list",
      database = "Chan Zuckerberg Initiative")
    client.scripts.post_python3_runs_outputs(
      job_id,
      run_id,
      'Table',
      table_id)
    df = civis.io.read_civis_sql(
      sql = "select* from report.target_list",
      database = "Chan Zuckerberg Initiative",
      use_pandas = True)
    df.to_csv('target_list.csv')
    file_id = None
    expiration_date = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S%z")
    with open('target_list.csv', 'r') as f:
      file_id = civis.io.file_to_civis(f, 'target_list.csv', expires_at=expiration_date)
    file_info = client.files.get(file_id)
    print("File ID: ",file_id)
    print("File size: ",file_info.file_size)
    client.scripts.post_python3_runs_outputs(job_id, run_id, 'File', file_id)

else:
    str1="""drop table if exists report.target_list;
    create table report.target_list as
    select voterbase_id,city,state,zip,spiritual_health_score
    from report.list_cutter
    where state = """
    str2=" and city = "
    str3=" order by spiritual_health_score desc limit "
    generate_table = str1+state+str2+city+str3+limit
    sql_script = client.scripts.post_sql(
      sql= generate_table,
      remote_host_id=db_id,
      credential_id=cred_id,
      name="CZI Spiritual Health Top Scores")
    print("Created export script ID {script_id}".format(
      script_id=sql_script['id']))
    sql_script_run = client.scripts.post_sql_runs(sql_script.id)
    civis.polling.PollableResult(
      client.scripts.get_sql_runs,
      (sql_script.id, sql_script_run.id)
      ).result()
    table_id = client.get_table_id(table = "report.target_list", database = "Chan Zuckerberg Initiative")
    client.scripts.post_python3_runs_outputs(job_id, run_id, 'Table', table_id)
    df = civis.io.read_civis_sql(
      sql = "select* from report.target_list",
      database = "Chan Zuckerberg Initiative",
      use_pandas = True)
    df.to_csv('target_list.csv')
    file_id = None
    expiration_date = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S%z")
    with open('target_list.csv', 'r') as f:
      file_id = civis.io.file_to_civis(f, 'target_list.csv', expires_at=expiration_date)
    file_info = client.files.get(file_id)
    print("File ID: ",file_id)
    print("File size: ",file_info.file_size)
    client.scripts.post_python3_runs_outputs(job_id, run_id, 'File', file_id) 
