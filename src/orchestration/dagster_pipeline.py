from dagster import job, op
from datetime import datetime

@op
def generate_data():
  return {"timestamp": datetime.now().isoformat()}

@op
def process_flink():
  return {"status": "flink_job_completed"}

@op
def run_dbt():
  return {"status": "dbt_run_completed"}

@job
def lakehouse_pipeline():
  step1 = generate_data()
  step2 = process_flink()
  step3 = run_dbt()
  step2 >> step3