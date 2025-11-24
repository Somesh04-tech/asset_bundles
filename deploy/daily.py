import os
from pathlib import Path
#depcon, deployment config, houses the settings that don't change frequently
from depcon import spv, dlts, user, policies, branches, access_control
import argparse
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--env', default = "stage", help="environment to deploy to")

args = parser.parse_known_args()[0]

#Users Laptop file location:
local_root=Path(__file__).parent.parent

#settings
env=args.env
branch_name=branches[env]
#You can just create the deployment.yaml or you could deploy and or launch job as well.
#to make modifaction quicker
T,F=True,False

notify=F

#This creates the deployment file

fil = open(f"{local_root}\\resources\\day.yml", "w")

fil.write(f"""resources:
  jobs:
    outbound_delta_feed:
      name: outbound_delta_feed""")
if (notify)|(env=="PROD"):
  fil.write(f"""
      email_notifications:
        on_success:
        - ryan.mullen@zebra.com
        - chirantan.andhalkar@zebra.com
        on_failure:
        - ryan.mullen@zebra.com
        - chirantan.andhalkar@zebra.com""")
fil.write(f"""
      tags:
        job_name: outbound_delta_feed
        workspace: {env}""")
if env=="PROD":
  fil.write(f"""
      schedule:
        quartz_cron_expression: 0 0 08 ? * TUE,WED,THU
        timezone_id: UTC
        pause_status: UNPAUSED""")
fil.write(f"""
      git_source:
        git_url: https://github.com/zebratechnologies/antuit-esp_ai_proj_bbu.git
        git_provider: gitHub
        git_branch: {branch_name}
      edit_mode: UI_LOCKED
      job_clusters:
      - job_cluster_key: Job_cluster
        new_cluster:
          spark_version: {spv}
          spark_conf:
            spark.hadoop.fs.azure.delete.threads: 20
            spark.databricks.delta.preview.enabled: true
            spark.sql.sources.commitProtocolClass: org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
            spark.hadoop.parquet.enable.summary-metadata: false
            mapreduce.fileoutputcommitter.marksuccessfuljobs: false
          policy_id: {policies[env]}
          custom_tags:
            task_name: DataTransfer
            job_name: outbound_delta_feed
            workspace: {env}
          node_type_id: Standard_D16ads_v5
          driver_node_type_id: Standard_D8ads_v5
          num_workers: 1
      tasks:
      - task_key: BBU-DAILY-OUTBOUND  
        spark_python_task:
          python_file: ETL/workflows/outbound/dp_prompt.py
          source: GIT
          parameters:
          - --outbound
          - ion_batch_forecast_delta
        job_cluster_key: Job_cluster
    pre_ingestion_daily:
      name: pre_ingestion_daily""")
if (notify)|(env=="PROD"):
  fil.write(f"""
      email_notifications:
        on_success:
        - ryan.mullen@zebra.com
        - chirantan.andhalkar@zebra.com
        on_failure:
        - ryan.mullen@zebra.com
        - chirantan.andhalkar@zebra.com""")
fil.write(f"""
      tags:
        job_name: pre_ingestion_daily
        workspace: {env}""")
if env=="PROD":
  fil.write(f"""
      schedule:
        quartz_cron_expression: 57 30 1 * * ?
        timezone_id: UTC
        pause_status: UNPAUSED""")
fil.write(f"""
      git_source:
        git_url: https://github.com/zebratechnologies/antuit-esp_ai_proj_bbu.git
        git_provider: gitHub
        git_branch: {branch_name}
      edit_mode: UI_LOCKED
      job_clusters:
      - job_cluster_key: Job_cluster
        new_cluster:
          spark_version: {spv}
          spark_conf:
            spark.master: local[*, 4]
          node_type_id: Standard_D4ds_v5
          policy_id: {policies[env]}
          custom_tags:
            ResourceClass: SingleNode
          runtime_engine: STANDARD
          num_workers: 0
      tasks:
      - task_key: BBU-DAILY-OUTBOUND  
        spark_python_task:
          python_file: ETL/workflows/bronze/bronze_pre_ingestion.py
          source: GIT
          parameters:
          - --client_stream
          - daily_price_feed,daily_scan
        job_cluster_key: Job_cluster""")

fil.close()
