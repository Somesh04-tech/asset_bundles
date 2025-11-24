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
job_name="BBU-WEEKLY-JOB"
branch_name=branches[env]
#You can just create the deployment.yaml or you could deploy and or launch job as well.
#to make modifaction quicker
T,F=True,False

DS=T
FCST=T
DP=T
ION=T
OUTBOUND=T

notify=F

#This creates the deployment file
fil = open(f"{local_root}\\resources\\wkly.yml", "w")

fil.write(f"""resources:
  jobs:
    {job_name}:
      name: {job_name}""")
if (notify)|(env=="PROD"):
  fil.write(f"""
      email_notifications:
        on_success:
        - ryan.mullen@zebra.com
        - amit.agrawal@zebra.com
        - chirantan.andhalkar@zebra.com
        - ranjith.kl@zebra.com
        on_failure:
        - ryan.mullen@zebra.com
        - chirantan.andhalkar@zebra.com
        - ranjith.kl@zebra.com""")
fil.write(f"""
      tags:
        job_name: {job_name}
        workspace: {env}""")
if env=="PROD":
  fil.write(f"""
      schedule:
        quartz_cron_expression: 54 30 2 ? * Sun
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
            spark.databricks.io.directoryCommit.createSuccessFile: false
            spark.scheduler.mode: FIFO
          policy_id: {policies[env]}
          custom_tags:
            task_name: DataTransfer
            job_name: {job_name}
            workspace: {env}
          node_type_id: Standard_D16ads_v5
          driver_node_type_id: Standard_D8ads_v5
          num_workers: 1
      - job_cluster_key: single
        new_cluster:
          spark_version: {spv}
          spark_conf:
            spark.master: local[*, 4]
            spark.databricks.cluster.profile: singleNode
          node_type_id: Standard_D16ads_v5
          custom_tags:
            ResourceClass: SingleNode
          runtime_engine: STANDARD
          num_workers: 0
      tasks:""")
if DS:
  fil.write(f"""
      - task_key: PRE_INGESTION_WEEKLY
        spark_python_task:
          python_file: ETL/workflows/bronze/bronze_pre_ingestion.py
          source: GIT
          parameters:
          - --is_weekly
          - 1
          - --client_stream
          - customer,location,product,move_history,scan,sales,preliminary_sales,patterns,menu,salesperson,tray_counts,ion_hh_sort_seq,lost_sales,prompt_forecast,prompt_batch,fl_communication
        job_cluster_key: single
      - task_key: SILVER_DATA_INGESTION
        depends_on:
        - task_key: PRE_INGESTION_WEEKLY
        pipeline_task:
          pipeline_id: {dlts[env]['silver']}
          full_refresh: false
      - task_key: GOLD_INGESTION
        depends_on:
        - task_key: SILVER_DATA_INGESTION
        pipeline_task:
          pipeline_id: {dlts[env]['ds']}
          full_refresh: false""")
if FCST:
  fil.write(f"""    
      - task_key: BBU-DS-JOB""")
  if DS:
    fil.write(f"""
        depends_on:
        - task_key: GOLD_INGESTION""")
  fil.write(f"""  
        run_job_task:
          job_id: {dlts[env]['forecast']}""")
if DP:
  fil.write(f"""
      - task_key: BBU-DP-JOB""")
  if FCST | DS | ION:
    fil.write(f"""
        depends_on:""")
    if ION:
      fil.write(f"""
        - task_key: BBU-ION-JOB""")
    if FCST:
      fil.write(f"""
        - task_key: BBU-DS-JOB""")
    elif DS:
      fil.write(f"""
        - task_key: GOLD_INGESTION""")
  fil.write(f"""  
        pipeline_task:
          pipeline_id: {dlts[env]['dp']}
          full_refresh: false""")
if OUTBOUND:
  fil.write(f"""
      - task_key: load_time_dim_desc""")
  if DP:
    fil.write(f"""
        depends_on:
        - task_key: BBU-DP-JOB""")
  fil.write(f"""  
        spark_python_task:
          python_file: ETL/workflows/outbound/s2_load.py
          source: GIT
          parameters:
          - --dp_tbls
          - time_data,time_dim_desc,year_dim_desc,month_dim_desc,week_dim_desc,time_dim_xref
        job_cluster_key: Job_cluster
      - task_key: load_product_dim_tables
        depends_on:
        - task_key: load_time_dim_desc""")
  fil.write(f"""
        spark_python_task:
          python_file: ETL/workflows/outbound/s2_load.py
          source: GIT
          parameters:
          - --dp_tbls
          - product_dim_desc,product1_dim_desc,product2_dim_desc,product3_dim_desc,product4_dim_desc,product5_dim_desc,product6_dim_desc,product7_dim_desc,product_dim_xref
        job_cluster_key: Job_cluster
      - task_key: load_customer_dim_tables
        depends_on:
        - task_key: load_product_dim_tables""")
  fil.write(f"""
        spark_python_task:
          python_file: ETL/workflows/outbound/s2_load.py
          source: GIT
          parameters:
          - --dp_tbls
          - customer_dim_desc,customer1_dim_desc,customer2_dim_desc,customer3_dim_desc,customer4_dim_desc,customer5_dim_desc,customer6_dim_desc,customer_dim_xref
        job_cluster_key: Job_cluster
      - task_key: load_location_dim_tables
        depends_on:
        - task_key: load_customer_dim_tables""")
  fil.write(f"""
        spark_python_task:
          python_file: ETL/workflows/outbound/s2_load.py
          source: GIT
          parameters:
          - --dp_tbls
          - location_dim_desc,location1_dim_desc,location2_dim_desc,location3_dim_desc,location4_dim_desc,location5_dim_desc,location6_dim_desc,location7_dim_desc,location8_dim_desc,location9_dim_desc,location10_dim_desc,location11_dim_desc,location_dim_xref
        job_cluster_key: Job_cluster
      - task_key: load_fact_data
        depends_on:
        - task_key: load_location_dim_tables""")
  fil.write(f"""
        spark_python_task:
          python_file: ETL/workflows/outbound/s2_load.py
          source: GIT
          parameters:
          - --dp_tbls
          - fact_data
        job_cluster_key: Job_cluster
        max_retries: 1
        min_retry_interval_millis: 180000
      - task_key: load_fact_static_measure
        depends_on:
        - task_key: load_fact_data""")
  fil.write(f"""
        spark_python_task:
          python_file: ETL/workflows/outbound/s2_load.py
          source: GIT
          parameters:
          - --dp_tbls
          - fact_static_measure_data,alert_fact,complex_filters_data,fact_override,events_data,event_dim_desc,event_dim_xref
        job_cluster_key: Job_cluster
      - task_key: BBU-WEEKLY-OUTBOUND""")
  if DP | ION:
    fil.write(f"""
        depends_on:
              """)
    if DP:
      fil.write(f"""
        - task_key: BBU-DP-JOB""")
    if ION:
      fil.write(f"""
        - task_key: BBU-ION-JOB""")
  fil.write(f"""  
        spark_python_task:
          python_file: ETL/workflows/outbound/dp_prompt.py
          source: GIT
          parameters:
          - --outbound
          - ion_forecast_batch,ion_wkly_forecast,initial_forecast,filtered_initial_forecast,ion_initial_forecast,ion_filtered_items,ion_batch_forecast_delta,lost_sales,customer,fl_communication_tgt,handheld_sequence,service_days,customer_history,salesperson,product,customer_service_pattern,ion_route_level,ion_route_product_level,ion_route_store_level,ion_route_store_product_level,prices,ion_historic_product_sales_daily,ion_historic_product_sales
        job_cluster_key: Job_cluster""")
if ION:
  fil.write(f"""
      - task_key: BBU-ION-JOB""")
  if DS:
    fil.write(f"""
        depends_on:
        - task_key: GOLD_INGESTION""")
  fil.write(f"""  
        pipeline_task:
          pipeline_id: {dlts[env]['ion']}
          full_refresh: false
""")

fil.close()
