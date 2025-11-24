from pathlib import Path
#depcon, deployment config, houses the settings that don't change frequently
from depcon import spv, nodes, workers, user, policies, file_root, branches, libs

import argparse
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--env', default = "stage", help="environment to deploy to")

args = parser.parse_known_args()[0]


#Users Laptop file location:
local_root=Path(__file__).parent.parent

#settings
env=args.env
type="full"
job_name="forecast"
branch_name=branches[env]

wk_bkp=workers.copy()
#You can just create the deployment.yaml or you could deploy and or launch job as well.
#to make modifaction quicker
T,F=True,False

#you can choose to run only part of the process
cust_clean=T
clean=T
prepd=T
prepw=T
report=T
model=T
daily=T
suggo=T
alerts=T

#for testing purposes you can filter to subsets or run historical frcdts
dows=[1]#range(0,7) #Which calendars do you want to run?
#workers.update({2:[10,7,4,5]}) #if your subset is very restrictive then update the amount of workers for each part
filter=None#"week_day_no=1"  #you can further subset using any customer level filter
altermediate=None
fid=None #which forecast creation id do you want to run?
dwmdworkers=[5,2,2,1,1,1]

#This is to ensure any modifications for tests are put back for a production like run
if type=="full":
    job_name="forecast"
    fid=None
    filter=None
    altermediate=None
    workers=wk_bkp
    dows=range(0,7)
    dwmdworkers=[40,19,66,14,15,4]

#These are common settings for a small scale test
if type=="test":
    env="stage"
    job_name="forecast_test"
    fid=None
    filter="week_day_no=1"
    altermediate=None
    workers=wk_bkp
    dows=[1]
    dwmdworkers=[5,2,2,1,1,1]

#This creates the deployment file
fil = open(f"{local_root}\\resources\\fcst.yml", "w")

fil.write(f"""resources:
  jobs:
    {job_name}:
      name: {job_name}""")
if type=="full":
  fil.write(f"""
      email_notifications:
        on_success:
        - tanmoy.maiti@zebra.com
        - ryan.mullen@zebra.com
        - amit.agrawal@zebra.com
        on_failure:
        - tanmoy.maiti@zebra.com
        - ryan.mullen@zebra.com""")
fil.write(f"""
      tags:
        job_name: {job_name}
        workspace: {env}
        type: {type}
      git_source:
        git_url: https://github.com/zebratechnologies/antuit-esp_ai_proj_bbu.git
        git_provider: gitHub
        git_branch: {branch_name}
      edit_mode: UI_LOCKED
      job_clusters:
      - job_cluster_key: importwk
        new_cluster:
          spark_version: {spv}
          policy_id: {policies[env]}
          data_security_mode: SINGLE_USER
          custom_tags:
            task_name: importwk
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['dp']}
          driver_node_type_id: {nodes['dpd']}
          num_workers: {dwmdworkers[1]}
          spark_conf:
            spark.hadoop.fs.azure.delete.threads: 20
            spark.databricks.execution.executorSideBroadcast.enabled: false
            spark.databricks.delta.optimizeWrite.enabled: false
            spark.sql.ui.explainMode: SIMPLE
            spark.checkpoint.compress: true
            spark.databricks.delta.autoCompact.enabled: false
            spark.databricks.io.cache.enabled: false
            spark.sql.autoBroadcastJoinThreshold: 50M
            spark.scheduler.mode: FIFO
      - job_cluster_key: model
        new_cluster:
          spark_version: {spv}
          policy_id: {policies[env]}
          runtime_engine: STANDARD
          data_security_mode: SINGLE_USER
          custom_tags:
            task_name: model
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['md']}
          driver_node_type_id: {nodes['dp']}
          num_workers: {dwmdworkers[2]}
          init_scripts: 
            - workspace: 
                destination: /Workspace/Repos/bbu_pipeline/antuit-esp_ai_proj_bbu/Inits/adf_init.sh
          spark_conf:
            spark.sql.autoBroadcastJoinThreshold: 200MB
            spark.hadoop.fs.azure.delete.threads: 20
            spark.databricks.delta.optimizeWrite.enabled: false
            spark.sql.ui.explainMode: SIMPLE
            spark.databricks.delta.autoCompact.enabled: false
            spark.sql.adaptive.enabled: false
            spark.scheduler.mode: FIFO
      - job_cluster_key: daily
        new_cluster:
          spark_version: {spv}
          policy_id: {policies[env]}
          runtime_engine: STANDARD
          data_security_mode: SINGLE_USER
          custom_tags:
            task_name: daily
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['dy']}
          driver_node_type_id: {nodes['dv']}
          num_workers: {dwmdworkers[3]}
          spark_conf:
            spark.hadoop.fs.azure.delete.threads: 20
            spark.databricks.delta.optimizeWrite.enabled: false
            spark.sql.ui.explainMode: SIMPLE
            spark.databricks.delta.autoCompact.enabled: false
            spark.scheduler.mode: FIFO
      tasks:""")
if cust_clean:
  fil.write(f"""
      - task_key: cust_cleanse""")
  if clean:
    fil.write(f"""
        depends_on:
        - task_key: clean""")
  fil.write(f"""
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          data_security_mode: SINGLE_USER
          policy_id: {policies[env]}
          custom_tags:
            task_name: cust_cleanse
            task_type: preamble
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['d6']}
          driver_node_type_id: {nodes['dv6']}
          num_workers: 2
          spark_conf:
            spark.sql.shuffle.partitions: 128
            spark.sql.ui.explainMode: SIMPLE
            spark.hadoop.fs.azure.delete.threads: 20
        spark_python_task:
          python_file: {file_root}dowager.py
          source: GIT""")
if prepd:
  fil.write(f"""
      - task_key: import_day""")
  if cust_clean:
    fil.write(f"""
        depends_on:
        - task_key: cust_cleanse""")
  fil.write(f"""
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          data_security_mode: SINGLE_USER
          policy_id: {policies[env]}
          custom_tags:
            task_name: import_day
            task_type: import
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['d6']}
          driver_node_type_id: {nodes['dv6']}
          num_workers: {dwmdworkers[0]}
          spark_conf:
            spark.sql.shuffle.partitions: {64*dwmdworkers[0]}
            spark.hadoop.fs.azure.delete.threads: 20
            spark.sql.ui.explainMode: SIMPLE
            spark.databricks.sql.ui.enhancedSQLTab.enabled: false
            spark.databricks.io.cache.enabled: false
            spark.sql.autoBroadcastJoinThreshold: 50M
        spark_python_task:
          python_file: {file_root}dataprepdy.py
          source: GIT
          parameters:""")
  if fid is not None:
    fil.write(f"""
          - --fid
          - {fid}""")
  if filter is not None:
    fil.write(f"""
          - --filter
          - {filter}""")
if clean:
  fil.write(f"""
      - task_key: clean
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          data_security_mode: SINGLE_USER
          policy_id: {policies[env]}
          custom_tags:
            task_name: clean
            task_type: preamble
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['ot']}
          driver_node_type_id: {nodes['dv']}
          num_workers: 2
          spark_conf:
            spark.sql.shuffle.partitions: 48
            spark.sql.ui.explainMode: SIMPLE
            spark.hadoop.fs.azure.delete.threads: 20
        spark_python_task:
          python_file: {file_root}clean.py
          source: GIT""")
for dow in dows:
    if prepw:
        fil.write(f"""
      - task_key: import{dow}""")
        if prepd:
          fil.write(f"""
        depends_on:
        - task_key: import_day""")
        fil.write(f"""
        job_cluster_key: importwk
        max_retries: 1
        libraries:
        - pypi:
            package: adf==3.0.2
        - pypi:
            package: ai-core==2.4.9
        spark_python_task:
          python_file: {file_root}dataprepwk.py
          source: GIT
          parameters:
          - --dow
          - {dow}""")
        if fid is not None:
            fil.write(f"""
          - --fid
          - {fid}""")
        if dow==2:
            fil.write(f"""
          - --ptile
          - 0.8""")
        if filter is not None:
            fil.write(f"""
          - --filter
          - {filter}""")
    if model:
        fil.write(f"""
      - task_key: model{dow}s""")
        if prepw:
          fil.write(f"""
        depends_on:
        - task_key: import{dow}""")
          if 5 in dows:
            fil.write(f"""
        - task_key: import5""")
        fil.write(f"""
        job_cluster_key: model
        max_retries: 1
        libraries:
        - pypi:
            package: adf==3.0.2
        - pypi:
            package: ai-core==2.4.9
        - maven:
            coordinates: com.microsoft.azure:synapseml_2.12:0.11.1
            repo: https://mmlspark.azureedge.net/maven
        - maven: 
            coordinates: ai.catboost:catboost-spark_3.4_2.12:1.2.5
        spark_python_task:
          python_file: {file_root}model_sales.py
          source: GIT
          parameters:
          - --aos
          - scan""")
        if altermediate is not None:
          fil.write(f"""
          - --alt_out
          - {altermediate}""")
        fil.write(f"""
          - --dow
          - {dow}""")
        if fid is not None:
          fil.write(f"""
          - --fid
          - {fid}""")
        fil.write(f"""
      - task_key: model{dow}a""")
        if prepw:
          fil.write(f"""
        depends_on:
        - task_key: import{dow}""")
          if 5 in dows:
            fil.write(f"""
        - task_key: import5""")
        fil.write(f"""
        job_cluster_key: model
        max_retries: 1
        libraries:
        - pypi:
            package: adf==3.0.2
        - pypi:
            package: ai-core==2.4.9
        - maven:
            coordinates: com.microsoft.azure:synapseml_2.12:0.11.1
            repo: https://mmlspark.azureedge.net/maven
        - maven: 
            coordinates: ai.catboost:catboost-spark_3.4_2.12:1.2.5
        spark_python_task:
          python_file: {file_root}model_sales.py
          source: GIT
          parameters:
          - --aos
          - aged""")
        if altermediate is not None:
          fil.write(f"""
          - --alt_out
          - {altermediate}""")
        fil.write(f"""
          - --dow
          - {dow}""")
        if fid is not None:
          fil.write(f"""
          - --fid
          - {fid}""")
    if daily:
        fil.write(f"""
      - task_key: daily{dow}""")
        if model:
          fil.write(f"""
        depends_on:
        - task_key: model{dow}s
        - task_key: model{dow}a""")
          if 5 in dows:
            fil.write(f"""
        - task_key: model5a""")
        fil.write(f"""
        job_cluster_key: daily
        max_retries: 1
        libraries:
        - pypi:
            package: adf==3.0.2
        - pypi:
            package: ai-core==2.4.9
        spark_python_task:
          python_file: {file_root}daily.py
          source: GIT
          parameters:
          - --dow
          - {dow}""")
        if fid is not None:
          fil.write(f"""
          - --fid
          - {fid}""")
if suggo:
    fil.write(f"""
      - task_key: suggo""")
    if daily:
      fil.write(f"""
        depends_on:""")
      for dow in dows:
        fil.write(f"""
        - task_key: daily{dow}""")
    fil.write(f"""
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          data_security_mode: SINGLE_USER
          policy_id: {policies[env]}
          custom_tags:
            task_name: suggo
            task_type: post
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['md']}
          driver_node_type_id: {nodes['dv']}
          num_workers: {dwmdworkers[4]}
          spark_conf:
            spark.sql.shuffle.partitions: {32*dwmdworkers[4]}
            spark.hadoop.fs.azure.delete.threads: 20
            spark.sql.ui.explainMode: SIMPLE
        libraries:
        - pypi:
           package: scs==3.2.3
        - pypi:
           package: cvxpy
        - pypi:
           package: cvxopt
        spark_python_task:
          python_file: {file_root}suggo.py
          source: GIT""")
if alerts:
    fil.write(f"""
      - task_key: alerts""")
    if suggo:
      fil.write(f"""
        depends_on:
        - task_key: suggo""")
    fil.write(f"""
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          data_security_mode: SINGLE_USER
          policy_id: {policies[env]}
          custom_tags:
            task_name: alerts
            task_type: post
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['d6']}
          driver_node_type_id: {nodes['dv6']}
          num_workers: {dwmdworkers[5]}
          spark_conf:
            spark.sql.shuffle.partitions: {32*dwmdworkers[5]}
            spark.hadoop.fs.azure.delete.threads: 20
            spark.sql.ui.explainMode: SIMPLE
        spark_python_task:
          python_file: {file_root}alerts.py
          source: GIT""")
if report:
    fil.write(f"""
      - task_key: report""")
    if prepw:
      fil.write(f"""
        depends_on:""")
      for dow in dows:
        fil.write(f"""
        - task_key: import{dow}""")
    fil.write(f"""
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          data_security_mode: SINGLE_USER
          policy_id: {policies[env]}
          custom_tags:
            task_name: report
            task_type: post
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['d6']}
          driver_node_type_id: {nodes['dv6']}
          num_workers: 8
          spark_conf:
            spark.sql.shuffle.partitions: 512
            spark.hadoop.fs.azure.delete.threads: 20
            spark.sql.ui.explainMode: SIMPLE
        spark_python_task:
          python_file: {file_root}report.py
          source: GIT""")
fil.close()
