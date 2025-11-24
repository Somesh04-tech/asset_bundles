import os
from pathlib import Path
#depcon, deployment config, houses the settings that don't change frequently
from depcon import spv, user, policies, adhoc_root, branches
import argparse
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--env', default = "stage", help="environment to deploy to")

args = parser.parse_known_args()[0]
#settings
env=args.env
job_name="Biannual"
branch_name=branches[env]

#You can just create the deployment.yaml or you could deploy and or launch job as well.
#to make modifaction quicker
T,F=True,False

#Users Laptop file location:
local_root=Path(__file__).parent.parent

#This creates the deployment file
fil = open(f"{local_root}\\resources\\bian.yml", "w")
fil.write(f"""resources:
  jobs:
    {job_name}:
      name: {job_name}
      tags:
        job_name: {job_name}
        workspace: {env}
      email_notifications:
        on_start:
        - santosh.shivanna@zebra.com
        on_success:
        - ryan.mullen@zebra.com
        - santosh.shivanna@zebra.com
        - sudhansu.sahoo@zebra.com
        - amit.agrawal@zebra.com
        - ashik.venkatesh@zebra.com
        on_failure:
        - ryan.mullen@zebra.com
      git_source:
        git_url: https://github.com/zebratechnologies/antuit-esp_ai_proj_bbu.git
        git_provider: gitHub
        git_branch: {branch_name}
      schedule:
        quartz_cron_expression: 40 0 11 ? dec,jun 2#1
        timezone_id: America/Chicago
        pause_status: UNPAUSED
      tasks:
      - task_key: cdp
        depends_on:
        - task_key: cal_day_event
        new_cluster:
          spark_version: {spv}
          policy_id: {policies[env]}
          data_security_mode: SINGLE_USER
          custom_tags:
            task_name: cdp
            task_type: preparefi
            job_name: {job_name}
            workspace: {env}
          node_type_id: Standard_D16d_v4
          driver_node_type_id: Standard_D8d_v4
          num_workers: 2
          spark_conf:
            spark.sql.shuffle.partitions: 96
            spark.hadoop.fs.azure.delete.threads: 20
        spark_python_task:
          python_file: {adhoc_root}makeprofiles.py
          source: GIT
      - task_key: cal_day_event
        new_cluster:
          spark_version: {spv}
          policy_id: {policies[env]}
          data_security_mode: SINGLE_USER
          custom_tags:
            task_name: cal_day_event
            task_type: preparefi
            job_name: {job_name}
            workspace: {env}
          node_type_id: Standard_F16
          driver_node_type_id: Standard_F8
          num_workers: 1
          spark_conf:
            spark.sql.shuffle.partitions: 64
            spark.hadoop.fs.azure.delete.threads: 20
        spark_python_task:
          python_file: {adhoc_root}calendar.py
          source: GIT
      - task_key: shelf_default
        new_cluster:
          spark_version: {spv}
          policy_id: {policies[env]}
          data_security_mode: SINGLE_USER
          custom_tags:
            task_name: shelf_default
            task_type: preparefi
            job_name: {job_name}
            workspace: {env}
          node_type_id: Standard_D16d_v4
          driver_node_type_id: Standard_D8d_v4
          num_workers: 2
          spark_conf:
            spark.sql.shuffle.partitions: 128
            spark.hadoop.fs.azure.delete.threads: 20
        spark_python_task:
          python_file: {adhoc_root}shelfdefaulter.py
          source: GIT
      - task_key: LevelShiftDetect
        new_cluster:
          spark_version: {spv}
          policy_id: {policies[env]}
          data_security_mode: SINGLE_USER
          custom_tags:
            task_name: LevelShiftDetect
            task_type: preparefi
            job_name: {job_name}
            workspace: {env}
          node_type_id: Standard_D16d_v4
          driver_node_type_id: Standard_D8d_v4
          num_workers: 2
          spark_conf:
            spark.sql.shuffle.partitions: 128
            spark.hadoop.fs.azure.delete.threads: 20
        spark_python_task:
          python_file: {adhoc_root}levelShiftDetect.py
          source: GIT
      - task_key: beachdetect
        new_cluster:
          spark_version: {spv}
          policy_id: {policies[env]}
          data_security_mode: SINGLE_USER
          custom_tags:
            task_name: beachdetect
            task_type: preparefi
            job_name: {job_name}
            workspace: {env}
          node_type_id: Standard_D16d_v4
          driver_node_type_id: Standard_D8d_v4
          num_workers: 2
          spark_conf:
            spark.sql.shuffle.partitions: 128
            spark.hadoop.fs.azure.delete.threads: 20
        spark_python_task:
          python_file: {adhoc_root}beachdetector.py
          source: GIT""")
fil.close()