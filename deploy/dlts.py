import os
from pathlib import Path
#depcon, deployment config, houses the settings that don't change frequently
from depcon import storage, pipeline_root, notify, dlt_workers, dlt_nodes
import argparse
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument('--env', default = "stage", help="environment to deploy to")

args = parser.parse_known_args()[0]
#Users Laptop file location:
local_root=Path(__file__).parent.parent

#settings

env=args.env

#purpose is an additional tag that can be placed on all the clusters.  Please, if you are using this make sure the purpose is CORRECT don't just use the same one every time.
purpose="Testing"
deployer=os.popen("whoami").read().strip()

#Common DLT settings

#if true clusters will remain on after completion of job (success or failure) for shutdown delay seconds
development="false"
#length of time user wants clusters to remain active after job completion
shutdown_delay="300s"
#Never Never Never use photon
photon="false"
#user can choose ADVANCED, PRO, or CORE see https://www.databricks.com/product/pricing/delta-live for more details
edition="ADVANCED"
#user can choose CURRENT (most recent version of DLT infrastructure) or PREVIEW (early look at a future release)
channel="CURRENT"

#to ensure that production is never run in development mode
if env=="PROD":
  development="false"
  purpose="Official Prod"

sword=env.lower()
if sword=="dr":
  sword="prod"

#This creates the deployment file
#MAKE sure the path exists on your laptop.
fil = open(f"{local_root}\\resources\\dlts.yml", "w")

fil.write(f"""resources:
  pipelines:
    dlt_silver_data_ingestion:
      name: dlt_silver_data_ingestion
      storage: dbfs:/mnt/{storage[env]}-datalake-rw/bbu-{env.lower()}-datalake
      configuration:
        pipelines.tableManagedByMultiplePipelinesCheck.enabled: false 
        pipelines.clusterShutdown.delay: {shutdown_delay}
      clusters:
      - label: default 
        node_type_id: Standard_D13_v2
        driver_node_type_id: Standard_D13_v2 
        custom_tags:
          job_name: dlt-maintainence
          workspace: {env}
        autoscale:
          min_workers: 1
          max_workers: 10
          mode: ENHANCED
      - label: updates
        node_type_id: {dlt_nodes["ag"][1]}
        driver_node_type_id: {dlt_nodes["ag"][0]}
        custom_tags:
          job_name: dlt-silver-ingestion
          workspace: {env}
          purpose: {purpose}
          deployer: {deployer}
        autoscale:
          min_workers: {dlt_workers["ag"][0]}
          max_workers: {dlt_workers["ag"][1]}
          mode: ENHANCED
      libraries:
      - file:
          path: {pipeline_root}silver/weekly.py
      notifications:
        {notify}
      continuous: false
      development: {development}
      photon: {photon}
      edition: {edition}
      channel: {channel}
    dlt_gold_ds_pipeline:
      name: dlt_gold_ds_pipeline
      storage: dbfs:/mnt/{storage[env]}-datalake-rw/bbu-{sword}-datalake
      configuration:
        pipelines.tableManagedByMultiplePipelinesCheck.enabled: false 
        pipelines.clusterShutdown.delay: {shutdown_delay}
      clusters:
      - label: default 
        node_type_id: Standard_D13_v2
        driver_node_type_id: Standard_D13_v2 
        custom_tags:
          job_name: dlt-maintainence
          workspace: {env}
        autoscale:
          min_workers: 1
          max_workers: 10
          mode: ENHANCED
      - label: updates
        node_type_id: {dlt_nodes["ds"][1]}
        driver_node_type_id: {dlt_nodes["ds"][0]}
        custom_tags:
          job_name: dlt_gold_ds_pipeline
          workspace: {env}
          purpose: {purpose}
          deployer: {deployer}
        autoscale:
          min_workers: {dlt_workers["ds"][0]}
          max_workers: {dlt_workers["ds"][1]}
          mode: ENHANCED
      libraries:
      - file:
          path: {pipeline_root}gold/data_science.py
      notifications:
        {notify}
      continuous: false
      development: {development}
      photon: {photon}
      edition: {edition}
      channel: {channel}
    dlt_gold_ion_pipeline:
      name: dlt_gold_ion_pipeline
      storage: dbfs:/mnt/{storage[env]}-datalake-rw/bbu-{sword}-datalake/gold/
      configuration:
        pipelines.tableManagedByMultiplePipelinesCheck.enabled: false 
        pipelines.clusterShutdown.delay: {shutdown_delay}  
      clusters:
      - label: default 
        node_type_id: Standard_D13_v2
        driver_node_type_id: Standard_D13_v2 
        custom_tags:
          job_name: dlt-maintainence
          workspace: {env}
        autoscale:
          min_workers: 1
          max_workers: 10
          mode: ENHANCED
      - label: updates
        node_type_id: {dlt_nodes["ion"][1]}
        driver_node_type_id: {dlt_nodes["ion"][0]}
        custom_tags:
          job_name: dlt_gold_ion_pipeline
          workspace: {env}
          purpose: {purpose}
          deployer: {deployer}
        autoscale:
          min_workers: {dlt_workers["ion"][0]}
          max_workers: {dlt_workers["ion"][1]}
          mode: ENHANCED
      libraries:
      - file:
          path: {pipeline_root}gold/ion.py
      notifications:
        {notify}
      continuous: false
      development: {development}
      photon: {photon}
      edition: {edition}
      channel: {channel}
    dlt_gold_dp_pipeline:
      name: dlt_gold_dp_pipeline
      storage: dbfs:/mnt/{storage[env]}-datalake-rw/bbu-{sword}-datalake
      configuration:
        pipelines.tableManagedByMultiplePipelinesCheck.enabled: false 
        pipelines.clusterShutdown.delay: {shutdown_delay}
      clusters:
      - label: default 
        node_type_id: Standard_D13_v2
        driver_node_type_id: Standard_D13_v2 
        custom_tags:
          job_name: dlt-maintainence
          workspace: {env}
        autoscale:
          min_workers: 1
          max_workers: 9
          mode: ENHANCED
      - label: updates
        node_type_id: {dlt_nodes["dp"][1]}
        driver_node_type_id: {dlt_nodes["dp"][0]}
        custom_tags:
          job_name: dlt_gold_dp_pipeline
          workspace: {env}
          purpose: {purpose}
          deployer: {deployer}
        autoscale:
          min_workers: {dlt_workers["dp"][0]}
          max_workers: {dlt_workers["dp"][1]}
          mode: ENHANCED
      libraries:
      - file:
          path: {pipeline_root}gold/demand_planning.py
      notifications:
        {notify}
      continuous: false
      development: {development}
      photon: {photon}
      edition: {edition}
      channel: {channel}""")
fil.close()
