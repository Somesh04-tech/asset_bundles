import os
#depcon, deployment config, houses the settings that don't change frequently
from depcon import spv, nodes, workers, user, file_root, storage, repo, policies
#to make modifaction quicker
T,F,N=True,False,None

#which environment?  "stage", "QA", or "PROD"
env="stage"

#You can just create the deployment.yaml or you could deploy and or launch job as well.
deploy=T
launch=F

#output location
personal_root=f"dbfs:/mnt/{storage[env]}-data-science-store-rw/ryan/"

#you can choose to run only part of the process (Not running something implies you already ran it and you don't want 
# to run it again.  The tasks themselves aren't independent.)
cust_clean=F
prepd=T
prepw=T
report=F
model=T
daily=T
suggo=F

#####settings
#type is either "test", "prod_inputs", or "full_simulation".  You don't have to supply a value for type, it is just there
#for frequent use cases to aid in parameter setting.  
type="test"

#job_name, this is what the workflow will be called in databricks so be careful not to use names that are currently
#being used for other things.  It's best to pick a name once and keep using that.  However, if you want to run 
#different simulations simultaneously they need different names.
job_name="testing"

#autopush runs the push to origin automatically so you don't have to remember. It will commit all SAVED differences.
#this assumes the branch you are actively using is the branch you want to run in Databricks.
autopush=F
msg="temporary push for DB"
#Databricks will run from the github version of this branch NOT your local version.

#This code retrieves your active branch name.
branch_name=os.popen("git rev-parse --abbrev-ref HEAD").read()
#or you can manually specify any branch
branch_name="develop"

#for testing purposes you can filter to subsets or run historical frcdts
dows=[2] #Which calendars do you want to run?

#if your subset is very restrictive then update the amount of workers for each part
#workers.update({2:[1,1,1,5]}) 

#you can further subset using any customer level filter. None means no filter
filter="week_day_no=2"  

#which forecast creation id do you want to run? None means run based on the available input data
fid=1410

#do you want to use week level forecast inputs from production (T), or are you going to create your own (F) in this case inputs/outputs are stored in DS_STORE_ROOT
useprodw=F
#do you want to use day level forecast inputs from production (T), or are you going to create your own (F) in this case inputs/outputs are stored in DS_STORE_ROOT
#There is almost never a reason to rerun the day level dataprep for analytical purposes.  It is very costly and takes time.  Just don't do it.
useprodd=F

#do you want to let DS_STORE_ROOT dicate where the outputs go (N), or give a specific path
#give a different path here (for every simulation) if you want to repeatedly use the same input
altermediate=f"{personal_root}base/"

###There is probably no need to modify anything below here.  Unless you know what you are doing or some path is wrong.
#or you want to modify spark settings, etc.

#######---------------------------settings done--------------------------------############

#for certain use cases the code below will overwrite any settings supplied earlier, this is to prevent careless mistakes
#use case: testing pipeline, this is a good way to do a robust test for whatever changes you want to push to production
if type=="test":
  job_name="forecast_test"
  env="stage"
  altermediate=N
  useprodw=F
  useprodd=T
  fid=N
  filter=N
  dows=[1]

#use case: use prod dataprep make sure altermediate is defined and you have write access to it.
if type=="prod inputs":
  env="stage"
  useprodw=T

#use case: fresh simulation change ds_store_root in config.ini and push to branch specified above
if type=="full simulation":
  env="stage"
  useprodd=F
  useprodw=F

if useprodd:
   cust_clean,prepd=F,F
if useprodw:
   prepw,prepd=F,F

inits=f"/Repos/official/{repo}/config/library.sh"

#This creates the deployment file Editing below here is to be done with extreme caution
fil = open(f"C:\DataBricks\\{repo}\\deploy\\temp.yaml", "w")
fil.write(f"""build:
  no_build: true
environments:
  {env}:
    workflows:
    - name: {job_name}
      tags:
        job_name: {job_name}
        workspace: {env}
        team: bbu
      git_source:
        git_url: https://github.com/zebratechnologies/{repo}.git
        git_provider: github
        git_branch: {branch_name}
      access_control_list:
      - user_name: {user}
        permission_level: IS_OWNER
      - group_name: Level2
        permission_level: CAN_MANAGE_RUN
      tasks:""")
if cust_clean:
  fil.write(f"""
      - task_key: cust_cleanse
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          policy_id: {policies[env]}
          custom_tags:
            task_name: cust_cleanse
            task_type: preamble
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['dp']}
          driver_node_type_id: {nodes['dv']}
          num_workers: 3
          spark_conf:
            spark.sql.shuffle.partitions: 192
            spark.hadoop.fs.azure.delete.threads: 20
        spark_python_task:
          python_file: {file_root}dowager.py
          source: GIT""")
if prepd:
  fil.write(f"""
      - task_key: import_day
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          policy_id: {policies[env]}
          custom_tags:
            task_name: import_day
            task_type: import
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['dp']}
          driver_node_type_id: {nodes['dpd']}
          num_workers: 10
          spark_conf:
            spark.sql.shuffle.partitions: {64*10}
            spark.hadoop.fs.azure.delete.threads: 20
            spark.databricks.io.cache.enabled: false
            spark.sql.autoBroadcastJoinThreshold: 50M
        spark_python_task:
          python_file: {file_root}dataprepdy.py
          source: GIT
          parameters:""")
  if filter is not None:
            fil.write(f"""
          - --filter
          - {filter}""")
  if fid is not None:
            fil.write(f"""
          - --fid
          - {fid}""")
for dow in dows:
    if prepw:
        fil.write(f"""
      - task_key: import{dow}""")
        if prepd:
          fil.write(f"""
        depends_on:
        - task_key: import_day""")
        fil.write(f"""
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          policy_id: {policies[env]}
          custom_tags:
            task_name: import{dow}
            task_type: import
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['dp']}
          driver_node_type_id: {nodes['dpd']}
          num_workers: {workers[dow][0]}
          spark_conf:
            spark.sql.shuffle.partitions: {64*workers[dow][0]}
            spark.hadoop.fs.azure.delete.threads: 20
            spark.databricks.io.cache.enabled: false
            spark.sql.autoBroadcastJoinThreshold: 50M
          init_scripts:
            workspace: 
              destination: {inits}
        spark_python_task:
          python_file: {file_root}dataprepwk.py
          source: GIT
          parameters:
          - --dow
          - {dow}""")
        if useprodd:
          fil.write(f"""
          - --fcst_input
          - prod""")
        if fid is not None:
            fil.write(f"""
          - --fid
          - {fid}""")
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
        fil.write(f"""
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          policy_id: {policies[env]}
          custom_tags:
            task_name: model{dow}s
            task_type: model
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['md']}
          driver_node_type_id: {nodes['dpd']}
          num_workers: {workers[dow][1]}
          spark_conf:
            spark.sql.shuffle.partitions: {96*workers[dow][1]}
            spark.hadoop.fs.azure.delete.threads: 20
            spark.databricks.delta.optimizeWrite.enabled: false
            spark.databricks.delta.autoCompact.enabled: false
            spark.sql.adaptive.enabled: false
          init_scripts:
            workspace: 
              destination: {inits}
        spark_python_task:
          python_file: {file_root}runner.py
          source: GIT
          parameters:
          - --aos
          - scan""")
        if useprodw:
          fil.write(f"""
          - --fcst_input
          - prod""")
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
        fil.write(f"""
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          policy_id: {policies[env]}
          custom_tags:
            task_name: model{dow}a
            task_type: model
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['md']}
          driver_node_type_id: {nodes['dpd']}
          num_workers: {workers[dow][2]}
          spark_conf:
            spark.sql.shuffle.partitions: {48*workers[dow][2]}
            spark.hadoop.fs.azure.delete.threads: 20
            spark.databricks.delta.optimizeWrite.enabled: false
            spark.databricks.delta.autoCompact.enabled: false
            spark.sql.adaptive.enabled: false
          init_scripts:
            workspace: 
              destination: {inits}
        spark_python_task:
          python_file: {file_root}runner.py
          source: GIT
          parameters:
          - --aos
          - aged""")
        if useprodw:
          fil.write(f"""
          - --fcst_input
          - prod""")
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
        fil.write(f"""
        new_cluster:
          spark_version: {spv}
          runtime_engine: STANDARD
          policy_id: {policies[env]}
          custom_tags:
            task_name: daily{dow}
            task_type: daily
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['dy']}
          driver_node_type_id: {nodes['dv']}
          num_workers: {workers[dow][3]}
          spark_conf:
            spark.sql.shuffle.partitions: {48*workers[dow][3]}
            spark.hadoop.fs.azure.delete.threads: 20
          init_scripts:
            workspace: 
              destination: {inits}
        spark_python_task:
          python_file: {file_root}daily.py
          source: GIT
          parameters:""")
        if useprodw:
          fil.write(f"""
          - --fcst_input
          - prodw""")
        elif useprodd:
          fil.write(f"""
          - --fcst_input
          - prodd""")
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
          policy_id: {policies[env]}
          custom_tags:
            task_name: suggo
            task_type: post
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['ot']}
          driver_node_type_id: {nodes['dv']}
          num_workers: 30
          spark_conf:
            spark.sql.shuffle.partitions: 480
            spark.hadoop.fs.azure.delete.threads: 20
          init_scripts:
            workspace: 
              destination: /Repos/official/antuit-esp_ai_proj_bbu/config/librarysuggs.sh
        spark_python_task:
          python_file: {file_root}suggo.py
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
          policy_id: {policies[env]}
          custom_tags:
            task_name: report
            task_type: post
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['dy']}
          driver_node_type_id: {nodes['dv']}
          num_workers: 8
          spark_conf:
            spark.sql.shuffle.partitions: 512
            spark.hadoop.fs.azure.delete.threads: 20
        spark_python_task:
          python_file: {file_root}report.py
          source: GIT""")
fil.close()

if deploy:
    rc=os.system(f"dbx deploy --jobs {job_name} --deployment-file deploy/temp.yaml -e {env}")
    os.system(f"del C:\DataBricks\\{repo}\\deploy\\temp.yaml")
if autopush:
   os.system(f'git commit -a -m "{msg}"')
   os.system("git push origin HEAD")
if launch & ~rc:
    os.system(f"dbx launch --job {job_name} -e {env}")