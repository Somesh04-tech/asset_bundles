import os
from depcon import spv, nodes, workers, user
#settings
job_name="holdout"
env="stage"
branch_name="develop"
file_root="Workflows/forecasting/"
altermediate="dbfs:/mnt/bbudatamartstdstage-data-science-store-rw/ryan/hold/"
fiscal_start=range(0,7)
filter="1==1"
aos="scan"
inits=f"/Repos/official/antuit-esp_ai_proj_bbu/config/library.sh"
week_ids=range(1385,1389)
deploy=True
launch=False
#end settings

fil = open(f"C:\DataBricks\\antuit-esp_ai_proj_bbu\\deploy\\temp.yaml", "w")
fil.write(f"""build:
  no_build: true
environments:
  {env}:
    workflows:
    - name: {job_name}
      tags:
        forecasting_model: holdout
        environment: {env}
        team: bbu
      git_source:
        git_url: https://github.com/zebratechnologies/antuit-esp_ai_proj_bbu.git
        git_provider: github
        git_branch: {branch_name}
      access_control_list:
      - user_name: {user}
        permission_level: IS_OWNER
      - group_name: Level2
        permission_level: CAN_MANAGE_RUN
      tasks:\n""")
for dow in fiscal_start:
  for wk in week_ids:
      fil.write(f"""      - task_key: week{dow}{wk}
        new_cluster:
          spark_version: {spv}
          custom_tags:
            task_name: model{dow}s
            task_type: model
            job_name: {job_name}
            workspace: {env}
          node_type_id: {nodes['md']}
          driver_node_type_id: {nodes['ot']}
          num_workers: {workers[dow][1]}
          spark_conf:
            spark.sql.shuffle.partitions: {32*workers[dow][1]}
            spark.hadoop.fs.azure.delete.threads: 20
          init_scripts:
            workspace: 
              destination: {inits}
        spark_python_task:
          python_file: {file_root}runner.py
          source: GIT
          parameters:
          - --alt_out
          - {altermediate}
          - --aos
          - {aos} 
          - --dow
          - {dow}
          - --fcst_input
          - qa
          - --fid
          - {wk}\n""")
fil.close()

if deploy:
    rc = os.system(f"dbx deploy --jobs {job_name} --deployment-file deploy/temp.yaml -e {env}")
    os.system(f"del C:\DataBricks\\antuit-esp_ai_proj_bbu\\deploy\\temp.yaml")
if launch & ~rc:
    os.system(f"dbx launch --job {job_name} -e {env}")