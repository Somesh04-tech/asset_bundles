import os
from pathlib import Path

#Set target to dev for running ad hoc pipelines, when doing so please limit the groups to only what you are working on.
#Otherwise target is stage or PROD.  Running this code will deploy everything according to how the LOCAL versions of your deployment scripts are configured.

target = "PROD"
groups = ["fcst","dlts","weekly","daily","biannual"]

#Users Laptop file location:
local_root=f"{Path(__file__).parent.parent}\\deploy\\"
yml_root=f"{Path(__file__).parent.parent}\\resources\\"
#This is specific to the user's laptop it won't work out of the box.
python_root = "C:\\Users\\rm5851\\AppData\\Local\\Microsoft\\WindowsApps\\python3.11.exe"

#If the target is dev this means the environment is stage, otherwise the target and env are equivalent
env={"dev":"stage"}.get(target,target)

#The profile name -env mapping is user specific please modify for according to your databricks.cfg
profile={"stage":"BBUspn","QA":"BBUqa","PROD":"BBUpspn","DR":"BBUproddr"}.get(env,"BBU")

#Create Ymls

for group in groups:
    os.system(f"{python_root} {local_root}{group}.py --env {env}")

#deploy via asset bundles

#os.system(f'$env:PATH += ";C:\\Users\\rm5851\\AppData\\Local\\Microsoft\\WinGet\\Packages\\Databricks.DatabricksCLI_Microsoft.Winget.Source_8wekyb3d8bbwe"')
os.system(f"cd asset_bundles && databricks bundle deploy -t {target} --profile={profile}")

#delete ymls
os.system(f"del {yml_root}*.yml")