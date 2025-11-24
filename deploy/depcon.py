spv="14.3.x-scala2.12" 
branches={"stage":"develop",
          "QA":"develop",
          "PROD":"prod",
          "DR":"prod"}
nodes={"dp":"Standard_E16ads_v5", #dataprep, cust_clean
       "dpd":"Standard_E4ads_v5", #dataprep driver, model driver
       "md":"Standard_D8ads_v5", #model, suggo
       "dy":"Standard_D16ads_v5", #daily, report
       "ot":"Standard_D8d_v4", #clean
       "dv":"Standard_D4ads_v5", #driver
       "e6":"Standard_E16pds_v6",
       "d6":"Standard_D16pds_v6",
       "dv6":"Standard_D4pds_v6"
       }
storage={"stage":"bbudatamartstdstage",
         "QA":"bbudatamartstdqa",
         "PROD":"bbudatamartstd",
         "DR":"bbudatamartstddr"}
policies={"stage":"D06332FEEB003393",
          "QA":"D0635B4C430006B6",
          "PROD":"D0635B4C43000237",
          "DR":"00135C56528FB76A"}
repo="antuit-esp_ai_proj_bbu"
#these are the currently believed most efficient choices for number of workers [import,models,modela,daily]
workers={0:[2,6,7,3],1:[1,1,1,1],2:[2,10,1,2],3:[1,4,2,1],4:[7,11,7,4],5:[5,7,12,4],6:[1,2,1,1]}
user="ryan.mullen@zebra.com"
file_root="DS/forecasting/"
pipeline_root="/Repos/bbu_pipeline/antuit-esp_ai_proj_bbu/ETL/workflows/"
adhoc_root="DS/adhoc/"
libs=["/Volumes/prod-libraries/default/libraries/ai_core_library/ai_core_library-2.4.9+g98cff78-cp310-cp310-linux_x86_64.whl",
      "/Volumes/prod-libraries/default/libraries/adf_library/adf_library-3.0.2-cp310-cp310-linux_x86_64.whl"]
dlts={"QA":{"silver":"5f907334-954c-4d5e-9dbd-c120fec28d2b",
            "ds":"9b92fc58-fcc1-4933-a70f-29b96d1f2980",
            "dp":"079bac3f-d8b6-4ac2-8c2c-d9d63f99938a",
            "ion":"1cf29594-9889-4aa1-b831-b2c59788c783",
            "forecast":"59990572359332"},
       "PROD":{"silver":"95af1aff-ba14-4c0b-bda9-54f024403a39",
            "ds":"d72c6848-cb7e-47d7-9fd6-12490059b95e",
            "dp":"addcc1a1-0838-4379-a70b-2d0c2cd7f711",
            "ion":"8e44d4b1-981b-4cb2-b4aa-2d0be4ba58ca",
            "forecast":"58445534971262"},
       "stage":{"silver":"5990292b-b414-4786-8b0b-c1c9fca78b23",
            "ds":"09c7164c-0c14-4aab-8b3d-5d62ec924068",
            "dp":"38104ae4-a15b-4cf4-a1d3-cb077f40e7a1",
            "ion":"74054c8e-6239-48ce-82fe-711c44778e29",
            "forecast":"882078985443773"},
       "DR":{"silver":"6a971a0d-ff34-4a40-8e3b-eba8109041f2",
            "ds":"ccaa15a9-c756-46f2-aa64-66fab91c39f7",
            "dp":"6d7b2b97-8111-4410-a329-bf8206b395f5",
            "ion":"d9187d49-66ba-47cf-a920-d2b7fcd248b4",
            "forecast":"353730291908429"}}

access_control="""- user_name: ryan.mullen@zebra.com
        permission_level: IS_OWNER
      - group_name: Level2
        permission_level: CAN_MANAGE
      - group_name: Monitors
        permission_level: CAN_MANAGE"""

notify="""- email_recipients:
            - ranjith.kl@zebra.com
          alerts:
            - on-update-success
            - on-update-failure
            - on-update-fatal-failure
            - on-flow-failure"""

dlt_workers={"ds":[5,21],
             "dp":[1,16],
             "ion":[4,16],
             "ag":[3,15],
             "agd":[1,2]}

dlt_nodes={"ds":["Standard_D16ads_v5","Standard_D16ads_v5"],
             "dp":["Standard_D16ads_v5","Standard_D16ads_v5"],
             "ion":["Standard_D16ads_v5","Standard_D16ads_v5"],
             "ag":["Standard_D8ads_v5","Standard_D16ads_v5"],
             "agd":["Standard_D4ads_v5","Standard_D4ads_v5"]}
