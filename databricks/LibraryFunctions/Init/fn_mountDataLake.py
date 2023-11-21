# Databricks notebook source
data_lake_name = "adlsg2dev001"

# COMMAND ----------

def fn_mountDataLake(layersToMount=None):
    if layersToMount is None:
        layersToMount = ["raw", "cleansed", "pretransformed", "transformed", "curated", "snapshot"]
    
    configs = {
        "fs.azure.account.auth.type": "CustomAccessToken",
        "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
    }
    
    for layer in layersToMount:
        try:
            mount_point_name = f'/mnt/{layer}'

            dbutils.fs.mount(
            source = f"abfss://{layer}@{data_lake_name}.dfs.core.windows.net/",
            mount_point = mount_point_name,
            extra_configs = configs)
            print(f"'{mount_point_name}' is successfully mounted.")
        except Exception as e:
            if "Directory already mounted" in str(e):
                print(f"Directory - '{mount_point_name}' mount point already exists!")

# COMMAND ----------

def fn_setADLSGen2AccessConf(env="dev"):
    key_vault_scope = "az-kv-secret-scope-001"
    #data_lake_key_secret_name = "datalake-gen2-acc-key"
    #data_lake_key = dbutils.secrets.get(scope=key_vault_scope, key=data_lake_key_secret_name)
    #spark.conf.set(f"fs.azure.account.key.{data_lake_name}.dfs.core.windows.net",  data_lake_key)


    data_lake_sas_key_secret_name = "adlsgen2-sas-key"
    data_lake_sas_key = dbutils.secrets.get(scope=key_vault_scope, key=data_lake_sas_key_secret_name)
    spark.conf.set(f"fs.azure.account.auth.type.{data_lake_name}.dfs.core.windows.net", "SAS")
    spark.conf.set(f"fs.azure.sas.token.provider.type.{data_lake_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(f"fs.azure.sas.fixed.token.{data_lake_name}.dfs.core.windows.net", data_lake_sas_key)

# COMMAND ----------

fn_mountDataLake()

# COMMAND ----------

fn_setADLSGen2AccessConf()

# COMMAND ----------

raw_layer_root_path = f"abfss://raw@{data_lake_name}.dfs.core.windows.net/"
cleansed_layer_root_path = f"abfss://cleansed@{data_lake_name}.dfs.core.windows.net/"
pre_transformed_layer_root_path = f"abfss://pretransformed@{data_lake_name}.dfs.core.windows.net/"
transformed_layer_root_path = f"abfss://transformed@{data_lake_name}.dfs.core.windows.net/"
curated_layer_root_path = f"abfss://curated@{data_lake_name}.dfs.core.windows.net/"
snapshot_layer_root_path = f"abfss://snapshot@{data_lake_name}.dfs.core.windows.net/"

# COMMAND ----------

init_variables = {"raw_path": raw_layer_root_path,
                  "cleansed_path": cleansed_layer_root_path,
                  "pretrans_path": pre_transformed_layer_root_path,
                  "trans_path": transformed_layer_root_path,
                  "curated_path": curated_layer_root_path,
                  "snapshot_path": snapshot_layer_root_path
                  }

# COMMAND ----------

print("Initialised Variables:\n-------------------------------------------------------------")
for _ in init_variables.keys(): print(f"{_} :\t{init_variables[_]}")

print("\n\n\n\n")

print('Individual variable names:\n--------------------------------------------------------------"')
print("Raw Layer Root Path:\traw_layer_root_path")
print("Cleansed Layer Root Path:\tcleansed_layer_root_path")
print("PreTransformed Layer Root Path:\tpre_transformed_layer_root_path")
print("Transformed Layer Root Path:\ttransformed_layer_root_path")
print("Curated Layer Root Path:\tcurated_layer_root_path")
print("Snapshot Layer Root Path:\tsnapshot_layer_root_path")
