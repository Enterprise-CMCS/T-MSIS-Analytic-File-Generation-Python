# Databricks notebook source
from taf.PRV.PRV_Runner import PRV_Runner

# COMMAND ----------

from taf.PRV.PRV_Metadata import PRV_Metadata

# COMMAND ----------

prv = PRV_Runner(da_schema        = dbutils.widgets.get("da_schema")
                ,reporting_period = dbutils.widgets.get("reporting_period")
                ,state_code       = dbutils.widgets.get("state_code")
                ,run_id           = dbutils.widgets.get("run_id")
                ,job_id           = dbutils.widgets.get("job_id"))

# COMMAND ----------

prv.job_control_wrt("PRV")

# COMMAND ----------

prv.job_control_updt()

# COMMAND ----------

prv.print()

# COMMAND ----------

prv.init()

# COMMAND ----------

prv.run()   

# COMMAND ----------

prv.view_plan()

# COMMAND ----------

display(prv.audit())

# COMMAND ----------

prv.job_control_updt2()

# COMMAND ----------

TABLE_NAME = "TAF_PRV"
FIL_4TH_NODE = "PBS"
 
prv.get_cnt(TABLE_NAME)
prv.getcounts("101_prvdr_build.sas", "base_Prov")
prv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
prv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "base_Prov", "Prov02_Base", "submtg_state_cd")
prv.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_PRV_LOC"
FIL_4TH_NODE = "PLO"
 
prv.get_cnt(TABLE_NAME)
prv.getcounts("101_prvdr_build.sas", "constructed_3_Prov03")
prv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
prv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_3_Prov03", "Prov03_Location_CNST", "submtg_state_cd")
prv.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_PRV_LIC"
FIL_4TH_NODE = "PLI"
 
prv.get_cnt(TABLE_NAME)
prv.getcounts("101_prvdr_build.sas", "constructed_Prov04")
prv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
prv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov04", "Prov04_Licensing_CNST", "submtg_state_cd")
prv.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_PRV_IDT"
FIL_4TH_NODE = "PID"
 
prv.get_cnt(TABLE_NAME)
prv.getcounts("101_prvdr_build.sas", "constructed_Prov05")
prv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
prv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov05", "Prov05_Identifiers_CNST", "submtg_state_cd")
prv.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_PRV_TAX"
FIL_4TH_NODE = "PTX"
 
prv.get_cnt(TABLE_NAME)
prv.getcounts("101_prvdr_build.sas", "constructed_Prov06")
prv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
prv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov06", "Prov06_Taxonomies_seg", "submtg_state_cd")
prv.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_PRV_ENR"
FIL_4TH_NODE = "PEN"
 
prv.get_cnt(TABLE_NAME)
prv.getcounts("101_prvdr_build.sas", "segment_Prov07")
prv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
prv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "segment_Prov07", "Prov07_Medicaid_ENROP", "submtg_state_cd")
prv.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_PRV_GRP"
FIL_4TH_NODE = "PAG"
 
prv.get_cnt(TABLE_NAME)
prv.getcounts("101_prvdr_build.sas", "constructed_Prov08")
prv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
prv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov08", "Prov08_Groups_CNST", "submtg_state_cd")
prv.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_PRV_PGM"
FIL_4TH_NODE = "PAP"
 
prv.get_cnt(TABLE_NAME)
prv.getcounts("101_prvdr_build.sas", "constructed_Prov09")
prv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
prv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov09", "Prov09_AffPgms_CNST", "submtg_state_cd")
prv.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_PRV_BED"
FIL_4TH_NODE = "PBT"
 
prv.get_cnt(TABLE_NAME)
prv.getcounts("101_prvdr_build.sas", "constructed_Prov10")
prv.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
prv.create_eftsmeta_info(TABLE_NAME, "101_prvdr_build.sas", "constructed_Prov10", "Prov10_BedType_CNST", "submtg_state_cd")
prv.file_contents(TABLE_NAME)
