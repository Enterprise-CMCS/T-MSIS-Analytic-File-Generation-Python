# Databricks notebook source
from taf.BSF.BSF_Runner import BSF_Runner

# COMMAND ----------

from taf.BSF.BSF_Metadata import BSF_Metadata

# COMMAND ----------

bsf = BSF_Runner(da_schema        = dbutils.widgets.get("da_schema")
                ,reporting_period = dbutils.widgets.get("reporting_period")
                ,state_code       = dbutils.widgets.get("state_code")
                ,run_id           = dbutils.widgets.get("run_id")
                ,job_id           = dbutils.widgets.get("job_id")
                ,file_version     = dbutils.widgets.get("file_version"))

# COMMAND ----------

bsf.job_control_wrt("MBSF")

# COMMAND ----------

bsf.job_control_updt()

# COMMAND ----------

bsf.print()

# COMMAND ----------

bsf.init()

# COMMAND ----------

bsf.run()   

# COMMAND ----------

bsf.view_plan()

# COMMAND ----------

display(bsf.audit())

# COMMAND ----------

sqlUnified = BSF_Metadata.unifySelect(bsf)

# COMMAND ----------

print(sqlUnified)

# COMMAND ----------

spark.sql(sqlUnified)

# COMMAND ----------

sqlInsert = BSF_Metadata.finalTableOutput(bsf)

# COMMAND ----------

spark.sql(sqlInsert)

# COMMAND ----------

bsf.job_control_updt2()

# COMMAND ----------

TABLE_NAME = "TAF_MON_BSF"
FIL_4TH_NODE = "BSF"
 
bsf.get_cnt(TABLE_NAME)
bsf.getcounts("023_bsf_ELG00023", "0.1. create initial table")
bsf.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
bsf.create_eftsmeta_info(TABLE_NAME, "023_bsf_ELG00023", "0.1. create initial table", f"BSF_{bsf.RPT_OUT}_{bsf.BSF_FILE_DATE}", "submtg_state_cd")
bsf.file_contents(TABLE_NAME)
