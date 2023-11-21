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
                ,file_version     = dbutils.widgets.get("file_version")
                ,run_stats_only   = dbutils.widgets.get("run_stats_only"))

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

# Only run this insert cell if we're doing a normal run. Otherwise, skip it
if run_stats_only == 0:
    spark.sql(sqlInsert)

# COMMAND ----------

bsf.job_control_updt2()

# COMMAND ----------

TABLE_NAME = "TAF_MON_BSF"
FIL_4TH_NODE = "BSF"
FILETYP = "BSF"

meta_view_nm = bsf.prep_meta_table("023_bsf_ELG00023", "0.1. create_initial_table", FILETYP, FILETYP, FILETYP, bsf.RPT_OUT, TABLE_NAME, bsf.BSF_FILE_DATE)
bsf.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
bsf.create_eftsmeta_info(TABLE_NAME, "023_bsf_ELG00023", "0.1. create_initial_table", "BSF_STEP1", "submtg_state_cd", meta_view_name=meta_view_nm)
bsf.file_contents(TABLE_NAME)
