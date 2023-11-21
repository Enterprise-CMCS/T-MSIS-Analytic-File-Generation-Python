# Databricks notebook source
from taf.APR.APR_Runner import APR_Runner

# COMMAND ----------

apr = APR_Runner(da_schema        = dbutils.widgets.get("da_schema")
                ,reporting_period = dbutils.widgets.get("reporting_period")
                ,state_code       = dbutils.widgets.get("state_code")
                ,run_id           = dbutils.widgets.get("run_id")
                ,job_id           = dbutils.widgets.get("job_id")
                ,file_version     = dbutils.widgets.get("file_version")
                ,run_stats_only   = dbutils.widgets.get("run_stats_only"))

# COMMAND ----------

for p in apr.preplan:
  print(p)

# COMMAND ----------

apr.job_control_wrt("APR")

# COMMAND ----------

apr.job_control_updt()

# COMMAND ----------

apr.print()

# COMMAND ----------

apr.init()

# COMMAND ----------

apr.run()

# COMMAND ----------

apr.view_plan()

# COMMAND ----------

display(apr.audit())

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PR_BASE"
FIL_4TH_NODE = "BSP"
 
apr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apr.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PR_BED"
FIL_4TH_NODE = "BED"
 
apr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apr.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PR_ENRLMT"
FIL_4TH_NODE = "ENP"
 
apr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apr.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PR_GRP"
FIL_4TH_NODE = "GRP"
 
apr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apr.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PR_ID"
FIL_4TH_NODE = "IDP"
 
apr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apr.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PR_LCNS"
FIL_4TH_NODE = "LIC"
 
apr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apr.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PR_LCTN"
FIL_4TH_NODE = "LCP"
 
apr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apr.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PR_PGM"
FIL_4TH_NODE = "PGM"
 
apr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apr.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PR_TXNMY"
FIL_4TH_NODE = "TAX"
 
apr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apr.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

apr.job_control_updt2()
