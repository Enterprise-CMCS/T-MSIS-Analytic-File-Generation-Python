# Databricks notebook source
from taf.APL.APL_Runner import APL_Runner

# COMMAND ----------

apl = APL_Runner(da_schema        = dbutils.widgets.get("da_schema")
                ,reporting_period = dbutils.widgets.get("reporting_period")
                ,state_code       = dbutils.widgets.get("state_code")
                ,run_id           = dbutils.widgets.get("run_id")
                ,job_id           = dbutils.widgets.get("job_id")
                ,file_version     = dbutils.widgets.get("file_version")
                ,run_stats_only   = dbutils.widgets.get("run_stats_only"))

# COMMAND ----------

apl.job_control_wrt("APL")

# COMMAND ----------

apl.job_control_updt()

# COMMAND ----------

apl.print()

# COMMAND ----------

apl.init()

# COMMAND ----------

apl.run()   

# COMMAND ----------

apl.view_plan()

# COMMAND ----------

display(apl.audit())

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PL_BASE"
FIL_4TH_NODE = "BSM"
 
apl.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apl.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PL_ENRLMT"
FIL_4TH_NODE = "EPM"
 
apl.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apl.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PL_LCTN"
FIL_4TH_NODE = "LCM"
 
apl.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apl.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PL_OA"
FIL_4TH_NODE = "OAM"
 
apl.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apl.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_PL_SAREA"
FIL_4TH_NODE = "SAM"
 
apl.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
apl.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

apl.job_control_updt2()
