# Databricks notebook source
from taf.DE.DE_Runner import DE_Runner

# COMMAND ----------

de = DE_Runner(da_schema        = dbutils.widgets.get("da_schema")
              ,reporting_period = dbutils.widgets.get("reporting_period")
              ,state_code       = dbutils.widgets.get("state_code")
              ,run_id           = dbutils.widgets.get("run_id")
              ,job_id           = dbutils.widgets.get("job_id")
              ,file_version     = dbutils.widgets.get("file_version")
              ,run_stats_only   = dbutils.widgets.get("run_stats_only"))

# COMMAND ----------

de.job_control_wrt("ADE")

# COMMAND ----------

de.job_control_updt()

# COMMAND ----------

de.print()

# COMMAND ----------

de.init()

# COMMAND ----------

de.run()

# COMMAND ----------

de.view_plan()

# COMMAND ----------

display(de.audit())

# COMMAND ----------

TABLE_NAME = "TAF_ANN_DE_BASE"
FIL_4TH_NODE = "BSE"
 
de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
de.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_DE_CNTCT_DTLS"
FIL_4TH_NODE = "ADR"
 
de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
de.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_DE_DSBLTY"
FIL_4TH_NODE = "DSB"
 
de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
de.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_DE_ELDTS"
FIL_4TH_NODE = "DTS"
 
de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
de.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_DE_HHSPO"
FIL_4TH_NODE = "HSP"
 
de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
de.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_DE_MC"
FIL_4TH_NODE = "MCR"
 
de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
de.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_DE_MFP"
FIL_4TH_NODE = "MFP"
 
de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
de.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

TABLE_NAME = "TAF_ANN_DE_WVR"
FIL_4TH_NODE = "WVR"
 
de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
de.create_efts_metadata(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

de.job_control_updt2()
