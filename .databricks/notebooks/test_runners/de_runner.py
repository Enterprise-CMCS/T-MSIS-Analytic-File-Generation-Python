# Databricks notebook source
from taf.DE.DE_Runner import DE_Runner

# COMMAND ----------

de = DE_Runner(da_schema        = dbutils.widgets.get("da_schema")
              ,reporting_period = dbutils.widgets.get("reporting_period")
              ,state_code       = dbutils.widgets.get("state_code")
              ,run_id           = dbutils.widgets.get("run_id")
              ,job_id           = dbutils.widgets.get("job_id"))

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

#TABLE_NAME = "ELDTS"
#FIL_4TH_NODE = "DTS"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "CNTCT_DTLS"
#FIL_4TH_NODE = "ADR"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "MC"
#FIL_4TH_NODE = "MCR"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "WVR"
#FIL_4TH_NODE = "WVR"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "MFP"
#FIL_4TH_NODE = "MFP"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "HHSPO"
#FIL_4TH_NODE = "HHSPO"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "DSBLTY"
#FIL_4TH_NODE = "DSB"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "BASE"
#FIL_4TH_NODE = "BSE"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

de.job_control_updt2()
