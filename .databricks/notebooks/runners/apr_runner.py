# Databricks notebook source
from taf.APR.APR_Runner import APR_Runner

# COMMAND ----------

apr = APR_Runner(da_schema        = dbutils.widgets.get("da_schema")
                ,reporting_period = dbutils.widgets.get("reporting_period")
                ,state_code       = dbutils.widgets.get("state_code")
                ,run_id           = dbutils.widgets.get("run_id")
                ,job_id           = dbutils.widgets.get("job_id")
                ,file_version     = dbutils.widgets.get("file_version"))

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

#TABLE_NAME = "LCTN"
#FIL_4TH_NODE = "LCP"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "LCNS"
#FIL_4TH_NODE = "LIC"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "ID"
#FIL_4TH_NODE = "IDP"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "TXNMY"
#FIL_4TH_NODE = "TAX"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "ENRLMT"
#FIL_4TH_NODE = "ENP"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "GRP"
#FIL_4TH_NODE = "GRP"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "PGM"
#FIL_4TH_NODE = "PGM"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "BED"
#FIL_4TH_NODE = "BED"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "BASE"
#FIL_4TH_NODE = "BSP"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

apr.job_control_updt2()
