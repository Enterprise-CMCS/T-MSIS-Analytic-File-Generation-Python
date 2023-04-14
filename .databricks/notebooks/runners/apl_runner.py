# Databricks notebook source
from taf.APL.APL_Runner import APL_Runner

# COMMAND ----------

apl = APL_Runner(da_schema        = dbutils.widgets.get("da_schema")
                ,reporting_period = dbutils.widgets.get("reporting_period")
                ,state_code       = dbutils.widgets.get("state_code")
                ,run_id           = dbutils.widgets.get("run_id")
                ,job_id           = dbutils.widgets.get("job_id")
                ,file_version     = dbutils.widgets.get("file_version"))

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

#TABLE_NAME = "LCTN"
#FIL_4TH_NODE = "LCM"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "SAREA"
#FIL_4TH_NODE = "SAM"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "ENRLMT"
#FIL_4TH_NODE = "EPM"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "OA"
#FIL_4TH_NODE = "OAM"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

#TABLE_NAME = "BASE"
#FIL_4TH_NODE = "BSM"
 
#de.get_ann_count(TABLE_NAME)
#de.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#de.create_efts_metadata()

# COMMAND ----------

apl.job_control_updt2()
