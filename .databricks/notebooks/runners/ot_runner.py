# Databricks notebook source
from taf.OT.OT_Runner import OT_Runner

# COMMAND ----------

from taf.OT.OT_Metadata import OT_Metadata

# COMMAND ----------

otr = OT_Runner(da_schema        = dbutils.widgets.get("da_schema")
               ,reporting_period = dbutils.widgets.get("reporting_period")
               ,state_code       = dbutils.widgets.get("state_code")
               ,run_id           = dbutils.widgets.get("run_id")
               ,job_id           = dbutils.widgets.get("job_id")
               ,file_version     = dbutils.widgets.get("file_version"))

# COMMAND ----------

otr.job_control_wrt("TOT")

# COMMAND ----------

otr.job_control_updt()

# COMMAND ----------

otr.print()

# COMMAND ----------

otr.init()

# COMMAND ----------

otr.run()

# COMMAND ----------

otr.view_plan()

# COMMAND ----------

display(otr.audit())

# COMMAND ----------

otr.job_control_updt2()

# COMMAND ----------

TABLE_NAME = "TAF_OTH"
FIL_4TH_NODE = "OTH"
 
otr.get_cnt(TABLE_NAME)
otr.getcounts("AWS_OT_MACROS", "1.1 AWS_Extract_Line_OT")
otr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
otr.create_eftsmeta_info(TABLE_NAME, "AWS_OT_MACROS", "1.1 AWS_Extract_Line_OT", "OT_HEADER", "new_submtg_state_cd")
otr.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_OTL"
FIL_4TH_NODE = "OTL"
 
otr.get_cnt(TABLE_NAME)
otr.getcounts("AWS_OT_MACROS", "1.1 AWS_Extract_Line_OT")
otr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
otr.create_eftsmeta_info(TABLE_NAME, "AWS_OT_MACROS", "1.1 AWS_Extract_Line_OT", "OT_LINE", "new_submtg_state_cd_line")
otr.file_contents(TABLE_NAME)
