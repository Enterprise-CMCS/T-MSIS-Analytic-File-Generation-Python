# Databricks notebook source
from taf.LT.LT_Runner import LT_Runner

# COMMAND ----------

from taf.LT.LT_Metadata import LT_Metadata

# COMMAND ----------

ltr = LT_Runner(da_schema        = dbutils.widgets.get("da_schema")
               ,reporting_period = dbutils.widgets.get("reporting_period")
               ,state_code       = dbutils.widgets.get("state_code")
               ,run_id           = dbutils.widgets.get("run_id")
               ,job_id           = dbutils.widgets.get("job_id"))


# COMMAND ----------

ltr.job_control_wrt("TLT")

# COMMAND ----------

ltr.job_control_updt()

# COMMAND ----------

ltr.print()

# COMMAND ----------

ltr.init()

# COMMAND ----------

ltr.run()

# COMMAND ----------

ltr.view_plan()

# COMMAND ----------

display(ltr.audit())

# COMMAND ----------

ltr.job_control_updt2()

# COMMAND ----------

TABLE_NAME = "TAF_LTH"
FIL_4TH_NODE = "LTH"
 
ltr.get_cnt(TABLE_NAME)
ltr.getcounts("AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT")
ltr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
ltr.create_eftsmeta_info(TABLE_NAME, "AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT", "LT_HEADER", "new_submtg_state_cd")
ltr.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_LTL"
FIL_4TH_NODE = "LTL"
 
ltr.get_cnt(TABLE_NAME)
ltr.getcounts("AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT")
ltr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
ltr.create_eftsmeta_info(TABLE_NAME, "AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT", "LT_LINE", "new_submtg_state_cd_line")
ltr.file_contents(TABLE_NAME)
