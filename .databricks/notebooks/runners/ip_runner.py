# Databricks notebook source
from taf.IP.IP_Runner import IP_Runner

# COMMAND ----------

from taf.IP.IP_Metadata import IP_Metadata

# COMMAND ----------

ipr = IP_Runner(da_schema        = dbutils.widgets.get("da_schema")
               ,reporting_period = dbutils.widgets.get("reporting_period")
               ,state_code       = dbutils.widgets.get("state_code")
               ,run_id           = dbutils.widgets.get("run_id")
               ,job_id           = dbutils.widgets.get("job_id")
               ,file_version     = dbutils.widgets.get("file_version"))

# COMMAND ----------

ipr.job_control_wrt("TIP")

# COMMAND ----------

ipr.job_control_updt()

# COMMAND ----------

ipr.print()

# COMMAND ----------

ipr.init()

# COMMAND ----------

ipr.run()

# COMMAND ----------

ipr.view_plan()

# COMMAND ----------

display(ipr.audit())

# COMMAND ----------

ipr.job_control_updt2()

# COMMAND ----------

TABLE_NAME = "TAF_IPH"
FIL_4TH_NODE = "IPH"
 
ipr.get_cnt(TABLE_NAME)
ipr.getcounts("AWS_IP_MACROS", "1.1 AWS_Extract_Line_IP")
ipr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
ipr.create_eftsmeta_info(TABLE_NAME, "AWS_IP_MACROS", "1.1 AWS_Extract_Line_IP", "IP_HEADER", "new_submtg_state_cd")
ipr.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_IPL"
FIL_4TH_NODE = "IPL"
 
ipr.get_cnt(TABLE_NAME)
ipr.getcounts("AWS_IP_MACROS", "1.1 AWS_Extract_Line_IP")
ipr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
ipr.create_eftsmeta_info(TABLE_NAME, "AWS_IP_MACROS", "1.1 AWS_Extract_Line_IP", "IP_LINE", "new_submtg_state_cd_line")
ipr.file_contents(TABLE_NAME)
