# Databricks notebook source
from taf.RX.RX_Runner import RX_Runner

# COMMAND ----------

from taf.RX.RX_Metadata import RX_Metadata

# COMMAND ----------

rxr = RX_Runner(da_schema        = dbutils.widgets.get("da_schema")
               ,reporting_period = dbutils.widgets.get("reporting_period")
               ,state_code       = dbutils.widgets.get("state_code")
               ,run_id           = dbutils.widgets.get("run_id")
               ,job_id           = dbutils.widgets.get("job_id"))

# COMMAND ----------

rxr.job_control_wrt("TRX")

# COMMAND ----------

rxr.job_control_updt()

# COMMAND ----------

rxr.print()

# COMMAND ----------

rxr.init()

# COMMAND ----------

rxr.run()

# COMMAND ----------

rxr.view_plan()

# COMMAND ----------

display(rxr.audit())

# COMMAND ----------

rxr.job_control_updt2()

# COMMAND ----------

TABLE_NAME = "TAF_RXH"
FIL_4TH_NODE = "RXH"
 
rxr.get_cnt(TABLE_NAME)
rxr.getcounts("AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX")
rxr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
rxr.create_eftsmeta_info(TABLE_NAME, "AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX", "RX_HEADER", "new_submtg_state_cd")
rxr.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_RXL"
FIL_4TH_NODE = "RXL"
 
rxr.get_cnt(TABLE_NAME)
rxr.getcounts("AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX")
rxr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
rxr.create_eftsmeta_info(TABLE_NAME, "AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX", "RX_LINE", "new_submtg_state_cd_line")
rxr.file_contents(TABLE_NAME)
