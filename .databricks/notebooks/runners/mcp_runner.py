# Databricks notebook source
from taf.MCP.MCP_Runner import MCP_Runner

# COMMAND ----------

from taf.MCP.MCP_Metadata import MCP_Metadata

# COMMAND ----------

mcp = MCP_Runner(da_schema        = dbutils.widgets.get("da_schema")
                ,reporting_period = dbutils.widgets.get("reporting_period")
                ,state_code       = dbutils.widgets.get("state_code")
                ,run_id           = dbutils.widgets.get("run_id")
                ,job_id           = dbutils.widgets.get("job_id"))

# COMMAND ----------

mcp.job_control_wrt("MCP")

# COMMAND ----------

mcp.job_control_updt()

# COMMAND ----------

mcp.print()

# COMMAND ----------

mcp.init()

# COMMAND ----------

mcp.run()   

# COMMAND ----------

mcp.view_plan()

# COMMAND ----------

display(mcp.audit())

# COMMAND ----------

mcp.job_control_updt2()

# COMMAND ----------

TABLE_NAME = "TAF_MCP"
FIL_4TH_NODE = "MCP"
 
mcp.get_cnt(TABLE_NAME)
mcp.getcounts("101_mc_build.sas", "base_MCP")
mcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
mcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "base_MCP", "MC02_Base", "submtg_state_cd")
mcp.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_MCL"
FIL_4TH_NODE = "MCL"

mcp.get_cnt(TABLE_NAME)
mcp.getcounts("101_mc_build.sas", "constructed_MC03")
mcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
mcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "constructed_MC03", "MC03_Location_CNST", "submtg_state_cd")
mcp.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_MCS"
FIL_4TH_NODE = "MCS"

mcp.get_cnt(TABLE_NAME)
mcp.getcounts("101_mc_build.sas", "constructed_MC04")
mcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
mcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "constructed_MC04", "MC04_Service_Area_CNST", "submtg_state_cd")
mcp.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_MCE"
FIL_4TH_NODE = "MCE"

mcp.get_cnt(TABLE_NAME)
mcp.getcounts("101_mc_build.sas", "segment_MC06")
mcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
mcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "segment_MC06", "MC06_Population", "submtg_state_cd")
mcp.file_contents(TABLE_NAME)
