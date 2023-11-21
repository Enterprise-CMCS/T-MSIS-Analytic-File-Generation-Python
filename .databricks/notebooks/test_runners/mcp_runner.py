# Databricks notebook source
from taf.MCP.MCP_Runner import MCP_Runner

# COMMAND ----------

from taf.MCP.MCP_Metadata import MCP_Metadata

# COMMAND ----------

mcp = MCP_Runner(da_schema        = dbutils.widgets.get("da_schema")
                ,reporting_period = dbutils.widgets.get("reporting_period")
                ,state_code       = dbutils.widgets.get("state_code")
                ,run_id           = dbutils.widgets.get("run_id")
                ,job_id           = dbutils.widgets.get("job_id")
                ,file_version     = dbutils.widgets.get("file_version")
                ,run_stats_only   = dbutils.widgets.get("run_stats_only"))

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
FILETYP = "MCP"

# prep table returns temporary view name to read to eliminate collisions of runs
meta_view_nm = mcp.prep_meta_table("101_mc_build.sas", "base_MCP", FILETYP, FILETYP, FILETYP, mcp.RPT_OUT, TABLE_NAME, mcp.BSF_FILE_DATE)
mcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
mcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "base_MCP", "MC02_Base", "submtg_state_cd", meta_view_name=meta_view_nm)
mcp.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_MCL"
FIL_4TH_NODE = "MCL"
FILETYP = "MCP"

# prep table returns temporary view name to read to eliminate collisions of runs
meta_view_nm = mcp.prep_meta_table("101_mc_build.sas", "constructed_MC03", FILETYP, FILETYP, FILETYP, mcp.RPT_OUT, TABLE_NAME, mcp.BSF_FILE_DATE)
mcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
mcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "constructed_MC03", "MC03_Location_CNST", "submtg_state_cd", meta_view_name=meta_view_nm)
mcp.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_MCS"
FIL_4TH_NODE = "MCS"
FILETYP = "MCP"

meta_view_nm = mcp.prep_meta_table("101_mc_build.sas", "constructed_MC04", FILETYP, FILETYP, FILETYP, mcp.RPT_OUT, TABLE_NAME, mcp.BSF_FILE_DATE)
mcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
mcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "constructed_MC04", "MC04_Service_Area_CNST", "submtg_state_cd", meta_view_name=meta_view_nm)
mcp.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_MCE"
FIL_4TH_NODE = "MCE"
FILETYP = "MCP"

meta_view_nm = mcp.prep_meta_table("101_mc_build.sas", "segment_MC06", FILETYP, FILETYP, FILETYP, mcp.RPT_OUT, TABLE_NAME, mcp.BSF_FILE_DATE)
mcp.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
mcp.create_eftsmeta_info(TABLE_NAME, "101_mc_build.sas", "segment_MC06", "MC06_Population", "submtg_state_cd", meta_view_name=meta_view_nm)
mcp.file_contents(TABLE_NAME)
