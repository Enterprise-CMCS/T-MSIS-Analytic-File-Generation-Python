# Databricks notebook source
from taf.RX.RX_Runner import RX_Runner

# COMMAND ----------

from taf.RX.RX_Metadata import RX_Metadata

# COMMAND ----------

rxr = RX_Runner(da_schema        = dbutils.widgets.get("da_schema")
               ,reporting_period = dbutils.widgets.get("reporting_period")
               ,state_code       = dbutils.widgets.get("state_code")
               ,run_id           = dbutils.widgets.get("run_id")
               ,job_id           = dbutils.widgets.get("job_id")
               ,file_version     = dbutils.widgets.get("file_version")
               ,run_stats_only   = dbutils.widgets.get("run_stats_only"))

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

# MAGIC %md
# MAGIC ### Parameter names for prepping the meta table. These  replace the get_cnt and get_counts as they are internal to the this function now ### 
# MAGIC ```taf_runner.prep_meta_table(self, pgm_name: str, step_name: str, filetyp: str = None, fl: str = None, fl2: str = None, rpt_out: str = None, clm_tbl: str = None, bsf_file_date: str = None): ```

# COMMAND ----------

TABLE_NAME = "TAF_RXH"
FIL_4TH_NODE = "RXH"
FILETYP = "RX"

# prep table returns temporary view name to read to eliminate collisions of runs
meta_view_nm = rxr.prep_meta_table("AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX", FILETYP, FILETYP, FILETYP, rxr.RPT_OUT, TABLE_NAME, rxr.BSF_FILE_DATE)
rxr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
rxr.create_eftsmeta_info(TABLE_NAME, "AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX", "RX_HEADER", "new_submtg_state_cd", meta_view_name=meta_view_nm)
rxr.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_RXL"
FIL_4TH_NODE = "RXL"
FILETYP = "RX"

# prep table returns temporary view name to read to eliminate collisions of runs
meta_view_nm = rxr.prep_meta_table("AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX", FILETYP, FILETYP, FILETYP, rxr.RPT_OUT, TABLE_NAME, rxr.BSF_FILE_DATE)
rxr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
rxr.create_eftsmeta_info(TABLE_NAME, "AWS_RX_MACROS", "1.1 AWS_Extract_Line_RX", "RX_LINE", "new_submtg_state_cd_line", meta_view_name=meta_view_nm)
rxr.file_contents(TABLE_NAME)
