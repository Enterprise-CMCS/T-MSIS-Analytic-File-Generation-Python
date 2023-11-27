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
               ,file_version     = dbutils.widgets.get("file_version")
               ,run_stats_only   = dbutils.widgets.get("run_stats_only"))

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

# MAGIC %md
# MAGIC ### Parameter names for prepping the meta table. These  replace the get_cnt and get_counts as they are internal to the this function now ### 
# MAGIC ```taf_runner.prep_meta_table(self, pgm_name: str, step_name: str, filetyp: str = None, fl: str = None, fl2: str = None, rpt_out: str = None, clm_tbl: str = None, bsf_file_date: str = None): ```

# COMMAND ----------

TABLE_NAME = "TAF_OTH"
FIL_4TH_NODE = "OTH"
FILETYP = "OTHR_TOC"

meta_view_nm = otr.prep_meta_table("AWS_OT_MACROS", "1.1 AWS_Extract_Line_OT", FILETYP, FILETYP, FILETYP, otr.RPT_OUT, TABLE_NAME, otr.BSF_FILE_DATE)
otr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
otr.create_eftsmeta_info(TABLE_NAME, "AWS_OT_MACROS", "1.1 AWS_Extract_Line_OT", "OT_HEADER", "new_submtg_state_cd", #meta_view_name=meta_view_nm)
otr.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_OTL"
FIL_4TH_NODE = "OTL"
FILETYP = "OTHR_TOC"

meta_view_nm = otr.prep_meta_table("AWS_OT_MACROS", "1.1 AWS_Extract_Line_OT", FILETYP, FILETYP, FILETYP, otr.RPT_OUT, TABLE_NAME, otr.BSF_FILE_DATE)
otr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
otr.create_eftsmeta_info(TABLE_NAME, "AWS_OT_MACROS", "1.1 AWS_Extract_Line_OT", "OT_LINE", "new_submtg_state_cd_line", meta_view_name=meta_view_nm)
otr.file_contents(TABLE_NAME)
