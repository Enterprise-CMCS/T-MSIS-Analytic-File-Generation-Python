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
               ,file_version     = dbutils.widgets.get("file_version")
               ,run_stats_only   = dbutils.widgets.get("run_stats_only"))

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

# MAGIC %md
# MAGIC ### Parameter names for prepping the meta table. These  replace the get_cnt and get_counts as they are internal to the this function now ### 
# MAGIC ```taf_runner.prep_meta_table(self, pgm_name: str, step_name: str, filetyp: str = None, fl: str = None, fl2: str = None, rpt_out: str = None, clm_tbl: str = None, bsf_file_date: str = None): ```

# COMMAND ----------

#TABLE_NAME = "TAF_IPH"
#FIL_4TH_NODE = "IPH"
#FILETYP = "IP"

# prep table returns temporary view name to read to eliminate collisions of runs
#meta_view_nm = ipr.prep_meta_table("AWS_IP_Macros", "1.1 AWS_Extract_Line_IP", FILETYP, FILETYP, FILETYP, ipr.RPT_OUT, TABLE_NAME, ipr.BSF_FILE_DATE)
#ipr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#ipr.create_eftsmeta_info(TABLE_NAME, "AWS_IP_Macros", "1.1 AWS_Extract_Line_IP", "IP_HEADER", "new_submtg_state_cd", meta_view_name=meta_view_nm)
#ipr.file_contents(TABLE_NAME)

# COMMAND ----------

#TABLE_NAME = "TAF_IPL"
#FIL_4TH_NODE = "IPL"
#FILETYP = "IP"

# prep table returns temporary view name to read to eliminate collisions of runs
#meta_view_nm = ipr.prep_meta_table("AWS_IP_Macros", "1.1 AWS_Extract_Line_IP", FILETYP, FILETYP, FILETYP, ipr.RPT_OUT, TABLE_NAME, ipr.BSF_FILE_DATE)
#ipr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
#ipr.create_eftsmeta_info(TABLE_NAME, "AWS_IP_Macros", "1.1 AWS_Extract_Line_IP", "IP_LINE", "new_submtg_state_cd_line", meta_view_name=meta_view_nm)
#ipr.file_contents(TABLE_NAME)
