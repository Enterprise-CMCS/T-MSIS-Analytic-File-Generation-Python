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

# MAGIC %md
# MAGIC ### Parameter names for prepping the meta table. These  replace the get_cnt and get_counts as they are internal to the this function now ### 
# MAGIC ```taf_runner.prep_meta_table(self, pgm_name: str, step_name: str, filetyp: str = None, fl: str = None, fl2: str = None, rpt_out: str = None, clm_tbl: str = None, bsf_file_date: str = None): ```

# COMMAND ----------

TABLE_NAME = "TAF_LTH"
FIL_4TH_NODE = "LTH"
FILETYP = "LT"

 
ltr.prep_meta_table("AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT", FILETYP, FILETYP, FILETYP, ltr.RPT_OUT, TABLE_NAME, ltr.BSF_FILE_DATE)  
ltr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
ltr.create_eftsmeta_info(TABLE_NAME, "AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT", "LT_HEADER", "new_submtg_state_cd")
ltr.file_contents(TABLE_NAME)

# COMMAND ----------

TABLE_NAME = "TAF_LTL"
FIL_4TH_NODE = "LTL"
FILETYP = "LT"
 
ltr.prep_meta_table("AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT", FILETYP, FILETYP, FILETYP, ltr.RPT_OUT, TABLE_NAME, ltr.BSF_FILE_DATE)  
ltr.create_meta_info(TABLE_NAME, FIL_4TH_NODE)
ltr.create_eftsmeta_info(TABLE_NAME, "AWS_LT_MACROS", "1.1 AWS_Extract_Line_LT", "LT_LINE", "new_submtg_state_cd_line")
ltr.file_contents(TABLE_NAME)
