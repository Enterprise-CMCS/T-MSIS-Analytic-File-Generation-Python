# Databricks notebook source
from taf.UP.UP_Runner import UP_Runner

# COMMAND ----------

up = UP_Runner(da_schema        = dbutils.widgets.get("da_schema")
              ,reporting_period = dbutils.widgets.get("reporting_period")
              ,state_code       = dbutils.widgets.get("state_code")
              ,run_id           = dbutils.widgets.get("run_id")
              ,job_id           = dbutils.widgets.get("job_id"))

# COMMAND ----------

up.job_control_wrt("AUP")

# COMMAND ----------

up.job_control_updt()

# COMMAND ----------

up.print()

# COMMAND ----------

up.init()

# COMMAND ----------

up.run()

# COMMAND ----------

up.view_plan()

# COMMAND ----------

display(up.audit())

# COMMAND ----------

#TABLE_NAME = "BASE"
#FIL_4TH_NODE = "BSU"

#up.create_meta_info(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

#TABLE_NAME = "TOP"
#FIL_4TH_NODE = "TOP"

#up.create_meta_info(TABLE_NAME, FIL_4TH_NODE)

# COMMAND ----------

up.job_control_updt2()
