# Databricks notebook source
import os
import hashlib
import shutil
import zipfile
import json

from datetime import datetime
from pyspark.sql.functions import array, to_json, struct, collect_list, col, lit
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

# COMMAND ----------

bucket_name = 'dataconnect-taf-data-val/FinalFiles'

# COMMAND ----------

# MAGIC %run "./TAF Flat Files Columns"

# COMMAND ----------

file_types = dbutils.widgets.get('file_types')
reporting_period = dbutils.widgets.get('reporting_period')

# COMMAND ----------

file_type_to_table_names = {
  'BSE': 'TAF_ANN_DE_BASE',
  'ADR': 'TAF_ANN_DE_CNTCT_DTLS',
  'DSB': 'TAF_ANN_DE_DSBLTY',
  'DTS': 'TAF_ANN_DE_ELDTS',
  'HSP': 'TAF_ANN_DE_HHSPO',
  'MCR': 'TAF_ANN_DE_MC',
  'MFP': 'TAF_ANN_DE_MFP',
  'WVR': 'TAF_ANN_DE_WVR',
  'BSM': 'TAF_ANN_PL_BASE',
  'EPM': 'TAF_ANN_PL_ENRLMT',
  'LCM': 'TAF_ANN_PL_LCTN',
  'OAM': 'TAF_ANN_PL_OA',
  'SAM': 'TAF_ANN_PL_SAREA',
  'BSP': 'TAF_ANN_PR_BASE',
  'BED': 'TAF_ANN_PR_BED',
  'ENP': 'TAF_ANN_PR_ENRLMT',
  'GRP': 'TAF_ANN_PR_GRP',
  'IDP': 'TAF_ANN_PR_ID',
  'LIC': 'TAF_ANN_PR_LCNS',
  'LCP': 'TAF_ANN_PR_LCTN',
  'PGM': 'TAF_ANN_PR_PGM',
  'TAX': 'TAF_ANN_PR_TXNMY',
  'BSU': 'TAF_ANN_UP_BASE',
  'TOP': 'TAF_ANN_UP_TOP',
  'IPH': 'TAF_IPH',
  'IPL': 'TAF_IPL',
  'LTH': 'TAF_LTH',
  'LTL': 'TAF_LTL',
  'MCE': 'TAF_MCE',
  'MCL': 'TAF_MCL',
  'MCP': 'TAF_MCP',
  'MCS': 'TAF_MCS',
  'BSF': 'TAF_MON_BSF',
  'OTH': 'TAF_OTH',
  'OTL': 'TAF_OTL',
  'PBS': 'TAF_PRV',
  'PBT': 'TAF_PRV_BED',
  'PEN': 'TAF_PRV_ENR',
  'PAG': 'TAF_PRV_GRP',
  'PID': 'TAF_PRV_IDT',
  'PLI': 'TAF_PRV_LIC',
  'PLO': 'TAF_PRV_LOC',
  'PAP': 'TAF_PRV_PGM',
  'PTX': 'TAF_PRV_TAX',
  'RXH': 'TAF_RXH',
  'RXL': 'TAF_RXL',
}

if file_types == 'all':
  file_type_list = file_type_to_table_names.keys()
else:
  file_type_list = json.loads(file_types)
print(file_type_list)

# COMMAND ----------

query_reporting_period = datetime.strptime(reporting_period, '%Y-%m')
query_reporting_period = query_reporting_period.replace(day=1)
query_year_month = query_reporting_period.strftime('%Y-%m-%d')

query = (
  f"select distinct da_run_id from taf_python.CCW_TEST_RUN_IDS where year_month='{query_year_month}'"
)
df = spark.sql(query)
da_run_ids = [str(row.da_run_id) for row in df.collect()]

query_da_run_ids = "'" + ("', '").join(da_run_ids) + "'"
joined_file_type_list = "'" + ("', '").join(file_type_list) + "'"

query = (
  f'select distinct itrtn_num, fil_dt, da_run_id, fil_4th_node_txt from taf_python.efts_fil_meta '
  f'where da_run_id in ({query_da_run_ids}) and '
  f"fil_4th_node_txt in ({joined_file_type_list});"
)
distinct_meta_df = spark.sql(query)


# COMMAND ----------

dt = datetime.now()


fdate = dt.strftime("%y%m%d")
ftime = dt.strftime("%H%M%S%f")[0:8]


# COMMAND ----------

def create_flat_files(meta_info):
  fil_dt = meta_info.fil_dt
  file_type = meta_info.fil_4th_node_txt
  itrtn_num = meta_info.itrtn_num
  da_run_id = meta_info.da_run_id

  dt = datetime.now()
  seq = int(dt.strftime("%Y%m%d%H%M%S"))
  
  if len(fil_dt) == 6:
    reporting_date = datetime.strptime(fil_dt, '%Y%m')
    reporting_year = 'Y' + str(reporting_date.strftime("%Y"))[2:]
    reporting_month_str = 'M' + str(reporting_date.strftime("%m")).zfill(2)
  elif len(fil_dt) == 4:
    reporting_year = 'Y' + str(fil_dt)[2:]
    reporting_month_str = 'ANN'
  else:
    raise Exception(f'Unhandled fil_dt: {fil_dt}')

  table_name = file_type_to_table_names[file_type]
  metadata_file_type = file_type[0] + 'M' + file_type[2]

  # lower environments should have a T in the first bit of the constructed file name
  # production should have a P in the first bit of the constructed filename
  header_file_name = "T.TMF.{file_type}.{reporting_year}{reporting_month}.I{iteration}.D{fdate}.T{ftime}_header_000".format(
    file_type=file_type,
    reporting_year=reporting_year,
    reporting_month=reporting_month_str,
    iteration=itrtn_num,
    fdate=fdate,
    ftime=ftime
  )
  part_file_name = "T.TMF.{file_type}.{reporting_year}{reporting_month}.I{iteration}.D{fdate}.T{ftime}".format(
    file_type=file_type,
    reporting_year=reporting_year,
    reporting_month=reporting_month_str,
    iteration=itrtn_num,
    fdate=fdate,
    ftime=ftime
  )
  manifest_file_name = "T.TMF.{file_type}.{reporting_year}{reporting_month}.I{iteration}.D{fdate}.T{ftime}_manifest".format(
    file_type=file_type,
    reporting_year=reporting_year,
    reporting_month=reporting_month_str,
    iteration=itrtn_num,
    fdate=fdate,
    ftime=ftime
  )
  archive_file_name = "T.TMF.{file_type}.{reporting_year}{reporting_month}.I{iteration}.D{fdate}.T{ftime}".format(
    file_type=file_type,
    reporting_year=reporting_year,
    reporting_month=reporting_month_str,
    iteration=itrtn_num,
    fdate=fdate,
    ftime=ftime
  )
  metadata_file_name = "T.TMF.{metadata_file_type}.{reporting_year}{reporting_month}.I{iteration}.D{fdate}.T{ftime}".format(
    metadata_file_type=metadata_file_type,
    reporting_year=reporting_year,
    reporting_month=reporting_month_str,
    iteration=itrtn_num,
    fdate=fdate,
    ftime=ftime
  )

  write_header_and_part(file_type, table_name, da_run_id, seq)
  rename_part_files_and_get_manifest(file_type, part_file_name, seq)
  write_outbound_part_files(file_type, seq)
  write_outbound_header_files(file_type, header_file_name, seq)
  write_outbound_manifest_files(file_type, manifest_file_name, seq)

  path = f"dbfs:/tmp/taf-python/{file_type}/{seq}/outbound/"
  dbutils.fs.cp(path, "file:///var/tmp/taf-python/", recurse=True)

  make_zipfile('/var/tmp/taf-outbound.zip', '/var/tmp/taf-python')
  dbutils.fs.cp(f"file:///var/tmp/taf-outbound.zip", path)

  shutil.rmtree('/var/tmp/taf-python', ignore_errors=True)
  os.remove('/var/tmp/taf-outbound.zip')

  dbutils.fs.mv(f"{path}/taf-outbound.zip", f"{path}/{archive_file_name}")
  with open(f"/dbfs/tmp/taf-python/{file_type}/{seq}/outbound/{archive_file_name}", "rb") as f:
    file_hash = hashlib.md5()
    while chunk := f.read(8192):
      file_hash.update(chunk)

  write_metadata(file_type, metadata_file_name, file_hash, da_run_id, itrtn_num, seq)

  dbutils.fs.cp(f"dbfs:/tmp/taf-python/{file_type}/{seq}/outbound/{archive_file_name}", f"s3://{bucket_name}")
  dbutils.fs.cp(f"dbfs:/tmp/taf-python/{file_type}/{seq}/outbound/{metadata_file_name}", f"s3://{bucket_name}")



# COMMAND ----------

def write_header_and_part(file_type, table_name, da_run_id, seq):
  columns = flat_file_columns[table_name]

  df = spark.sql(
    """
    SELECT 
      {query_columns}
    FROM
      taf_python.{taf_table}
    WHERE
      da_run_id = {da_run_id}
    """.format(
      query_columns=', '.join(columns),
      taf_table=table_name,
      da_run_id=da_run_id
    )
  )

  header_schema = df.schema
  dfHeader = spark.createDataFrame([], schema=header_schema)

  # write header file
  dfHeader.coalesce(1).write.option(
    "sep", "\t"
  ).option(
    "encoding", "UTF-8"
  ).option(
    "quote", ""
  ).option(
    "emptyValue", None
  ).option(
    "nullValue", None
  ).csv(f"dbfs:/tmp/taf-python/{file_type}/{seq}/header", header=True)

  # write part files
  df.write.option(
    "sep", "\t"
  ).option(
    "encoding", "UTF-8"
  ).option(
    "quote", ""
  ).option(
    "emptyValue", None
  ).option(
    "nullValue", None
  ).option(
    "compression", "gzip"
  ).csv(f"dbfs:/tmp/taf-python/{file_type}/{seq}/part", header=False)

# COMMAND ----------

# rename part files
def rename_part_files_and_get_manifest(file_type, part_file_name, seq):
  manifest_data = []
  success = False
  path = f"/dbfs/tmp/taf-python/{file_type}/{seq}/part"

  for filename in os.listdir(path):
    file_path = os.path.join(path, filename)
    name, extension = os.path.splitext(filename)

    if (name.casefold().startswith("part")):
      # rename
      part_number = name.split("-")[1]
      new_name = f"{path}/{part_file_name}_{part_number}_part_00.gz"
      dest = shutil.move(file_path, new_name)

      # manifest data
      file_stats = os.stat(new_name)
      manifest_data.append((f"s3://{bucket_name}/{part_file_name}_{part_number}_part_00.gz", file_stats.st_size))
    elif (name.casefold() == "_success"):
      success = True

  if not success:
    raise Exception("Part files were not written successfully, check job logs")

  # generate file manifest
  dfManifest = spark.createDataFrame(manifest_data).toDF(*["url", "content_length"])

  dfManifest.select("url", struct("content_length").alias("meta")).select(
    struct("url", "meta").alias("entries_json")
  ).agg(
    collect_list("entries_json").alias("entries")
  ).select(
    col("entries")
  ).coalesce(1).write.format("json").option("escape", "").save(
    f"dbfs:/tmp/taf-python/{file_type}/{seq}/manifest"
  )

# COMMAND ----------

def write_outbound_part_files(file_type, seq):
  path = f"/dbfs/tmp/taf-python/{file_type}/{seq}/part"
  outpath = f"/dbfs/tmp/taf-python/{file_type}/{seq}/outbound"

  if not os.path.exists(outpath):
    os.makedirs(outpath)

  for filename in os.listdir(path):
    file_path = os.path.join(path, filename)
    name, extension = os.path.splitext(filename)

    if (name.casefold().startswith("t.tmf")):
      # rename
      new_name = outpath + "/" + name + extension
      dest = shutil.move(file_path, new_name)

# COMMAND ----------

def write_outbound_header_files(file_type, header_file_name, seq):
  success = False
  path = f"/dbfs/tmp/taf-python/{file_type}/{seq}/header"
  outpath = f"/dbfs/tmp/taf-python/{file_type}/{seq}/outbound"

  if not os.path.exists(outpath):
    os.makedirs(outpath)

  for filename in os.listdir(path):
    file_path = os.path.join(path, filename)
    name, extension = os.path.splitext(filename)

    if (name.casefold().startswith("part")):
      # rename
      new_name = outpath + "/" + header_file_name
      dest = shutil.move(file_path, new_name)

    elif (name.casefold() == "_success"):
      success = True

  if not success:
    raise Exception("Part files were not written successfully, check job logs")

# COMMAND ----------

def write_outbound_manifest_files(file_type, manifest_file_name, seq):
  success = False
  path = f"/dbfs/tmp/taf-python/{file_type}/{seq}/manifest"
  outpath = f"/dbfs/tmp/taf-python/{file_type}/{seq}/outbound"

  if not os.path.exists(outpath):
    os.makedirs(outpath)

  for filename in os.listdir(path):
    file_path = os.path.join(path, filename)
    name, extension = os.path.splitext(filename)

    if (name.casefold().startswith("part")):
      # rename
      new_name = outpath + "/" + manifest_file_name
      dest = shutil.move(file_path, new_name)

    elif (name.casefold() == "_success"):
      success = True

  if not success:
    raise Exception("Manifest file was not written successfully, check job logs")

# COMMAND ----------

def make_zipfile(output_filename, source_dir):
  relroot = os.path.abspath(os.path.join(source_dir, os.pardir))
  with zipfile.ZipFile(output_filename, "w", zipfile.ZIP_DEFLATED) as zip:
    for root, dirs, files in os.walk(source_dir):
      # do not add tmp directory name. unzip to current dir
      # zip.write(root, os.path.relpath(root, relroot))
      for file in files:
        filename = os.path.join(root, file)
        filename_extension = filename.split('.')[-1]
        if os.path.isfile(filename) and filename_extension.casefold() != 'crc':
          # regular files only and does not end in .crc
          
          # do not add tmp directory name
          # arcname = os.path.join(os.path.relpath(root, relroot), file)
          zip.write(filename, file)

# COMMAND ----------

def write_metadata(file_type, metadata_file_name, file_hash, da_run_id, itrtn_num, seq):
  total_record_count = spark.sql(f"""
      SELECT sum(rec_cnt_by_state_cd)
      FROM taf_python.efts_fil_meta
      WHERE fil_4th_node_txt = '{file_type}'
        AND itrtn_num = '{itrtn_num}'
        AND da_run_id = {da_run_id}
  """).first()[0]

  z = f"""
      SELECT fil_4th_node_txt AS TAF_type,
        date_format(current_date(),'MM/dd/yyyy') as Creation_Date,
        rptg_prd AS Reporting_Month,
        concat('I', itrtn_num) AS iteration,
        incldd_state_cd AS State_Code,
        rec_cnt_by_state_cd as Record_Count,
        '{metadata_file_name}' as File_Name,
        '{file_hash.hexdigest()}' as Checksum
      FROM taf_python.efts_fil_meta
      WHERE fil_4th_node_txt = '{file_type}'
        AND itrtn_num = '{itrtn_num}'
        AND da_run_id = {da_run_id}
  """

  df = spark.sql(z)

  # write metadata file
  df.select(
    "TAF_type", "Creation_Date", "Reporting_Month", "iteration", "File_Name", "Checksum", struct("State_Code", "Record_Count").alias("json")
  ).groupBy(
    "TAF_type", "Creation_Date", "Reporting_Month", "iteration", "File_Name", "Checksum"
  ).agg(
    collect_list("json").alias("Included_States")
  ).select(
    col("TAF_type"), col("Creation_Date"), col("Reporting_Month"), col("iteration"), col("Included_States"), col("File_Name"), col("Checksum")
  ).withColumn(
    "Total_Record_Count", lit(total_record_count)
  ).select(
    col("TAF_type"), col("Creation_Date"), col("Reporting_Month"), col("iteration"), col("Total_Record_Count"), col("Included_States"), col("File_Name"), col("Checksum")
  ).coalesce(1).write.format("json").option("escape", "").save(
    f"dbfs:/tmp/taf-python/{file_type}/{seq}/metadata"
  )

  # write outbound file
  path = f"/dbfs/tmp/taf-python/{file_type}/{seq}/metadata"
  outpath = f"/dbfs/tmp/taf-python/{file_type}/{seq}/outbound"

  if not os.path.exists(outpath):
    os.makedirs(outpath)

  for filename in os.listdir(path):
    file_path = os.path.join(path, filename)
    name, extension = os.path.splitext(filename)

    if (name.casefold().startswith("part")):
      # rename
      new_name = outpath + "/" + metadata_file_name
      dest = shutil.move(file_path, new_name)


# COMMAND ----------

#serial:
#works slowly
for distinct_row in distinct_meta_df.rdd.collect():
  create_flat_files(distinct_row)

#attempt at spark threading
#doesn't like use of dbutils inside of worker
#distinct_meta_df.rdd.foreach(create_flat_files)

#attempt at multiprocessing threadpool
#died because of collision to /var/tmp/taf-outbound.zip
#from multiprocessing.pool import ThreadPool
#pool = ThreadPool(10)
#pool.map(create_flat_files, distinct_meta_df.rdd.collect())

