import logging
import sys

from pyspark.sql import SparkSession
from datetime import datetime
from taf.TAF_Metadata import TAF_Metadata


class TAF_Runner():
    """
    A class to represent the creation of a T-MSIS analytic file.
    """

    PERFORMANCE = 11

    def __init__(self,
                 da_schema: str,
                 reporting_period: str,
                 state_code: str,
                 run_id: str,
                 job_id: int,
                 file_version: str):
        """
        Constructs all the necessary attributes for the T-MSIS analytic file runner object.

            Parameters:
                da_schema (str): Schema to be written to
                reporting_period (str): Day of month to filter T-MSIS data on (in YYYY-MM-DD format)
                state_code (str): Comma-separated list of T-MSIS state code(s) values to include
                run_id (str): Comma-separated list of T-MSIS run identifier(s) values to include
                job_id (int): Final data will use this for da_run_id

            Returns:
                None
        """

        from datetime import date, datetime, timedelta

        self.now = datetime.now()
        self.initialize_logger(self.now)

        if len(file_version) == 3:
            self.version = file_version
        else:
            self.logger.error("ERROR: File Version must be 3 characters.")
            sys.exit(1)

        # state submission type
        TAF_Metadata.getFormatsForValidationAndRecode()

        # This gets passed in from the runner and is the job_id from DataBricks
        self.state_code = state_code
        self.DA_RUN_ID = job_id
        self.DA_SCHEMA = da_schema  # For using data from Redshift for testing DE and up
        self.DA_SCHEMA_DC = da_schema

        self.reporting_period = datetime.strptime(reporting_period, '%Y-%m-%d')

        begmon = self.reporting_period
        begmon = date(begmon.year, begmon.month, 1)
        self.begmon = begmon.strftime('%Y-%m-%d').upper()
        self.st_dt = f'{self.begmon}'

        self.BSF_FILE_DATE = int(self.reporting_period.strftime('%Y%m'))
        self.TAF_FILE_DATE = self.BSF_FILE_DATE

        self.RPT_PRD = f'{self.reporting_period.strftime("%Y-%m-%d")}'
        self.RPT_OUT = f"{self.reporting_period.strftime('%d%b%Y').upper()}"

        if self.now.month == 12:  # December
            last_day = date(self.now.year, self.now.month, 31)
        else:
            last_day = date(self.now.year, self.now.month + 1, 1) - timedelta(days=1)

        self.FILE_DT_END = last_day.strftime('%Y-%m-%d').upper()

        if len(state_code) and len(run_id):
            if (state_code.find(',') != -1) and (run_id.find(',') != -1):
                self.combined_list = list(tuple(zip(eval(state_code), eval(run_id))))
            else:
                self.combined_list = [(eval(state_code), eval(run_id)),]
        else:
            self.combined_list = []

        # determine if national or state specific run
        if len(state_code) > 4:
            self.national_run = 1
        else:
            self.national_run = 0

        self.sql = {}
        self.preplan = []
        self.plan = {}

    def print(self):
        """
        Prints parameter values derived by the constructor

            Parameters:
                None

            Returns:
                None
        """
        print('Version:\t' + self.version)
        print('-----------------------------------------------------')
        print('')
        print('-----------------------------------------------------')
        print('begmon:\t' + str(self.begmon))
        print('st_dt:\t' + self.st_dt)
        print('BSF_FILE_DATE:\t' + str(self.BSF_FILE_DATE))
        print('TAF_FILE_DATE:\t' + str(self.TAF_FILE_DATE))
        print('RPT_PRD:\t' + str(self.RPT_PRD))
        print('FILE_DT_END:\t' + str(self.FILE_DT_END))
        print('DA_SCHEMA:\t' + str(self.DA_SCHEMA))
        print('DA_SCHEMA_DC:\t' + str(self.DA_SCHEMA_DC))
        print('COMBINED_LIST:\t' + str(self.combined_list))

    def get_link_key(self):
        """
        Creates a unique key identifying claim header records.

            Parameters:
                None

            Returns:
                z (str): SQL statement string to create a link key
        """

        return f"""
            cast ((concat('{self.version }',  '-',  {self.TAF_FILE_DATE},  '-',  NEW_SUBMTG_STATE_CD,  '-',
              trim(COALESCE(NULLIF(ORGNL_CLM_NUM,'~'),'0')), '-',  trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM,'~'),'0')),  '-',
                CAST(year(ADJDCTN_DT) AS CHAR(4)), CAST(DATE_PART('MONTH',ADJDCTN_DT) AS CHAR(2)),
                 CAST(DATE_PART('DAY',ADJDCTN_DT) AS CHAR(2)), '-',  COALESCE(ADJSTMT_IND_CLEAN,'X'))) as varchar(126))
        """

    def get_link_key_line(self):
        """
        Creates a unique key identifying claim line item records.

            Parameters:
                None

            Returns:
                z (str): SQL statement string to create a line link key
        """

        return f"""
            cast ((concat('{self.version }',  '-',  {self.TAF_FILE_DATE},  '-',  NEW_SUBMTG_STATE_CD_LINE,  '-',
              trim(COALESCE(NULLIF(ORGNL_CLM_NUM_LINE,'~'),'0')), '-',  trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM_LINE,'~'),'0')),  '-',
                CAST(year(ADJDCTN_DT_LINE) AS CHAR(4)), CAST(DATE_PART('MONTH',ADJDCTN_DT_LINE) AS CHAR(2)),
                 CAST(DATE_PART('DAY',ADJDCTN_DT_LINE) AS CHAR(2)), '-',  COALESCE(LINE_ADJSTMT_IND_CLEAN,'X'))) as varchar(126))
        """

    @staticmethod
    def compress(string):
        """
        Splits a string into a list and joins all items into a string using a single
        space as separator.

            Parameters:
                string (str): string value to compress

            Returns:
                z (str): compressed string value
        """

        return ' '.join(string.split())

    def log(self, viewname: str, sql=''):
        """
        Prints view name and formatted SQL to log output

            Parameters:
                viewname (str): A view's name
                sql (str): The view's definition (in SQL)

            Returns:
                None
        """

        self.logger.info('\t' + viewname)
        if sql != '':
            self.logger.debug(TAF_Runner.compress(sql.replace('\n', '')))

    def initialize_logger(self, now: datetime):
        """
        Initializes the logger for a T-MSIS analytic file run.

            Parameters:
                now (datetime): The current date and time

            Returns:
                None
        """

        logging.addLevelName(TAF_Runner.PERFORMANCE, 'PERFORMANCE')

        self.logger = logging.getLogger('taf_log')
        self.logger.setLevel(logging.INFO)

        ch = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        if (self.logger.hasHandlers()):
            self.logger.handlers.clear()

        self.logger.addHandler(ch)

        # writing to stdout
        stdout = logging.StreamHandler(sys.stdout)
        stdout.setLevel(logging.ERROR)
        stdout.setFormatter(formatter)
        self.logger.addHandler(stdout)

    def fetch_combined_list(self):
        """
        Query T-MSIS file header eligibility data to determine the latest T-MSIS
        run identifier value for each submitting state.

            Parameters:
                None

            Returns:
                None
        """

        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()

        sdf = spark.sql("""
            select distinct
                submtg_state_cd,
                max(tmsis_run_id) as tmsis_run_id
            from
                tmsis.tmsis_fhdr_rec_elgblty
            where tmsis_actv_ind = 1 and
                tmsis_rptg_prd is not null and
                tot_rec_cnt > 0 and
                ssn_ind in ('1','0')
            group by
                submtg_state_cd
            order by
                submtg_state_cd""")

        rdd = sdf.rdd
        self.combined_list = rdd.map(tuple)

    def get_combined_list(self):
        """
        Join all items in COMBINED_LIST into a comma-separated string

            Parameters:
                None

            Returns:
                None
        """

        tuples = []
        for j in self.combined_list:
            tuples.append('concat' + str(j))
        return ','.join(tuples)

    def ssn_ind(self):
        """
        Create a temporary view to determine if each submitting state uses social
        security numbers to identify members.

            Parameters:
                None

            Returns:
                z (str): SQL statement string to create a temporary view
        """

        return """
                create or replace temporary view ssn_ind as
                select distinct submtg_state_cd, ssn_ind
                from tmsis.tmsis_fhdr_rec_elgblty
                where tmsis_actv_ind = 1
                    and tmsis_rptg_prd is not null
                    and tot_rec_cnt > 0
                    and ssn_ind IN ('1','0')
                    and concat(submtg_state_cd,tmsis_run_id) in ({self.get_combined_list()})
        """

    def job_control_rd(self, da_run_id: int, file_type: str):
        """
        Creates a job control table.
        """

        return f"""
                CREATE TABLE JOB_CD_LOOKUP AS
                SELECT
                    {da_run_id}
                   ,schld_ordr_num
                   ,job_parms_txt
                   ,taf_cd_spec_vrsn_name
                FROM {self.DA_SCHEMA}.job_cntl_parms
                WHERE schld_ordr_num = (
                    SELECT MIN(schld_ordr_num)
                    FROM {self.DA_SCHEMA}.job_cntl_parms
                    WHERE sucsfl_ind IS NULL
                    AND fil_type = {file_type}
                )
        """

    def job_control_wrt(self, file_type: str):
        """
        Insert the job control parameters.
        """

        spark = SparkSession.getActiveSession()

        if (self.national_run):
            parms = f"{self.st_dt}"
        else:
            parms = f"{self.st_dt}" + ", " + "submtg_state_cd" + " " + "in" + " " + "(" + f"{self.state_code}" + ")"

        print("DEBUG: " + f"""
            INSERT INTO {self.DA_SCHEMA}.job_cntl_parms (
                da_run_id
               ,fil_type
               ,schld_ordr_num
               ,job_parms_txt
               ,cd_spec_vrsn_name
               ,job_strt_ts
               ,job_end_ts
               ,sucsfl_ind
               ,rec_add_ts
               ,rec_updt_ts
               ,rfrsh_vw_flag
               ,taf_cd_spec_vrsn_name
            )
            VALUES (
                {self.DA_RUN_ID}
               ,"{file_type}"
               ,1
               ,"{parms}"
               ,concat("{self.version}", ",", "7.1")
               ,NULL
               ,NULL
               ,False
               ,from_utc_timestamp(current_timestamp(), "EST")
               ,NULL
               ,False
               ,concat("{self.version}", ",", "7.1")
            )
        """)

        spark.sql(
            f"""
            INSERT INTO {self.DA_SCHEMA}.job_cntl_parms (
                da_run_id
               ,fil_type
               ,schld_ordr_num
               ,job_parms_txt
               ,cd_spec_vrsn_name
               ,job_strt_ts
               ,job_end_ts
               ,sucsfl_ind
               ,rec_add_ts
               ,rec_updt_ts
               ,rfrsh_vw_flag
               ,taf_cd_spec_vrsn_name
            )
            VALUES (
                {self.DA_RUN_ID}
               ,"{file_type}"
               ,1
               ,"{parms}"
               ,concat("{self.version}", ",", "7.1")
               ,NULL
               ,NULL
               ,False
               ,from_utc_timestamp(current_timestamp(), "EST")
               ,NULL
               ,False
               ,concat("{self.version}", ",", "7.1")
            )
        """
        )

    def job_control_updt(self):
        """
        Update the job control parameters.
        """

        spark = SparkSession.getActiveSession()

        spark.sql(
            f"""
                UPDATE {self.DA_SCHEMA_DC}.job_cntl_parms
                SET job_strt_ts = from_utc_timestamp(current_timestamp(), 'EST')
                WHERE da_run_id = {self.DA_RUN_ID}
        """
        )

    def job_control_updt2(self):
        """
        Helper function to update the job control parameters with the da_run_id.
        """

        spark = SparkSession.getActiveSession()

        spark.sql(
            f"""
                UPDATE {self.DA_SCHEMA_DC}.job_cntl_parms
                SET job_end_ts = from_utc_timestamp(current_timestamp(), 'EST'),
                sucsfl_ind = 1
                WHERE da_run_id = {self.DA_RUN_ID}
        """
        )

    def get_cnt(self, table_name: str):
        """
        Create a temporary view of the count of tmsis_run_id filtered by da_run_id.
        """
        spark = SparkSession.getActiveSession()

        spark.sql(
            f"""
                CREATE OR REPLACE TEMPORARY VIEW record_count AS
                SELECT count(tmsis_run_id) AS row_cnt
                FROM {self.DA_SCHEMA}.{table_name}
                WHERE da_run_id = {self.DA_RUN_ID}
        """
        )

    def create_meta_info(
        self,
        table_name: str,
        fil_4th_node: str,
    ):
        """
        Helper function to create and inject meta info.
        """
        spark = SparkSession.getActiveSession()

        spark.sql(
            f"""
                INSERT INTO {self.DA_SCHEMA_DC}.job_otpt_meta
                SELECT {self.DA_RUN_ID} AS da_run_id
                    ,'TABLE' AS otpt_type
                    ,'{table_name}' AS otpt_name
                    ,'{self.DA_SCHEMA_DC}' AS otpt_lctn_txt
                    ,row_cnt AS rec_cnt
                    ,'{fil_4th_node}' AS fil_4th_node_txt
                    ,from_utc_timestamp(current_timestamp(), 'EST') as rec_add_ts
                    ,NULL as rec_updt_ts
                    ,NULL as trnct_ts
                FROM record_count
        """
        )

    def create_eftsmeta_info(
        self,
        table_name: str,
        pgm_name: str,
        step_name: str,
        object_name: str,
        audt_count: int,
    ):
        """
        Helper function to create and inject eftsmeta info.
        """
        spark = SparkSession.getActiveSession()

        spark.sql(
            f"""
                INSERT INTO {self.DA_SCHEMA}.efts_fil_meta (
                    da_run_id
                   ,fil_4th_node_txt
                   ,otpt_name
                   ,rptg_prd
                   ,itrtn_num
                   ,tot_rec_cnt
                   ,fil_cret_dt
                   ,incldd_state_cd
                   ,rec_cnt_by_state_cd
                   ,rec_add_ts
                   ,rec_updt_ts
                   ,fil_dt
                   ,taf_cd_spec_vrsn_name
                   ,rfrsh_vw_flag
                   ,ltst_run_ind
                   ,ccb_qtr
                   ,rif_finl_vrsn
                   ,rif_prelim_vrsn
                )
                SELECT t1.da_run_id
                    ,t2.fil_4th_node_txt
                    ,t2.otpt_name
                    ,date_format(cast(substring(t1.job_parms_txt, 1, 10) AS date), "MMMM,yyyy") AS rptg_prd
                    ,substring(t1.taf_cd_spec_vrsn_name, 2, 2) AS itrtn_num
                    ,t2.rec_cnt AS tot_rec_cnt
                    ,date_format(cast(t2.rec_add_ts AS date), "MM/dd/yyyy") AS fil_cret_dt
                    ,coalesce(t3.submtg_state_cd, 'Missing') AS incldd_state_cd
                    ,t3.audt_cnt_val AS rec_cnt_by_state_cd
                    ,from_utc_timestamp(current_timestamp(), 'EST') as rec_add_ts
                    ,NULL AS rec_updt_ts
                    ,{self.TAF_FILE_DATE} AS fil_dt
                    ,t1.taf_cd_spec_vrsn_name
                    ,False as rfrsh_vw_flag
                    ,False as ltst_run_ind
                    ,NULL as ccb_qtr
                    ,NULL as rif_finl_vrsn
                    ,NULL as rif_prelim_vrsn
                FROM {self.DA_SCHEMA}.job_cntl_parms as t1
                    ,{self.DA_SCHEMA}.job_otpt_meta as t2
                    ,{self.DA_SCHEMA}.pgm_audt_cnts as t3
                    ,pgm_audt_cnt_lkp as t4
                WHERE t1.da_run_id = {self.DA_RUN_ID}
                    AND t1.da_run_id = t2.da_run_id
                    AND t2.da_run_id = t3.da_run_id
                    AND t2.otpt_name = '{table_name}'
                    AND t3.pgm_audt_cnt_id = t4.pgm_audt_cnt_id
                    AND t1.sucsfl_ind = True
                    AND t4.pgm_name = '{pgm_name}'
                    AND t4.step_name = '{step_name}'
                    AND t4.obj_name = '{object_name}'
                    AND t4.audt_cnt_of = '{audt_count}'
        """
        )

    def final_control_info(self):
        """
        Create the final control info table.
        """
        spark = SparkSession.getActiveSession()

        spark.sql(
            f"""
                CREATE OR REPLACE TEMPORARY VIEW FINAL_CONTROL_INFO AS
                SELECT A.DA_RUN_ID
                    ,JOB_STRT_TS
                    ,JOB_END_TS
                    ,CAST(OTPT_NAME AS CHAR(100)) AS OTPTNAME
                    ,CAST(OTPT_LCTN_TXT AS CHAR(100)) AS OTPTLCTNTXT
                    ,REC_CNT
                FROM {self.DA_SCHEMA}.JOB_CNTL_PARMS as A
                    ,{self.DA_SCHEMA}.JOB_OTPT_META as B
                WHERE A.DA_RUN_ID = B.DA_RUN_ID
                    AND A.DA_RUN_ID = {self.DA_RUN_ID}
        """
        )

    def file_contents(self, table_name: str):
        """
        Helper function to display contents of a table given table name and da_run_id.
        """
        spark = SparkSession.getActiveSession()

        spark.sql(
            f"""
                SELECT *
                FROM {self.DA_SCHEMA}.{table_name}
                WHERE da_run_id = {self.DA_RUN_ID}
                LIMIT 1
        """
        )

    def getcounts(self, pgm_name: str, step_name: str):
        """
        Helper function to get counts of a table.
        """
        spark = SparkSession.getActiveSession()

        df = spark.sql(f"""
            SELECT *
            FROM pgm_audt_cnt_lkp
            WHERE pgm_name = "{pgm_name}"
                AND step_name = "{step_name}"
        """)

        dict_audt = df.toPandas().to_dict(orient="records")

        for i in dict_audt:
            spark.sql(f"""
                CREATE OR REPLACE TEMPORARY VIEW row_count AS
                SELECT da_run_id
                    ,pgm_audt_cnt_id
                    ,submtg_state_cd
                    ,audt_cnt_val
                FROM (
                    SELECT {self.DA_RUN_ID} AS da_run_id
                        ,{i.get("pgm_audt_cnt_id")} AS pgm_audt_cnt_id
                        ,t1.state AS submtg_state_cd
                        ,t1.cnt AS audt_cnt_val
                    FROM (
                        SELECT {i.get("grp_by")} AS state
                            ,count({i.get("audt_cnt_of")}) AS cnt
                        FROM {i.get("obj_name")}
                        GROUP BY {i.get("grp_by")}
                    ) AS t1
                ) AS t2
                UNION
                SELECT {self.DA_RUN_ID} AS da_run_id
                    ,{i.get("pgm_audt_cnt_id")} AS pgm_audt_cnt_id
                    ,"xx" as submtg_state_cd
                    ,0 AS audt_cnt_val
            """)

            rtCntDF = spark.sql("""
                SELECT CASE
                        WHEN t1.cnt = 1
                            THEN 1
                        WHEN t1.cnt > 1
                            THEN t1.cnt - 1
                        END AS rcnt
                FROM (
                    SELECT count(*) AS cnt
                    FROM row_count
                    ) AS t1
            """)

            rtcnt = rtCntDF.select(max(["rcnt"])).distinct().collect()[0][0]

            spark.sql(f"""
                INSERT INTO {self.DA_SCHEMA}.pgm_audt_cnts
                SELECT {self.DA_RUN_ID} AS da_run_id
                    ,pgm_audt_cnt_id
                    ,nullif(submtg_state_cd, 'xx') AS submtg_state_cd
                    ,audt_cnt_val
                    ,from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                    ,NULL as rec_updt_ts
                FROM row_count
                ORDER BY audt_cnt_val DESC
                LIMIT {rtcnt}
            """)

    def prepend(self, z: str):
        """
        Helper function to prepend segments to the query plan.
        """

        self.preplan.append(z)

    def append(self, segment: str, z: str):
        """
        Helper function to append segments to the query plan.
        """

        if segment not in self.plan.keys():
            self.plan[segment] = []

        v = '\n'.join(z.split('\n')[0:2])
        vs = v.split()

        if len(vs) >= 5:
            self.sql[vs[5]] = z

        # self.log(f"{self.tab_no}", z)
        self.plan[segment].append(z)

    def view_plan(self):
        """
        Helper function to view the query plan.
        """

        for sql in self.preplan:
            print('-- preplan')
            print(sql)

        for segment, chain in self.plan.items():
            for sql in chain:
                print(f"-- {segment}")
                print(sql)

    def write(self, module: str = ''):
        """
        Write the SQL files.
        """

        print('Writing SQL Files ...')

        for chain in self.preplan:

            print('\tpreplan...')
            for z in chain:

                v = '\n'.join(z.split('\n')[0:2])
                vs = v.split()

                if len(vs) >= 5:
                    # fn = './test/sql/python/' + 'preplan' + '/' + vs[5] + '.sql'
                    if module != '':
                        fn = '../../sql/python/' + module + '/' + 'preplan' + '/' + vs[5] + '.sql'
                    else:
                        fn = '../../sql/python/' + 'preplan' + '/' + vs[5] + '.sql'
                    print(fn)
                    f = open(fn, 'w')
                    f.write(z)
                    f.close()

        for segment, chain in self.plan.items():

            print('\t' + segment + '...')
            for z in chain:

                v = '\n'.join(z.split('\n')[0:2])
                vs = v.split()

                if len(vs) >= 5:
                    # fn = './test/sql/python/' + segment + '/' + vs[5] + '.sql'
                    if module != '':
                        fn = '../../sql/python/' + module + '/' + segment + '/' + vs[5] + '.sql'
                    else:
                        fn = '../../sql/python/' + segment + '/' + vs[5] + '.sql'
                    print(fn)
                    f = open(fn, 'w')
                    f.write(z)
                    f.close()

    def run(self):
        """
        Run the generated SQL query.
        """

        from taf.BSF.BSF_Metadata import BSF_Metadata
        from pyspark.sql.types import StructType, StructField, StringType
        import pandas as pd

        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()

        df = pd.DataFrame(BSF_Metadata.prmry_lang_cd, columns=['LANG_CD'])
        schema = StructType([StructField("LANG_CD", StringType(), True)])

        self.logger.info('Creating Primary Language Code Table...')

        sdf = spark.createDataFrame(data=df, schema=schema)
        sdf.registerTempTable('prmry_lang_cd')

        self.logger.info('Creating SSN Indicator View...')

        spark.sql(self.ssn_ind())

        self.logger.info('Creating TAF Views...')

        for chain in self.preplan:
            self.logger.info('\tpreplan...')
            for z in chain:
                # self.logger.info('\n'.join(z.split('\n')[0:2]))

                v = '\n'.join(z.split('\n')[0:2])
                vs = v.split()

                if len(vs) >= 5:
                    print('\t\t' + vs[5])

                # self.logger.info('\t\t' + v.split()[5])

                spark.sql(z)

        for segment, chain in self.plan.items():
            self.logger.info('\t' + segment + '...')
            for z in chain:
                # self.logger.info('\n'.join(z.split('\n')[0:2]))

                v = '\n'.join(z.split('\n')[0:2])
                vs = v.split()

                if len(vs) >= 5:
                    print('\t\t' + vs[5])

                # self.logger.info('\t\t' + v.split()[5])

                spark.sql(z)

    def audit(self):
        """
        Helper function to assist with auditing.  Nice to have, but not necessary.
        """

        from taf.BSF.BSF_Metadata import BSF_Metadata
        from pyspark.sql.types import StructType, StructField, StringType
        import pandas as pd

        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()

        if spark:

            df = pd.DataFrame(BSF_Metadata.prmry_lang_cd, columns=['LANG_CD'])
            schema = StructType([StructField("LANG_CD", StringType(), True)])

            sdf = spark.createDataFrame(data=df, schema=schema)
            sdf.registerTempTable('prmry_lang_cd')

            self.logger.info('Auditing  "0.1. create_initial_table" - "distinct msis_ident_num" ...')

            pdf = None
            for chain in self.preplan:
                self.logger.info('\tpreplan...')
                for z in chain:
                    v = '\n'.join(z.split('\n')[0:2])
                    vs = v.split()

                    if len(vs) >= 5:
                        # print('\t\t' + vs[5])

                        obj_name = v.split()[5]
                        if obj_name in ['ELG00002', 'ELG00003', 'ELG00004', 'ELG00005', 'ELG00006', 'ELG00007', 'ELG00008', 'ELG00009', 'ELG00010',
                                        'ELG00011', 'ELG00012', 'ELG00013', 'ELG00014', 'ELG00015', 'ELG00016', 'ELG00017', 'ELG00018', 'ELG00020',
                                        'ELG00021', 'TPL00002']:
                            sdf_audit_cnt = spark.sql(f"""
                                select
                                    '{obj_name}' as obj_name,
                                    submtg_state_cd,
                                    count(distinct msis_ident_num) as audt_cnt_val
                                from
                                    {obj_name}
                                group by
                                    obj_name,
                                    submtg_state_cd
                                order by
                                    obj_name,
                                    submtg_state_cd""")

                            if pdf is None:
                                print(obj_name)
                                pdf = sdf_audit_cnt.toPandas()
                            else:
                                print("appending - " + obj_name)
                                pdf = pdf.append(sdf_audit_cnt.toPandas())


            for segment, chain in self.plan.items():
                self.logger.info('\t' + segment + '...')
                for z in chain:
                    v = '\n'.join(z.split('\n')[0:2])
                    vs = v.split()

                    if len(vs) >= 5:
                        # print('\t\t' + vs[5])

                        obj_name = v.split()[5]
                        if obj_name in ['ELG00002', 'ELG00003', 'ELG00004', 'ELG00005', 'ELG00006', 'ELG00007', 'ELG00008', 'ELG00009', 'ELG00010',
                                        'ELG00011', 'ELG00012', 'ELG00013', 'ELG00014', 'ELG00015', 'ELG00016', 'ELG00017', 'ELG00018', 'ELG00020',
                                        'ELG00021', 'TPL00002']:
                            sdf_audit_cnt = spark.sql(f"""
                                select
                                    '{obj_name}' as obj_name,
                                    submtg_state_cd,
                                    count(distinct msis_ident_num) as audt_cnt_val
                                from
                                    {obj_name}
                                group by
                                    obj_name,
                                    submtg_state_cd
                                order by
                                    obj_name,
                                    submtg_state_cd""")

                            if pdf is None:
                                print(obj_name)
                                pdf = sdf_audit_cnt.toPandas()
                            else:
                                print("appending - " + obj_name)
                                pdf = pdf.append(sdf_audit_cnt.toPandas())

            return pdf
        else:
            self.logger.info('No valid Spark Session')
            return None


# -----------------------------------------------------------------------------
# CC0 1.0 Universal

# Statement of Purpose

# The laws of most jurisdictions throughout the world automatically confer
# exclusive Copyright and Related Rights (defined below) upon the creator and
# subsequent owner(s) (each and all, an "owner") of an original work of
# authorship and/or a database (each, a "Work").

# Certain owners wish to permanently relinquish those rights to a Work for the
# purpose of contributing to a commons of creative, cultural and scientific
# works ("Commons") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Commons to promote the ideal of a free culture and the
# further production of creative, cultural and scientific works, or to gain
# reputation or greater distribution for their Work in part through the use and
# efforts of others.

# For these and/or other purposes and motivations, and without any expectation
# of additional consideration or compensation, the person associating CC0 with a
# Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
# and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
# and publicly distribute the Work under its terms, with knowledge of his or her
# Copyright and Related Rights in the Work and the meaning and intended legal
# effect of CC0 on those rights.

# 1. Copyright and Related Rights. A Work made available under CC0 may be
# protected by copyright and related or neighboring rights ("Copyright and
# Related Rights"). Copyright and Related Rights include, but are not limited
# to, the following:

#   i. the right to reproduce, adapt, distribute, perform, display, communicate,
#   and translate a Work

#   ii. moral rights retained by the original author(s) and/or performer(s)

#   iii. publicity and privacy rights pertaining to a person's image or likeness
#   depicted in a Work

#   iv. rights protecting against unfair competition in regards to a Work,
#   subject to the limitations in paragraph 4(a), below

#   v. rights protecting the extraction, dissemination, use and reuse of data in
#   a Work

#   vi. database rights (such as those arising under Directive 96/9/EC of the
#   European Parliament and of the Council of 11 March 1996 on the legal
#   protection of databases, and under any national implementation thereof,
#   including any amended or successor version of such directive) and

#   vii. other similar, equivalent or corresponding rights throughout the world
#   based on applicable law or treaty, and any national implementations thereof.

# 2. Waiver. To the greatest extent permitted by, but not in contravention of,
# applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
# unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
# and Related Rights and associated claims and causes of action, whether now
# known or unknown (including existing as well as future claims and causes of
# action), in the Work (i) in all territories worldwide, (ii) for the maximum
# duration provided by applicable law or treaty (including future time
# extensions), (iii) in any current or future medium and for any number of
# copies, and (iv) for any purpose whatsoever, including without limitation
# commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
# the Waiver for the benefit of each member of the public at large and to the
# detriment of Affirmer's heirs and successors, fully intending that such Waiver
# shall not be subject to revocation, rescission, cancellation, termination, or
# any other legal or equitable action to disrupt the quiet enjoyment of the Work
# by the public as contemplated by Affirmer's express Statement of Purpose.

# 3. Public License Fallback. Should any part of the Waiver for any reason be
# judged legally invalid or ineffective under applicable law, then the Waiver
# shall be preserved to the maximum extent permitted taking into account
# Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
# is so judged Affirmer hereby grants to each affected person a royalty-free,
# non transferable, non sublicensable, non exclusive, irrevocable and
# unconditional license to exercise Affirmer's Copyright and Related Rights in
# the Work (i) in all territories worldwide, (ii) for the maximum duration
# provided by applicable law or treaty (including future time extensions), (iii)
# in any current or future medium and for any number of copies, and (iv) for any
# purpose whatsoever, including without limitation commercial, advertising or
# promotional purposes (the "License"). The License shall be deemed effective as
# of the date CC0 was applied by Affirmer to the Work. Should any part of the
# License for any reason be judged legally invalid or ineffective under
# applicable law, such partial invalidity or ineffectiveness shall not
# invalidate the remainder of the License, and in such case Affirmer hereby
# affirms that he or she will not (i) exercise any of his or her remaining
# Copyright and Related Rights in the Work or (ii) assert any associated claims
# and causes of action with respect to the Work, in either case contrary to
# Affirmer's express Statement of Purpose.

# 4. Limitations and Disclaimers.

#   a. No trademark or patent rights held by Affirmer are waived, abandoned,
#   surrendered, licensed or otherwise affected by this document.

#   b. Affirmer offers the Work as-is and makes no representations or warranties
#   of any kind concerning the Work, express, implied, statutory or otherwise,
#   including without limitation warranties of title, merchantability, fitness
#   for a particular purpose, non infringement, or the absence of latent or
#   other defects, accuracy, or the present or absence of errors, whether or not
#   discoverable, all to the greatest extent permissible under applicable law.

#   c. Affirmer disclaims responsibility for clearing rights of other persons
#   that may apply to the Work or any use thereof, including without limitation
#   any person's Copyright and Related Rights in the Work. Further, Affirmer
#   disclaims responsibility for obtaining any necessary consents, permissions
#   or other rights required for any use of the Work.

#   d. Affirmer understands and acknowledges that Creative Commons is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecommons.org/publicdomain/zero/1.0/>
