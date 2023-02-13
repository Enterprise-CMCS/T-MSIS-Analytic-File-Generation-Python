from taf.APR.APR_Runner import APR_Runner
from taf.TAF import TAF


class APR(TAF):
    """
    Annual Provider (APR) TAF: The APR TAF contain information about each Medicaid and CHIP provider
    that had a qualifying T-MSIS provider attributes main record, as reflected by the effective and
    end dates, during the calendar year. The information contained in the APR TAF includes but is not
    limited to: provider name, provider taxonomy, enrollment status, the provider group (if applicable), 
    programs with which the provider is affiliated, addresses, license number(s), NPI and other providers, 
    and facility-specific information (if applicable, e.g. bed count), and the various Medicaid/CHIP 
    programs/waivers/demonstrations, locations, licenses, and identifiers (state-specific as well as national 
    provider identifiers) associated with the provider. Each APR TAF is comprised of nine files: the Base file; 
    Affiliated Groups file; Affiliated Programs file; Taxonomy file; Enrollment file; Location file; Licensing file; 
    Identifiers file; and Bed-Type file. All nine files can be linked together using unique keys that are 
    constructed based on various data elements. The nine APR TAF are generated for each calendar year 
    in which the data are reported.
    """

    def __init__(self, apr: APR_Runner):

        self.apr = apr
        self.st_fil_type = 'APR'
        self.fil_typ = 'PR'
        self.LFIL_TYP = self.fil_typ.lower()
        self.main_id = 'SUBMTG_STATE_PRVDR_ID'  # main_id= MC_PLAN_ID for MCP and SUBMTG_STATE_PRVDR_ID for PRV
        self.loc_id = 'PRVDR_LCTN_ID'
        self.monthsb = ['12', '11', '10', '09', '08', '07', '06', '05', '04', '03', '02', '01']
        self.year = self.apr.reporting_period.year

        APR.max_run_id(self, 'PRV', '', self.apr.reporting_period.year)

   
    def create(self, tblname, FIL_4TH_NODE):
        """
        Function create_segment, which will be called for each of the segments to run the respective
        create Function, which includes output to the permanent table. This Function will then get the count
        of records in that table, and output to the metadata table. (Note Function CREATE_META_INFO is in
        AWS_Shared_Functions)

        Function parms:
            -tblname=name of the output permanent table name (after TAF_ANN_{fil_typ}_)
            -FIL_4TH_NODE=abbreviated name of the table to go into the metadata table
        """

        # %create_{tblname};

        # sysecho 'in get cnt';
        # %get_ann_count({tblname}):

        # sysecho 'create metainfo';
        # %CREATE_META_INFO({self.apr.DA_SCHEMA}, TAF_ANN_{fil_typ}_{tblname}, {self.apr.DA_RUN_ID}, &ROWCOUNT., &FIL_4TH_NODE.):

        # sysecho 'create EFT file metadata';
        # %create_efts_metadata;
        pass

    def max_run_id(self, file: str, tbl: str, inyear):
        """
        Function max_run_id to get the highest da_run_id for the given state for input monthly TAF. This
        table will then be merged back to the monthly TAF to pull all records for that state, month, and da_run_id.
        It is also inserted into the metadata table to keep a record of the state/month DA_RUN_IDs that make up 
        each annual run.
        To get the max run ID, must go to the job control table and get the latest national run, and then also
        get the latest state-specific run. Determine the later by state and month and then pull those IDs.

        Function parms:
            inyear=input year, set to the current year
        """

        filel = file.lower()
        if tbl == '':
            tbl = f"taf_{filel}"

        # For NON state-specific runs (where job_parms_text does not include submtg_state_cd in),
        #    pull highest da_run_id by time ;

        z = f"""
                CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear}_nat AS
                SELECT {file}_fil_dt
                    ,max(da_run_id) AS da_run_id
                FROM (
                    SELECT substring(job_parms_txt, 1, 4) || substring(job_parms_txt, 6, 2) AS {file}_fil_dt
                        ,da_run_id
                    FROM {self.apr.DA_SCHEMA}.job_cntl_parms
                    WHERE upper(fil_type) = '{file}'
                        AND sucsfl_ind = 1
                        AND substring(job_parms_txt, 1, 4) = '{inyear}'
                        AND charindex('submtg_state_cd in', regexp_replace(job_parms_txt, '\\\s+', ' ')) = 0
                    )
                GROUP BY {file}_fil_dt
        """
        self.apr.append(type(self).__name__, z)

        # For state-specific runs (where job_parms_text includes submtg_state_cd in),
        #    pull highest da_run_id by time and state;

        z = f"""
                CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear}_ss AS
                SELECT {file}_fil_dt
                    ,submtg_state_cd
                    ,max(da_run_id) AS da_run_id
                FROM (
                    SELECT substring(job_parms_txt, 1, 4) || substring(job_parms_txt, 6, 2) AS {file}_fil_dt
                        ,regexp_extract(substring(job_parms_txt, 10), '([0-9]{{2}})') AS submtg_state_cd
                        ,da_run_id
                    FROM {self.apr.DA_SCHEMA}.job_cntl_parms
                    WHERE upper(fil_type) = '{file}'
                        AND sucsfl_ind = 1
                        AND substring(job_parms_txt, 1, 4) = '{inyear}'
                        AND charindex('submtg_state_cd in', regexp_replace(job_parms_txt, '\\\s+', ' ')) > 0
                    )
                GROUP BY {file}_fil_dt
                    ,submtg_state_cd
        """
        self.apr.append(type(self).__name__, z)

        # Now join the national and state lists by month - take the national run ID if higher than
        #    the state-specific, otherwise take the state-specific.
        #    Must ALSO stack with the national IDs so they are not lost.
        #    In outer query, get a list of unique IDs to pull;

        z = f"""
                CREATE OR REPLACE TEMPORARY VIEW job_cntl_parms_both_{file}_{inyear} AS
                SELECT DISTINCT {file}_fil_dt
                    ,da_run_id
                FROM (
                    SELECT coalesce(a.{file}_fil_dt, b.{file}_fil_dt) AS {file}_fil_dt
                        ,CASE
                            WHEN a.da_run_id > b.da_run_id
                                OR b.da_run_id IS NULL
                                THEN a.da_run_id
                            ELSE b.da_run_id
                            END AS da_run_id
                    FROM max_run_id_{file}_{inyear}_nat a
                    FULL JOIN max_run_id_{file}_{inyear}_ss b ON a.{file}_fil_dt = b.{file}_fil_dt

                    UNION ALL

                    SELECT {file}_fil_dt
                        ,da_run_id
                    FROM max_run_id_{file}_{inyear}_nat
                    )
        """
        self.apr.append(type(self).__name__, z)

        # Now join to EFTS data to get table of month/state/run IDs to use for data pull.
        #    Note must then take the highest da_run_id by state/month (if any state-specific runs
        #    were identified as being later than a national run):

        z = f"""
            create or replace temporary view max_run_id_{file}_{inyear} as
            select a.{file}_fil_dt
                ,b.submtg_state_cd
                ,max(b.da_run_id) as da_run_id
                ,max(b.fil_cret_dt) as fil_cret_dt

            from job_cntl_parms_both_{file}_{inyear} a
                inner join
                (select da_run_id, incldd_state_cd as submtg_state_cd, fil_cret_dt
                from {self.apr.DA_SCHEMA}.efts_fil_meta where incldd_state_cd != 'Missing' ) b

            on a.da_run_id = b.da_run_id
        """

        if (str(self.apr.ST_FILTER).find("ALL") != -1):
            z += f"""
                 WHERE {self.apr.ST_FILTER}
            """

        z += f"""
            group by a.{file}_fil_dt
                    ,b.submtg_state_cd
        """
        self.apr.append(type(self).__name__, z)

        #    Insert into metadata table so we keep track of all monthly DA_RUN_IDs
        #    that go into each annual {fil_typ} file

        z = f"""
            insert into {self.apr.DA_SCHEMA}.TAF_ANN_{self.st_fil_type}_INP_SRC
            select
                {self.apr.DA_RUN_ID} as ANN_DA_RUN_ID,
                'apr' as ann_fil_type,
                SUBMTG_STATE_CD,
                lower('{file}') as src_fil_type,
                {file}_FIL_DT as src_fil_dt,
                DA_RUN_ID as SRC_DA_RUN_ID,
			    fil_cret_dt as src_fil_creat_dt

            from max_run_id_{file}_{inyear}
        """
        self.apr.append(type(self).__name__, z)

    def join_monthly(self, fileseg, fil_typ, inyear, main_id):
        """
        Function join_monthly to join the max da_run_ids for the given state/month back to the monthly TAF and
        then join each month by submtg_state_cd and main_id (plan or provider). Note this table will be pulled into the subquery in the
        creation of each base and supplemental segment.
        """

        if fil_typ == 'PL':
            file = 'MCP'
        else:
            file = 'PRV'

        file='PRV'

        if self.fileseg.casefold() == 'bsp':
            fileseg = 'PRV'
        else:
            fileseg = self.fileseg

        # Create the backbone of unique state/msis_id to then left join each month back to
        fseg = f"""
            (select 
                a.submtg_state_cd,
                b.{self.main_id},
                count(a.submtg_state_cd) as nmonths
            from 
                max_run_id_{file}_{inyear} a
            inner join 
                {self.apr.DA_SCHEMA}.taf_{fileseg} as b
            on 
                a.submtg_state_cd = b.submtg_state_cd and
                a.{file}_fil_dt = b.{file}_fil_dt and
                a.da_run_id = b.da_run_id
            group by 
                a.submtg_state_cd,
                b.{self.main_id}
            ) as fseg
            """

        result = []
        result.append(fseg.format())

        # Now loop through each month to left join back to the backbone
        for m in range(1, 13):

            mm = '{:02d}'.format(m)

            z = f"""
                left join

                    (select b.*
                    from
                        max_run_id_{file}_{inyear} a
                    inner join
                        {self.apr.DA_SCHEMA}.taf_{fileseg} b
                    on 
                        a.submtg_state_cd = b.submtg_state_cd and
                        a.{file}_fil_dt = b.{file}_fil_dt and
                        a.da_run_id = b.da_run_id

                    where 
                        substring(a.{file}_fil_dt,5,2)='{mm}'
                    ) as m{mm}

                on 
                    fseg.submtg_state_cd=m{mm}.submtg_state_cd and
                    fseg.{self.main_id}=m{mm}.{self.main_id}
            """
            result.append(z.format())

        return "\n    ".join(result)
    
    def all_monthly_segments(self, filet):
        """
        Function all_monthly_segment(intbl=, filet=) to join the records with max da_run_ids for the given state/month back to the monthly TAF and
        select all records for the target year (plan or provider). Note: this table will be the source for creation of annual supplemental segments.
        """

        if self.fileseg.casefold() in ('bed', 'lic', 'idt'):
            splmtl_submsn_type = f"""
                when right(b.{filet}_loc_link_key,5)='-CHIP' then 'CHIP'
                when right(b.{filet}_loc_link_key,4)='-TPA' then 'TPA'
                     else ' '
                """
        else:
            splmtl_submsn_type = f"""
                when right(b.{filet}_link_key,5)='-CHIP' then 'CHIP'
                when right(b.{filet}_link_key,4)='-TPA' then 'TPA'
                     else ' '
                """

        if self.fileseg.casefold() in ('bed', 'enr', 'grp', 'idt', 'lic', 'loc', 'pgm', 'tax'):
            b = f"{self.apr.DA_SCHEMA}.taf_{filet}_{self.fileseg}"
        else:
            b = f"{self.apr.DA_SCHEMA}.taf_{self.fileseg}"

        if filet == 'PRV':
            b = f"{self.apr.DA_SCHEMA}.taf_{filet}_{self.fileseg}"
        else:
            b = f"{self.apr.DA_SCHEMA}.taf_{self.fileseg}"            

        # Create file that includes state/id/submission type and other data elements for all records in the year for this segment
        z = f"""
                select  
                    b.*
                from
                    max_run_id_{filet}_{self.year} a
                inner join
                    {b} b
                on 
                    a.submtg_state_cd = b.submtg_state_cd and
                    a.{filet}_fil_dt = b.{filet}_fil_dt and
                    a.da_run_id = b.da_run_id

        """
        return z.format()

    def create_temp_table(self, fileseg, tblname, inyear, subcols, outercols='', subcols2='', subcols3='', subcols4='', subcols5='', subcols6='', subcols7='', subcols8=''):
        """
        Function create_temp_table to create each main table. For each table, there are columns we must get from the raw data in
        the subquery, and then columns we must get from the outer query that pulls from the subquery.

        Function parms:
        fileseg: MCP options MCP/MCL/MCS/MCE - for OA a different method is used since no monthly supplimental file exists
                PRV options PRV/PRV_LOC/PRV_GRP/PRV_PGM/PRV_TAX/PRV_ENR/PRV_LIC/PRV_IDT/PRV_BED
        tblname=table name
        subcols=creation statements for all columns that must be pulled from the raw data in the subquery
        outercols=creation statements for all columns that must be pulled from the subquery
        subcols2 - subcols8=additional subcols when needing to loop over MC and waiver slots, because cannot
                            loop over all slots within one Function var or will exceed text limit of 65534 chars
        """

        # distkey({self.main_id})
        # sortkey(submtg_state_cd,{self.main_id},splmtl_submsn_type) as

        z = f"""
            create or replace temporary view {tblname}_{self.year} as
            select *, {outercols}
                from (
                    select fseg.submtg_state_cd
                        ,fseg.{self.main_id}
                        ,{subcols}
                         {subcols2}
                         {subcols3}
                         {subcols4}
                         {subcols5}
                         {subcols6}
                         {subcols7}
                         {subcols8}

                    from ( {self.join_monthly(fileseg, self.fil_typ, inyear, self.main_id) } )

                    ) sub

            order by submtg_state_cd,
                    {self.main_id}

        """
        self.apr.append(type(self).__name__, z)

    def annual_segment(self, fileseg, dtfile, collist, mnths, outtbl):
        """
        fileseg - identifies which file segment is being created
        dtfile - XXX part of XXX_FIL_DT file date field with YYYYMM values
        collist - Function variable with unique file grouping vars (separated by commas)
        mnths - base name for the monthly flag fields
        outtbl - name of the output table
        """

        z = f"""
             create or replace temporary view temp_rollup_{fileseg} as
             select SUBMTG_STATE_CD, {self.main_id}, { ', '.join(collist) },
                    sum(case when (substring({dtfile}_fil_dt,5,2)='01') then 1 else 0 end) as _01,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='02') then 1 else 0 end) as _02,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='03') then 1 else 0 end) as _03,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='04') then 1 else 0 end) as _04,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='05') then 1 else 0 end) as _05,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='06') then 1 else 0 end) as _06,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='07') then 1 else 0 end) as _07,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='08') then 1 else 0 end) as _08,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='09') then 1 else 0 end) as _09,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='10') then 1 else 0 end) as _10,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='11') then 1 else 0 end) as _11,
                    sum(case when (substring({dtfile}_fil_dt,5,2)='12') then 1 else 0 end) as _12
             from ( { self.all_monthly_segments(filet=dtfile) } )
             group by SUBMTG_STATE_CD, {self.main_id}, { ', '.join(collist) }
             order by SUBMTG_STATE_CD, {self.main_id}, { ', '.join(collist) }
          """
        self.apr.append(type(self).__name__, z)

        z = f"""
             create or replace temporary view {outtbl} as
             select SUBMTG_STATE_CD, {self.main_id}, { ', '.join(collist) },
                    case when (_01>0) then 1 else 0 end as {mnths}_01,
                    case when (_02>0) then 1 else 0 end as {mnths}_02,
                    case when (_03>0) then 1 else 0 end as {mnths}_03,
                    case when (_04>0) then 1 else 0 end as {mnths}_04,
                    case when (_05>0) then 1 else 0 end as {mnths}_05,
                    case when (_06>0) then 1 else 0 end as {mnths}_06,
                    case when (_07>0) then 1 else 0 end as {mnths}_07,
                    case when (_08>0) then 1 else 0 end as {mnths}_08,
                    case when (_09>0) then 1 else 0 end as {mnths}_09,
                    case when (_10>0) then 1 else 0 end as {mnths}_10,
                    case when (_11>0) then 1 else 0 end as {mnths}_11,
                    case when (_12>0) then 1 else 0 end as {mnths}_12
             from temp_rollup_{fileseg}
             order by SUBMTG_STATE_CD, {self.main_id}, { ', '.join(collist) }
          """
        self.apr.append(type(self).__name__, z)

    def any_col(incols, outcol, condition='=1'):
        """
        Function any_col to look across a list of columns (non-monthly) to determine if ANY meet a given
        condition. The default condition is = 1.

        Function parms:
        incols=input columns
        outcol=name of column to be output 
        condition=monthly condition to be evaulated, where default is = 1
        """

        cases = []
        for col in incols.split():
            cases.append(f", case when {col} {condition}")

        return f"case when {' or '.join(cases)} then 1 else 0 end as {outcol}"

    def sum_months(incol, raw=0, outcol=''):
        """
        Function sum_months to take a SUM over all the input months.

        Function parms:
        incol=input monthly column which will be summed (with _MO suffix for each month)
        raw=indicator for whether the monthly variables are raw (must come from the 12 monthly files) or were created
            in an earlier subquery and will therefore have the _MO suffixes, where default = 0
        outcol=output column with summation, where the default is the incol name with the _MONTHS suffix
        """


        if outcol == '':
            outcol = incol + '_MOS'

        if raw == 1:

            z = f""",
                coalesce(m01.{incol}, 0) + coalesce(m02.{incol}, 0) + coalesce(m03.{incol}, 0) +
                coalesce(m04.{incol}, 0) + coalesce(m05.{incol}, 0) + coalesce(m06.{incol}, 0) +
                coalesce(m07.{incol}, 0) + coalesce(m08.{incol}, 0) + coalesce(m09.{incol}, 0) +
                coalesce(m10.{incol}, 0) + coalesce(m11.{incol}, 0) + coalesce(m12.{incol}, 0)"""

        if raw == 0:

            z = f""",
                coalesce({incol}_01, 0) + coalesce({incol}_02, 0) + coalesce({incol}_03, 0) +
                coalesce({incol}_04, 0) + coalesce({incol}_05, 0) + coalesce({incol}_06, 0) +
                coalesce({incol}_07, 0) + coalesce({incol}_08, 0) + coalesce({incol}_09, 0) +
                coalesce({incol}_10, 0) + coalesce({incol}_11, 0) + coalesce({incol}_12, 0)"""

        return f"{z} as {outcol}"

    def monthly_array_ind_raw(incol, outcol=''):
        """
        Function to return a case statement of the monthly array raw indexes.
        """

        if outcol == '':
            outcol = incol

        cases = []
        for m in range(1, 12):
            mm = '{:02d}'.format(m)
            cases.append(f",case when m{mm}{incol} is not null then 1 else 0 end as {outcol}_{mm}")

        return ' '.join(cases)

    def map_arrayvars(varnm='', N=1):
        """
        Function to return the map array variables.  
        """

        vars = []
        for I_ in range(1, N):
            i = '{:02d}'.format(I_)
            vars.append(f", max(case when (_ndx={I_}) then {varnm} else null end) as {varnm}_{i}")

        return ' '.join(vars)

    def nonmiss_month(self, incol, outcol=''):
        """
        Function nonmiss_month to loop through given variable from month 12 to 1 and identify the month with
        the first non-missing value. This will then be used to pull additional columns that should be paired
        with that month. The month = 00 if NO non-missing month.

        Function parms:
        incol=input monthly column
        outcol=output column with month number, where the default is the incol name with the _MN (month number) suffix
        """

        if outcol == '':
            outcol = incol + '_MN'

        cases = []
        for m in self.monthsb:

            z = f"""m{m}.{incol} is not null """
            z += f"""and m{m}.{incol} not in ('',' ') then '{m}'"""

            cases.append(z)

        return f"""case when {' when '.join(cases)} else '00' end as {outcol}"""

    def ind_nonmiss_month(self, outcol):
        """
        Function ind_nonmiss_month to loop through individual provider variables
        from month 12 to 1 and identify the month with the first non-missing value
        for any of those variables. This will then be used to get those variables 
        from that month if needed. The month = 00 if NO non-missing month.
        """

        cases = []
        for m in self.monthsb:
            cases.append(f"when (m{m}.PRVDR_1ST_NAME is not null and m{m}.PRVDR_1ST_NAME not in ('',' ')) or ")
            cases.append(f"     (m{m}.PRVDR_MDL_INITL_NAME is not null and m{m}.PRVDR_MDL_INITL_NAME not in ('',' ')) or ")
            cases.append(f"     (m{m}.PRVDR_LAST_NAME is not null and m{m}.PRVDR_LAST_NAME not in ('',' ')) or ")
            cases.append(f"     (m{m}.GNDR_CD is not null and m{m}.GNDR_CD not in ('',' ')) or ")
            cases.append(f"     (m{m}.BIRTH_DT is not null) or ")
            cases.append(f"     (m{m}.DEATH_DT is not null) or ")
            cases.append(f"     (m{m}.AGE_NUM is not null) then '{m}'")

        return f"case {' '.join(cases)} else '00' end as {outcol}"

    def assign_nonmiss_month(self, outcol, monthval1, incol1, monthval2='', incol2=''):
        """
        Function assign_nonmiss_month looks at the values for the monthly variables assigned in nonmiss_month,
        and pulls multiple variables for that month based on the assigned month from nonmiss_month. Note
        this can be based on 1 or 2 monthly assignments from nonmiss_month, where the first is evaluated and
        if a month is never assigned to that variable, the second will be evaluated. This happens for HOME and
        MAIL address. Note that nonmiss_month must be run in the subquery before assign_nonmiss_month is run in
        the outer query.

        Function parms:
            outcol=column to assign based on the month captured in nonmiss_month,
            monthval1=monthly value to evaluate captured in nonmiss_month,
            incol1=input column to assign if monthval1 is met,
            monthval2=optional monthly value to evaluate captured in nonmiss_month, IF monthval1=00,
            incol2=optional input column to assign if monthval2 is met

        """

        cases = []
        for m in self.monthsb:
            cases.append(f"when {monthval1}='{m}' then {incol1}_{m}")

            if monthval2 != '':
                for m in self.monthsb:
                    cases.append(f"when {monthval2}='{m}' then {incol2}_{m}")

        return f"case {' '.join(cases)} else null end as {outcol}"

    def create_splmlt(self, segname, segfile):
        """
        Create or replace temporary view <segment name>._SPLMTL to join to base
        """

        # distkey({self.main_id})
        # sortkey(submtg_state_cd,{self.main_id}) as
        z = f"""
            create or replace temporary view {segname}_SPLMTL_{self.year} as

            select 
                submtg_state_cd
                ,{self.main_id}
                ,count(submtg_state_cd) as {segname}_SPLMTL_CT

            from 
                {segfile}

            group by 
                submtg_state_cd,
                {self.main_id}
        """
        self.apr.append(type(self).__name__, z)

    def table_id_cols(self, loctype=0):
        """
        Function table_id_cols to add the 6 cols that are the same across all tables into the final insert select
        statement (DA_RUN_ID, {fil_typ}_LINK_KEY, {fil_typ}_FIL_DT, ANN_{fil_typ}_VRSN, SUBMTG_STATE_CD, &main_id)
        link key includes supplimental state submission code for 'CHIP' or 'TPA' from the monthly TAF link key.
        fil_typ - so this can be used for more than one TAF file type
        """

        cols = []

        cols.append(f"{self.apr.DA_RUN_ID} as DA_RUN_ID")

        if loctype == 0 or loctype == 1:

            cols.append(f"""
            cast (('{self.apr.DA_RUN_ID}' || '-' || '{self.year}' || '-' || '{self.apr.version}' || '-' ||
                    SUBMTG_STATE_CD || '-' || {self.main_id}) as varchar(56)) as {self.fil_typ}_LINK_KEY
                    """)

            if loctype == 1:

                cols.append(f"""
                cast (('{self.apr.DA_RUN_ID}' || '-' || '{self.year}' || '-' || '{self.apr.version}' || '-' ||
                        SUBMTG_STATE_CD || '-' || {self.main_id} || '-' || coalesce(PRVDR_LCTN_ID, '**')) as varchar(74)) as {self.fil_typ}_LOC_LINK_KEY
                        """)

        elif loctype == 2:

            cols.append(f"""
            cast (('{self.apr.DA_RUN_ID}' || '-' || '{self.year}' || '-' || '{self.apr.version}' || '-' ||
                    SUBMTG_STATE_CD || '-' || {self.main_id} || '-' || coalesce(PRVDR_LCTN_ID, '**')) as varchar(74)) as {self.fil_typ}_LOC_LINK_KEY
                    """)

        cols.append(f"""'{self.year}' as {self.fil_typ}_FIL_DT""")
        cols.append(f"""'{self.apr.version}' as {self.fil_typ}_VRSN""")
        cols.append(f"""SUBMTG_STATE_CD""")
        cols.append(f"""{self.main_id}""")

        return ','.join(cols.copy())

    def get_ann_count(self, tblname):
        """
        Function get_ann_cnt to get the count of the given table and put the count into a Function var
        Function parms: tblname=perm table name
        """

        z = f"""
            (select count(submtg_state_cd) as row_cnt
                from {self.apr.DA_SCHEMA}.TAF_ANN_{self.fil_typ}_{tblname}
                where da_run_id={self.apr.DA_RUN_ID}
            )
        """
        self.apr.append(type(self).__name__, z)

    def create_efts_metadata(self, tblname):
        """
        Function create_efts_metadata to get the count of the given table by state and insert into the EFT
        metadata table. Will be called in the get_segment Function.
        """

        #  Create state counts and insert into metadata table

        z = f"""
            create or replace temporary view state_counts as
            select da_run_id,
                submtg_state_cd,
                count(submtg_state_cd) as rowcount_state

            from {self.apr.DA_SCHEMA}.TAF_ANN_{self.st_fil_typ}_{tblname}
            where da_run_id = {self.apr.DA_RUN_ID}
            group by da_run_id,
                     submtg_state_cd
        """
        self.apr.append(type(self).__name__, z)

        #    Insert the following values into the table:
        #     - DA_RUN_ID
        #     - 4th node text value
        #     - table name
        #     - year
        #     - version number (01)
        #     - overall table count
        #     - run date
        #     - state code
        #     - table count for given state
        #     - fil_dt
        #     - version

        z = f"""
            insert into {self.apr.DA_SCHEMA}.EFTS_FIL_META
            (da_run_id,fil_4th_node_txt,otpt_name,rptg_prd,itrtn_num,tot_rec_cnt,fil_cret_dt,incldd_state_cd,rec_cnt_by_state_cd,fil_dt,taf_cd_spec_vrsn_name)

            select a.da_run_id
                ,'&FIL_4TH_NODE.' as fil_4th_node_txt
                ,'TAF_ANN_{self.fil_typ}_{tblname}' as otpt_name
                ,'{self.apr.YEAR}' as rptg_prd
                ,'{self.apr.VERSION}' as itrtn_num
                ,&rowcount. as tot_rec_cnt
                ,to_char(date(c.rec_add_ts),'MM/DD/YYYY') as fil_cret_dt
                ,submtg_state_cd as incldd_state_cd
                ,rowcount_state as rec_cnt_by_state_cd
                ,'{self.apr.YEAR}' as fil_dt
                ,b.taf_cd_spec_vrsn_name

            from state_counts a
                inner join
                {self.apr.DA_SCHEMA}.JOB_CNTL_PARMS b

                on a.da_run_id = b.da_run_id

                inner join
                (select * from {self.apr.DA_SCHEMA}.JOB_OTPT_META where fil_4th_node_txt = 'FIL_4TH_NODE' ) c

                on a.da_run_id = c.da_run_id
        """
        self.apr.append(type(self).__name__, z)


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
# <http://creativecommons.org/publicdomain/zero/1.0/>elg00005
