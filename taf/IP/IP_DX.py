from taf.IP.IP_Runner import IP_Runner
from taf.IP.IP_Metadata import IP_Metadata
from taf.TAF_Closure import TAF_Closure
from taf.TAF_Metadata import TAF_Metadata

class IP_DX:
    """
    Each IP TAF is comprised of two files - a claim-header level file and a claim-line level file and a diagnosis level file.
    The claims included in these files are active, final-action, non-voided, and non-denied claims.
    Only header claims with a date in the TAF month/year, along with their associated claim line and diagnosis
    records, are included. All files can be linked together using a unique key that is constructed
    based on various claim header, claim line, and claim DX data elements. The three IP TAF are produced for each
    calendar month in which the data are reported.
    """

    def create(self, runner: IP_Runner):
        """
        Create the IP claim-DX level segment.
        """
    
        z = f"""
        create or replace temporary view IP_DX as
            select 
            {runner.DA_RUN_ID} as DA_RUN_ID,
            {runner.get_link_key()} as IP_LINK_KEY,
            '{runner.VERSION}' as IP_VRSN,
            '{runner.TAF_FILE_DATE}' as IP_FIL_DT
            ,TMSIS_RUN_ID
            ,{ TAF_Closure.var_set_type1('MSIS_IDENT_NUM') }
            ,NEW_SUBMTG_STATE_CD as SUBMTG_STATE_CD
            , { TAF_Closure.var_set_type3('orgnl_clm_num', cond1='~') }
            , { TAF_Closure.var_set_type3('adjstmt_clm_num', cond1='~') }
            , ADJSTMT_IND_CLEAN as ADJSTMT_IND
            , case
                when (ADJDCTN_DT < to_date('1600-01-01')) then to_date('1599-12-31')
                when ADJDCTN_DT=to_date('1960-01-01') then NULL
                else ADJDCTN_DT
                end as ADJDCTN_DT
            , case when trim(upper(dgns_type_cd)) in {tuple(TAF_Metadata.DGNS_TYPE_CD_values)} then trim(upper(dgns_type_cd)) else NULL end as DGNS_TYPE_CD
            , DGNS_SQNC_NUM
            ,{ TAF_Closure.var_set_type2('DGNS_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
            ,{ TAF_Closure.var_set_type1('DGNS_CD') }
            ,{ TAF_Closure.var_set_type4('DGNS_POA_IND', 'YES', cond1='Y', cond2='N', cond3='U', cond4='W', cond5='1') }
            ,from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
            ,from_utc_timestamp(current_timestamp(), 'EST') as REC_UPDT_TS             --this must be equal to REC_ADD_TS for CCW pipeline
            ,taf_classic_ind
            from (
                select
                    *,
                    case when ADJSTMT_IND is NOT NULL and
                    trim(ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(ADJSTMT_IND) else NULL end as ADJSTMT_IND_CLEAN
                from
                    dx_IP
                )
            """
        runner.append("IP", z)

        z = f"""
            create or replace temporary view IP_DX_classic as
                select *
                from IP_DX
                where TAF_Classic_ind = 1
        """
        runner.append("IP", z)

        z = f"""
            create or replace temporary view IP_DX_denied as
                select *
                from IP_DX
                where TAF_Classic_ind = 0
        """
        runner.append("IP", z)

    def build(self, runner: IP_Runner):
        """
        Build the IP claim-DX level segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        input_table = {
            False:"IP_DX_classic",
            True:"IP_DX_Denied"
        }
        output_table = {
            False: "taf_ip_dx",
            True:  "taf_ip_dx_d"}

        for denied_flag in [False,True]:
            z = f"""
                    INSERT INTO {runner.DA_SCHEMA}.{output_table[denied_flag]}
                    SELECT
                        { IP_Metadata.finalFormatter(IP_Metadata.dx_columns) }
                    FROM {input_table[denied_flag]}
            """

            runner.append(type(self).__name__, z)