from taf.OT.OT_Runner import OT_Runner
from taf.OT.OT_Metadata import OT_Metadata
from taf.TAF_Closure import TAF_Closure


class OT_DX:
    """
    Each OT TAF is comprised of two files – a claim-header level file and a claim-line level file and a diagnosis level file.
    The claims included in these files are active, final-action, non-voided, and non-denied claims.
    Only header claims with a date in the TAF month/year, along with their associated claim line and diagnosis
    records, are included. All files can be linked together using a unique key that is constructed
    based on various claim header, claim line, and claim DX data elements. The three OT TAF are produced for each
    calendar month in which the data are reported.
    """

    def create(self, runner: OT_Runner):
        """
        Create the OT claim-DX level segment.
        """
    
        z = f"""
        create or replace temporary view OT_DX as
            select 
            {runner.DA_RUN_ID} as DA_RUN_ID,
            {runner.get_link_key()} as OT_LINK_KEY,
            '{runner.VERSION}' as OT_VRSN,
            '{runner.TAF_FILE_DATE}' as OT_FIL_DT

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
            ,{ TAF_Closure.var_set_type4('DGNS_TYPE_CD', 'YES', cond1='A', cond2='D', cond3='E', cond4='O', cond5='P', cond6='R') }
            , DGNS_SQNC_NUM
            ,{ TAF_Closure.var_set_type2('DGNS_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
            ,DGNS_CD
            ,from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
            ,from_utc_timestamp(current_timestamp(), 'EST') as REC_UPDT_TS             --this must be equal to REC_ADD_TS for CCW pipeline
            from (
                select
                    *,
                    case when ADJSTMT_IND is NOT NULL and
                    trim(ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(ADJSTMT_IND) else NULL end as ADJSTMT_IND_CLEAN
                from
                    dx_OTHR_TOC
                )
            """
        runner.append("OTHR_TOC", z)
        
    def build(self, runner: OT_Runner):
        """
        Build the OT claim-DX level segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.TAF_OT_DX
                SELECT
                    { OT_Metadata.finalFormatter(OT_Metadata.dx_columns) }
                FROM OT_DX
        """

        runner.append(type(self).__name__, z)