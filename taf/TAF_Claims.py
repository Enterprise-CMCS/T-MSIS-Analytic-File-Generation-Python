from taf.TAF_Runner import TAF_Runner


class TAF_Claims():

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, runner: TAF_Runner):

        self.runner = runner
        self.rep_yr = runner.reporting_period.year
        self.rep_mo = runner.reporting_period.month

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def analysis_date(self, fl, analysis_date):

        if fl == 'IP':
            return f",case when {analysis_date} is NULL then 1 else 0 end as NO_DISCH_DT"
        elif fl == 'OTHR_TOC':
            return f",case when {analysis_date} is NULL and a.SRVC_BGNNG_DT is null then 1 else 0 end as NO_SRVC_DT"
        else:
            return ''

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def where_analysis_date(self, fl, analysis_date, rep_yr, rep_mo):
        clause = ''
        if fl == 'OTHR_TOC':
            # analysis date year is ref year and
            # analysis date month is ref month
            # or missing both
            clause = f"""
                    (year (nvl({analysis_date},a.SRVC_BGNNG_DT)) = {rep_yr} and
                    date_part ('month', nvl({analysis_date},a.SRVC_BGNNG_DT)) = {rep_mo})
                    or ({analysis_date} is null AND a.SRVC_BGNNG_DT is null)
                """
        else:
            # analysis date year is ref year and
            # analysis date month is ref month
            clause = f"""
                    (year ({analysis_date}) = {rep_yr} and
                    date_part ('month', {analysis_date}) = {rep_mo})
                """
        if fl == 'IP':
            # for IP, include NULL discharge dates
            clause += f"""or {analysis_date} is NULL"""

        return clause.format()

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def where_state_level_filter(self, fl, alias):
        # if fl == 'OTHR_TOC':  # For OT it s state by state
        #     return f""" and {alias}.submtg_state_cd = '{self.submtg_state_cd}'
        #                 and {alias}.tmsis_run_id = {self.tmsis_run_id}
        #            """
        # else:  # For all other states we do all included states at once
        #     return f""" and concat({alias}.submtg_state_cd, {alias}.tmsis_run_id) in ({self.runner.get_combined_list()}) """
        return f""" and concat({alias}.submtg_state_cd, {alias}.tmsis_run_id) in ({self.runner.get_combined_list()}) """

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def select_date(self, fl):
        if fl == 'IP':
            return ",A.NO_DISCH_DT"
        elif fl == 'OTHR_TOC':
            return ",A.NO_SRVC_DT"
        else:
            return ""

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def where_date(self, fl):
        if fl == 'IP':
            return "A.NO_DISCH_DT = 0"
        elif fl == 'OTHR_TOC':
            return "A.NO_SRVC_DT = 0"

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def exclude_missing_end_date(self, fl):
        if fl == 'IP':
            return "coalesce(L.SRVC_ENDG_DT, L.SRVC_BGNNG_DT) is not NULL and H.NO_DISCH_DT = 1"
        elif fl == 'OTHR_TOC':
            return "L.SRVC_ENDG_DT is not null and H.NO_SRVC_DT = 1"

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def coalesce_dos(self, fl):
        if fl in ['IP', 'OTHR_TOC']:
            return """
                ,coalesce(A.SRVC_ENDG_DT_DRVD_H,H.SRVC_ENDG_DT_DRVD_L) as SRVC_ENDG_DT_DRVD
                ,coalesce(A.SRVC_ENDG_DT_CD_H,H.SRVC_ENDG_DT_CD_L) as SRVC_ENDG_DT_CD
            """
        else:
            return ""

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def rep_yr_mo(self, fl, rep_yr, rep_mo):
        if fl == 'IP':
            # max svc ending date year is ref year and
            # max svc ending date month is ref month
            return f"""(date_part ('year', coalesce(SRVC_ENDG_DT, SRVC_BGNNG_DT)) = {rep_yr} and
                        date_part ('month', coalesce(SRVC_ENDG_DT, SRVC_BGNNG_DT)) = {rep_mo})
                """
        elif fl == 'OTHR_TOC':
            # analysis date year is ref year and
            # analysis date month is ref month
            return f"""((year (SRVC_ENDG_DT) = {rep_yr} and
                         date_part ('month', SRVC_ENDG_DT) = {rep_mo}))
                """

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def use_header(self, fl):
        if fl in ['IP', 'OTHR_TOC']:
            return "COMBINED_HEADER"
        else:
            return f"HEADER2_{fl}"

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def selectDataElements(self, fl: str, segment_id: str, alias: str):
        if (fl.casefold() == 'ip'):
            from taf.IP.IP_Metadata import IP_Metadata
            return IP_Metadata.selectDataElements(segment_id, alias)
        elif (fl.casefold() == 'lt'):
            from taf.LT.LT_Metadata import LT_Metadata
            return LT_Metadata.selectDataElements(segment_id, alias)
        elif (fl.casefold() == 'othr_toc'):
            from taf.OT.OT_Metadata import OT_Metadata
            return OT_Metadata.selectDataElements(segment_id, alias)
        elif (fl.casefold() == 'rx'):
            from taf.RX.RX_Metadata import RX_Metadata
            return RX_Metadata.selectDataElements(segment_id, alias)

    # ---------------------------------------------------------------------------------
    #
    #  Subset Header File applying inclusion and exclusion rules
    #
    #  For IP use, create a flag indicating presence/absence of discharge date in claim
    #  For OT use, create a flag indicating absence of both beg/end service date in claim
    #
    # ---------------------------------------------------------------------------------
    def AWS_Claims_Family_Table_Link(self, TMSIS_SCHEMA, tab_no, _2x_segment, fl, analysis_date):

        # -----------------------------------------------------------------------------
        #
        #  HEADER_?
        #
        # -----------------------------------------------------------------------------
        z = f"""
            create or replace temporary view HEADER_{fl} as

            select
                { self.selectDataElements(fl, tab_no, 'a') }
                { self.analysis_date(fl, analysis_date) }

            from
                {TMSIS_SCHEMA}.{_2x_segment} A

            where
                ( {self.where_analysis_date(fl, analysis_date, self.rep_yr, self.rep_mo) } )
                and
                     A.TMSIS_ACTV_IND = 1 and
                    (upper(A.CLM_STUS_CTGRY_CD) <> 'F2' or A.CLM_STUS_CTGRY_CD is null) and
                    (upper(A.CLM_TYPE_CD) <> 'Z' or A.CLM_TYPE_CD is null) and
                    (A.CLM_DND_IND <> '0' or A.CLM_DND_IND is null) and
                    (A.CLM_STUS_CD NOT IN('26','87','026','087','542','585','654') or A.CLM_STUS_CD is null) and
                    (A.ADJSTMT_IND <> '1' or A.ADJSTMT_IND IS NULL)

                {self.where_state_level_filter(fl, 'a')}
        """
        self.runner.append(fl, z)
        # -----------------------------------------------------------------------------
        #
        #  HEADER2_?
        #
        # -----------------------------------------------------------------------------
        z = f"""
            create or replace temporary view HEADER2_{fl} as

            select
                A.TMSIS_RUN_ID
                ,A.ORGNL_CLM_NUM
                ,A.ADJSTMT_CLM_NUM
                ,A.SUBMTG_STATE_CD
                ,coalesce(A.ADJDCTN_DT, to_date('1960-01-01')) as ADJDCTN_DT
                ,A.ADJSTMT_IND
                {self.select_date(fl)}

            from
                HEADER_{fl} A

            group by
                A.TMSIS_RUN_ID,
                A.SUBMTG_STATE_CD,
                A.ORGNL_CLM_NUM,
                A.ADJSTMT_CLM_NUM,
                A.ADJDCTN_DT,
                A.ADJSTMT_IND

                {self.select_date(fl)}

            having
                count(A.TMSIS_RUN_ID) = 1
        """
        self.runner.append(fl, z)

        # ---------------------------------------------------------------------------------
        #
        #  create subset of records without discharge dates in prep for joining with line
        #  file to see which ones have a corresponding svc end date
        #  this is only done for IP
        #
        # ---------------------------------------------------------------------------------
        if fl in ['IP', 'OTHR_TOC']:

            # -----------------------------------------------------------------------------
            #
            #  NO_DISCHARGE_DATES
            #
            # -----------------------------------------------------------------------------
            # Exclude records with missing end date, and keep TMSIS active and where header is missing disch

            z = f"""
                create or replace temporary view NO_DISCHARGE_DATES as

                with MAX_DATES as
                    (select
                        H.TMSIS_RUN_ID
                        ,H.ORGNL_CLM_NUM
                        ,H.ADJSTMT_CLM_NUM
                        ,H.SUBMTG_STATE_CD
                        ,H.ADJDCTN_DT
                        ,H.ADJSTMT_IND
                        ,MAX(L.SRVC_ENDG_DT) as SRVC_ENDG_DT
                        ,MAX(L.SRVC_BGNNG_DT) as SRVC_BGNNG_DT

                    from
                        HEADER2_{fl} H

                    inner join
                        {TMSIS_SCHEMA}.TMSIS_CLL_REC_{fl} L

                        on  H.TMSIS_RUN_ID = L.TMSIS_RUN_ID and
                            H.SUBMTG_STATE_CD = L.SUBMTG_STATE_CD and
                            H.ORGNL_CLM_NUM = upper(coalesce(L.ORGNL_CLM_NUM,'~')) and
                            H.ADJSTMT_CLM_NUM = upper(coalesce(L.ADJSTMT_CLM_NUM,'~')) and
                            H.ADJDCTN_DT = coalesce(L.ADJDCTN_DT,'01JAN1960') and
                            H.ADJSTMT_IND = upper(coalesce(L.LINE_ADJSTMT_IND,'X'))

                        where
                            L.TMSIS_ACTV_IND = 1 and
                            {self.exclude_missing_end_date(fl)}
                            {self.where_state_level_filter(fl, 'L')}

                        group by
                            H.TMSIS_RUN_ID,
                            H.ORGNL_CLM_NUM,
                            H.ADJSTMT_CLM_NUM,
                            H.SUBMTG_STATE_CD,
                            H.ADJDCTN_DT,
                            H.ADJSTMT_IND
                    )
                select
                    *
                from
                    MAX_DATES
                where
                    {self.rep_yr_mo(fl, self.rep_yr, self.rep_mo)}
                """
            self.runner.append(fl, z)

        # ---------------------------------------------------------------------------------------------------------------------------------------
        #   create file containing primary keys to be used in joining with claims family table to pick up value of final action indicator.
        #   output only contains records where final action indicator=1.  For IP, source of keys is the file with discharge dates and file with
        #   discharge dates joined with line file to determine if there is a corresponding svc date
        # ---------------------------------------------------------------------------------------------------------------------------------------

        # -----------------------------------------------------------------------------
        #
        #  CLM_FMLY_?
        #
        # -----------------------------------------------------------------------------
        #  Limit to final action claims in family table

        if fl.casefold() == 'othr_toc':
            _fl = 'ot'
        else:
            _fl = fl

        z = f"""
                create or replace temporary view CLM_FMLY_{fl} as

                select
                    TMSIS_RUN_ID
                   ,coalesce(upper(ORGNL_CLM_NUM), '~') as ORGNL_CLM_NUM
                   ,coalesce(upper(ADJSTMT_CLM_NUM), '~') as ADJSTMT_CLM_NUM
                   ,SUBMTG_STATE_CD
                   ,coalesce(ADJDCTN_DT, to_date('1960-01-01')) as ADJDCTN_DT
                   ,COALESCE(ADJSTMT_IND,'X') as ADJSTMT_IND

                --- {TMSIS_SCHEMA}.TMSIS_CLM_FMLY_{fl} F
                --- subquery contains the claim family view definition

                from (

                    SELECT
                        submitting_state AS SUBMTG_STATE_CD,
                        icn_orig AS ORGNL_CLM_NUM,
                        icn_adj AS ADJSTMT_CLM_NUM,
                        adjudication_date AS ADJDCTN_DT,
                        tms_run_id AS TMSIS_RUN_ID,
                        claim_header_filename AS CLH_FIL_NAME,
                        claim_header_reporting_period AS CLH_RPTG_PRD,
                        claim_header_sequence_number AS CLH_SQNC_NUM,
                        claim_header_byte_offset AS CLH_OFST_BYTE_NUM,
                        claims_family_id AS CLM_FMLY_ID,
                        claims_family_sequence_number AS CLM_FMLY_SQNC_NUM,
                        claims_family_is_final_action AS CLM_FMLY_FINL_ACTN_IND,
                        claims_family_grouping_algorithm AS CLM_FMLY_GRPNG_DESC,
                        claims_family_grouping_algorithm_version AS CLM_FMLY_GRPNG_VRSN_NUM,
                        claims_family_grouping_error AS CLM_FMLY_GRPNG_ERR_DESC,
                        claims_family_sequencing_algorithm AS CLM_FMLY_SQNC_DESC,
                        claims_family_sequencing_algorithm_version AS CLM_FMLY_SQNC_VRSN_NUM,
                        claims_family_sequencing_error AS CLM_FMLY_SQNC_ERR_DESC,
                        claims_family_final_action_algorithm AS CLM_FMLY_FINL_ACTN_DESC,
                        claims_family_final_action_algorithm_version AS CLM_FMLY_FINL_ACTN_VRSN_NUM,
                        claims_family_final_action_error AS CLM_FMLY_FINL_ACTN_ERR_DESC,
                        adjustment_ind AS ADJSTMT_IND

                    FROM
                        {TMSIS_SCHEMA}.CLAIM_CLAIMS_FAMILY_META_{_fl}
                ) as F

                where
                    clm_fmly_finl_actn_ind  = 1
                    {self.where_state_level_filter(fl, 'F')}

                group by
                    1,2,3,4,5,6
                having
                    count(tmsis_run_id) = 1
        """

        self.runner.append(fl, z)

        # -----------------------------------------------------------------------------
        #
        #  COMBINED_HEADER
        #
        # -----------------------------------------------------------------------------
        if fl in ['IP', 'OTHR_TOC']:

            z = f"""
                    create or replace temporary view COMBINED_HEADER as
                    (select
                        A.TMSIS_RUN_ID,
                        A.ORGNL_CLM_NUM,
                        A.ADJSTMT_CLM_NUM,
                        A.SUBMTG_STATE_CD,
                        A.ADJDCTN_DT,
                        A.ADJSTMT_IND,

                        null as SRVC_ENDG_DT_DRVD_L,
                        null as SRVC_ENDG_DT_CD_L

                    from
                        HEADER2_{fl} A

                    where
                        {self.where_date(fl)}

                    union all

                    select
                        B.TMSIS_RUN_ID,
                        B.ORGNL_CLM_NUM,
                        B.ADJSTMT_CLM_NUM,
                        B.SUBMTG_STATE_CD,
                        B.ADJDCTN_DT,
                        B.ADJSTMT_IND,
                        case
                            when nullif(B.SRVC_ENDG_DT, '01JAN1960') is null
                            or SRVC_ENDG_DT is null then SRVC_BGNNG_DT
                            else SRVC_ENDG_DT
                        end as SRVC_ENDG_DT_DRVD_L,
                        case
                            when nullif(B.SRVC_ENDG_DT, '01JAN1960') is not null
                                and SRVC_ENDG_DT is not null then '4'
                            when nullif(B.SRVC_BGNNG_DT, '01JAN1960') is not null
                                and SRVC_BGNNG_DT is not null then '5'
                            else null
                        end as SRVC_ENDG_DT_CD_L
                    from
                        NO_DISCHARGE_DATES B )
            """
            self.runner.append(fl, z)

        # -----------------------------------------------------------------------------
        #
        #  ALL_HEADER_?
        #
        # -----------------------------------------------------------------------------

        # Join to limited and de-duped HEADER table to CLAIM FAMILY TABLE
        z = f"""
                create or replace temporary view ALL_HEADER_{fl} as

                select
                    H.*

                from
                    {self.use_header(fl)} H

                inner join
                    CLM_FMLY_{fl} F

                    on
                        H.ORGNL_CLM_NUM = F.ORGNL_CLM_NUM
                    and H.ADJSTMT_CLM_NUM = F.ADJSTMT_CLM_NUM
                    and H.ADJDCTN_DT =F.ADJDCTN_DT
                    and H.ADJSTMT_IND = F.ADJSTMT_IND
                    and H.SUBMTG_STATE_CD = F.SUBMTG_STATE_CD
                    and H.TMSIS_RUN_ID = F.TMSIS_RUN_ID
        """
        self.runner.append(fl, z)

        # ---------------------------------------------------------------------------------------------------------------------------------------
        #  merge back the remaining header finder file that has been linked with the claims family table with the main header file to pick up the
        #  remaining data elements from the header file. Output of this step shd contain header records to be kept in final file
        # ---------------------------------------------------------------------------------------------------------------------------------------

        # -----------------------------------------------------------------------------
        #
        #  FA_HDR_?
        #
        # -----------------------------------------------------------------------------
        # Join de-duped HEADER finder file to T-MSIS HEADER FILE
        z = f"""
            create or replace temporary view FA_HDR_{fl} as

            select
                A.*,
                h.submtg_state_cd as new_submtg_state_cd
                { self.coalesce_dos(fl) }
            from
                ALL_HEADER_{fl} H

            inner join
                HEADER_{fl} A

                on  H.SUBMTG_STATE_CD = A.SUBMTG_STATE_CD and
                    H.TMSIS_RUN_ID = A.TMSIS_RUN_ID and
                    H.ORGNL_CLM_NUM = A.ORGNL_CLM_NUM and
                    H.ADJSTMT_CLM_NUM = A.ADJSTMT_CLM_NUM and
                    H.ADJDCTN_DT = A.ADJDCTN_DT and
                    H.ADJSTMT_IND = A.ADJSTMT_IND
        """
        self.runner.append(fl, z)
