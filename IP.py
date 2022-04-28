from taf.IP.IP_Metadata import IP_Metadata
from taf.IP.IP_Runner import IP_Runner
from taf.TAF import TAF


class IP(TAF):

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def __init__(self, runner: IP_Runner):
        super().__init__(runner)
        self.st_fil_type = 'IP'

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def AWS_Extract_Line(self, TMSIS_SCHEMA, fl2, fl, tab_no, _2x_segment):

        # Create a temporary line file
        z = f"""
            create or replace temporary view {fl2}_LINE_IN as
            select
                SUBMTG_STATE_CD,
                { IP_Metadata.selectDataElements(tab_no, 'a') }

            from
                {TMSIS_SCHEMA}.{_2x_segment} A

            where
                a.TMSIS_ACTV_IND = 1
                and concat(a.submtg_state_cd,a.tmsis_run_id) in ({self.runner.get_combined_list()})
        """
        self.runner.append(self.st_fil_type, z)

        # Subset line file and attach row numbers to all records belonging to an ICN set.  Fix PA & IA
        z = f"""
            create or replace temporary view {fl2}_LINE as
            select
                a.*,
                row_number() over (
                    partition by
                        a.SUBMTG_STATE_CD,
                        a.ORGNL_CLM_NUM_LINE,
                        a.ADJSTMT_CLM_NUM_LINE,
                        a.ADJDCTN_DT_LINE,
                        a.LINE_ADJSTMT_IND
                    order by
                        a.SUBMTG_STATE_CD,
                        a.ORGNL_CLM_NUM_LINE,
                        a.ADJSTMT_CLM_NUM_LINE,
                        a.ADJDCTN_DT_LINE,
                        a.LINE_ADJSTMT_IND,
                        a.TMSIS_FIL_NAME,
                        a.REC_NUM
                ) as RN,
                a.submtg_state_cd as new_submtg_state_cd_line

            from
                {fl2}_LINE_IN as A

            inner join FA_HDR_{fl} H

            on
                H.TMSIS_RUN_ID = a.TMSIS_RUN_ID_LINE and
                H.SUBMTG_STATE_CD = a.SUBMTG_STATE_CD and
                H.ORGNL_CLM_NUM = a.ORGNL_CLM_NUM_LINE and
                H.ADJSTMT_CLM_NUM = a.ADJSTMT_CLM_NUM_LINE and
                H.ADJDCTN_DT = a.ADJDCTN_DT_LINE and
                H.ADJSTMT_IND = a.LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)

        # Pull out maximum row_number for each partition
        z = f"""
            create or replace temporary view RN_{fl2} as
            select
                NEW_SUBMTG_STATE_CD_LINE
                , ORGNL_CLM_NUM_LINE
                , ADJSTMT_CLM_NUM_LINE
                , ADJDCTN_DT_LINE
                , LINE_ADJSTMT_IND
                , max(RN) as NUM_CLL

            from
                {fl2}_LINE

            group by
                NEW_SUBMTG_STATE_CD_LINE,
                ORGNL_CLM_NUM_LINE,
                ADJSTMT_CLM_NUM_LINE,
                ADJDCTN_DT_LINE,
                LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)

        # Attach num_cll variable to header records as per instruction
        z = f"""
            create or replace temporary view {fl2}_HEADER as
            select
                HEADER.*
                ,coalesce(RN.NUM_CLL,0) as NUM_CLL

            from
                FA_HDR_{fl2} HEADER left join RN_{fl2} RN

            on
                HEADER.NEW_SUBMTG_STATE_CD = RN.NEW_SUBMTG_STATE_CD_LINE and
                HEADER.ORGNL_CLM_NUM = RN.ORGNL_CLM_NUM_LINE and
                HEADER.ADJSTMT_CLM_NUM = RN.ADJSTMT_CLM_NUM_LINE and
                HEADER.ADJDCTN_DT = RN.ADJDCTN_DT_LINE and
                HEADER.ADJSTMT_IND = RN.LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)