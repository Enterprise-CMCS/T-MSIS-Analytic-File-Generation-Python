from taf.RX.RX_Metadata import RX_Metadata
from taf.RX.RX_Runner import RX_Runner
from taf.TAF import TAF


class RX(TAF):

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def __init__(self, runner: RX_Runner):
        super().__init__(runner)
        self.st_fil_type = 'RX'

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
                { RX_Metadata.selectDataElements(tab_no, 'a') }

            from
                {TMSIS_SCHEMA}.{_2x_segment} A

            where
                a.TMSIS_ACTV_IND = 1
                and concat(a.submtg_state_cd,a.tmsis_run_id) in ({self.runner.get_combined_list()})
        """
        self.runner.append(self.st_fil_type, z)

        z = f"""
            create or replace temporary view {fl2}_LINE as
            select
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
                ) as RN
                ,a.*
                ,CASE
                    WHEN A.SUBMTG_STATE_CD = '97' THEN '42'
                    WHEN A.SUBMTG_STATE_CD = '96' THEN '19'
                    WHEN A.SUBMTG_STATE_CD = '94' THEN '30'
                    WHEN A.SUBMTG_STATE_CD = '93' THEN '56'
                    ELSE A.SUBMTG_STATE_CD
                    END AS NEW_SUBMTG_STATE_CD_LINE
                ,CASE
                    WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
                    ELSE SUBSTRING(A.DRUG_UTLZTN_CD,1,2)
                    END AS RSN_SRVC_CD
                ,CASE
                    WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
                    ELSE SUBSTRING(A.DRUG_UTLZTN_CD,3,2)
                    END AS PROF_SRVC_CD
                ,CASE
                    WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
                    ELSE SUBSTRING(A.DRUG_UTLZTN_CD,5,2)
                    END AS RSLT_SRVC_CD

            from
                {fl2}_LINE_IN as A

            inner join FA_HDR_{fl} HEADER

            on
                HEADER.TMSIS_RUN_ID = a.TMSIS_RUN_ID_LINE and
                HEADER.SUBMTG_STATE_CD = a.SUBMTG_STATE_CD and
                HEADER.ORGNL_CLM_NUM = a.ORGNL_CLM_NUM_LINE and
                HEADER.ADJSTMT_CLM_NUM = a.ADJSTMT_CLM_NUM_LINE and
                HEADER.ADJDCTN_DT = a.ADJDCTN_DT_LINE and
                HEADER.ADJSTMT_IND = a.LINE_ADJSTMT_IND
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
            create or replace temporary view {fl}_HEADER as
            select
                HEADER.*
                , coalesce(RN.NUM_CLL,0) as NUM_CLL

            from
                FA_HDR_{fl} HEADER left join RN_{fl2} RN

                on
                    HEADER.NEW_SUBMTG_STATE_CD = RN.NEW_SUBMTG_STATE_CD_LINE and
                    HEADER.ORGNL_CLM_NUM = RN.ORGNL_CLM_NUM_LINE and
                    HEADER.ADJSTMT_CLM_NUM = RN.ADJSTMT_CLM_NUM_LINE and
                    HEADER.ADJDCTN_DT = RN.ADJDCTN_DT_LINE and
                    HEADER.ADJSTMT_IND = RN.LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def build(self):

        # INSERT INTO {self.runner.DA_SCHEMA}.TAF_RXL
        z = """
                create or replace temporary view TAF_RXL as
                select
                      DA_RUN_ID
                    , RX_LINK_KEY
                    , RX_VRSN
                    , RX_FIL_DT
                    , TMSIS_RUN_ID
                    , MSIS_IDENT_NUM
                    , SUBMTG_STATE_CD
                    , ORGNL_CLM_NUM
                    , ADJSTMT_CLM_NUM
                    , ORGNL_LINE_NUM
                    , ADJSTMT_LINE_NUM
                    , ADJDCTN_DT
                    , LINE_ADJSTMT_IND
                    , TOS_CD
                    , NDC_CD
                    , UOM_CD
                    , SUPLY_DAYS_CNT
                    , NEW_REFL_IND
                    , BRND_GNRC_IND
                    , DSPNS_FEE_AMT
                    , DRUG_UTLZTN_CD
                    , DTL_MTRC_DCML_QTY
                    , CMPND_DSG_FORM_CD
                    , REBT_ELGBL_IND
                    , IMNZTN_TYPE_CD
                    , BNFT_TYPE_CD
                    , ALOWD_SRVC_QTY
                    , ACTL_SRVC_QTY
                    , CMS_64_FED_REIMBRSMT_CTGRY_CD
                    , XIX_SRVC_CTGRY_CD
                    , XXI_SRVC_CTGRY_CD
                    , CLL_STUS_CD
                    , BILL_AMT
                    , ALOWD_AMT
                    , COPAY_AMT
                    , TPL_AMT
                    , MDCD_PD_AMT
                    , MDCR_PD_AMT
                    , MDCD_FFS_EQUIV_AMT
                    , MDCR_COINSRNC_PD_AMT
                    , MDCR_DDCTBL_AMT
                    , OTHR_INSRNC_AMT
                    , RSN_SRVC_CD
                    , PROF_SRVC_CD
                    , RSLT_SRVC_CD
                    , LINE_NUM
                from
                    (SELECT * FROM RXL)
        """
        self.runner.append(self.st_fil_type, z)
