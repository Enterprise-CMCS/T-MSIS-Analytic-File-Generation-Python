from taf.LT.LT_Metadata import LT_Metadata
from taf.LT.LT_Runner import LT_Runner
from taf.TAF import TAF


class LT(TAF):

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def __init__(self, runner: LT_Runner):
        super().__init__(runner)
        self.st_fil_type = 'LT'

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def AWS_Extract_Line(self, TMSIS_SCHEMA, fl2, fl, tab_no, _2x_segment):

        # Subset line file and attach row numbers to all records belonging to an ICN set.  Fix PA & IA
        z = f"""
            create or replace temporary view {fl2}_LINE_IN as
            select
                SUBMTG_STATE_CD,
                { LT_Metadata.selectDataElements(tab_no, 'a') }

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

                CASE
                    WHEN A.SUBMTG_STATE_CD = '97' THEN '42'
                    WHEN A.SUBMTG_STATE_CD = '96' THEN '19'
                    WHEN A.SUBMTG_STATE_CD = '94' THEN '30'
                    WHEN A.SUBMTG_STATE_CD = '93' THEN '56'
                    ELSE A.SUBMTG_STATE_CD
                    END AS NEW_SUBMTG_STATE_CD_LINE

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

        # Pull out maximum row_number for each partition and compute calculated variables here
        # Revisit coding for accommodation_paid, ancillary_paid, cvr_mh_days_over_65, cvr_mh_days_under_21 here
        z = f"""
            create or replace temporary view constructed_{fl2} as

            select
                  NEW_SUBMTG_STATE_CD_LINE
                , ORGNL_CLM_NUM_LINE
                , ADJSTMT_CLM_NUM_LINE
                , ADJDCTN_DT_LINE
                , LINE_ADJSTMT_IND
                , max(RN) as NUM_CLL
                , sum (case when substring(lpad(REV_CD,4,'0'),1,2)='01' or substring(lpad(REV_CD,4,'0'),1,3) in ('020', '021')
                        then MDCD_PD_AMT
                        when rev_cd is not null and mdcd_pd_amt is not null then 0
                        end
                    ) as ACCOMMODATION_PAID
                , sum (case when substring(lpad(REV_CD,4,'0'),1,2) <> '01' and
                            substring(lpad(REV_CD,4,'0'),1,3) not in ('020', '021')
                        then MDCD_PD_AMT
                        when REV_CD is not null and MDCD_PD_AMT is not null then 0
                        end
                    ) as ANCILLARY_PAID
                , max (case when lpad(TOS_CD,3,'0')in ('044','045') then 1
                    when TOS_CD is null then null
                    else 0
                    end
                    ) as MH_DAYS_OVER_65
                , max(case when lpad(TOS_CD,3,'0') = '048' then 1
                        when TOS_CD is null then null
                    else 0
                    end
                    ) as MH_DAYS_UNDER_21

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
        # Use this step to add in constructed variables for accomodation and ancillary paid amounts
        # Will probably need to move this step lower
        z = f"""
            create or replace temporary view {fl}_HEADER as
            select
                HEADER.*
                , coalesce(CONSTR.NUM_CLL,0) as NUM_CLL
                , CONSTR.ACCOMMODATION_PAID
                , CONSTR.ANCILLARY_PAID
                , case when CONSTR.MH_DAYS_OVER_65 = 1 then HEADER.MDCD_CVRD_IP_DAYS_CNT
                    when CONSTR.MH_DAYS_OVER_65 = 0 and  HEADER.MDCD_CVRD_IP_DAYS_CNT is not null then 0
                    end as CVRD_MH_DAYS_OVER_65
                , case when CONSTR.MH_DAYS_UNDER_21 = 1 then HEADER.MDCD_CVRD_IP_DAYS_CNT
                    when CONSTR.MH_DAYS_UNDER_21 = 0 and  HEADER.MDCD_CVRD_IP_DAYS_CNT is not null then 0
                    end as CVRD_MH_DAYS_UNDER_21

            from
                FA_HDR_{fl} HEADER left join constructed_{fl2} CONSTR

              on
                HEADER.NEW_SUBMTG_STATE_CD = CONSTR.NEW_SUBMTG_STATE_CD_LINE and
                HEADER.ORGNL_CLM_NUM = CONSTR.ORGNL_CLM_NUM_LINE and
                HEADER.ADJSTMT_CLM_NUM = CONSTR.ADJSTMT_CLM_NUM_LINE and
                HEADER.ADJDCTN_DT = CONSTR.ADJDCTN_DT_LINE and
                HEADER.ADJSTMT_IND = CONSTR.LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def build(self):

        # INSERT INTO {self.runner.DA_SCHEMA}.TAF_LTL
        z = """
                create or replace temporary view TAF_LTL as
                select
                      DA_RUN_ID
                    , LT_LINK_KEY
                    , LT_VRSN
                    , LT_FIL_DT
                    , TMSIS_RUN_ID
                    , MSIS_IDENT_NUM
                    , SUBMTG_STATE_CD
                    , ORGNL_CLM_NUM
                    , ADJSTMT_CLM_NUM
                    , ADJSTMT_LINE_NUM
                    , ORGNL_LINE_NUM
                    , ADJDCTN_DT
                    , LINE_ADJSTMT_IND
                    , TOS_CD
                    , IMNZTN_TYPE_CD
                    , CMS_64_FED_REIMBRSMT_CTGRY_CD
                    , XIX_SRVC_CTGRY_CD
                    , XXI_SRVC_CTGRY_CD
                    , CLL_STUS_CD
                    , SRVC_BGNNG_DT
                    , SRVC_ENDG_DT
                    , BNFT_TYPE_CD
                    , BLG_UNIT_CD
                    , NDC_CD
                    , UOM_CD
                    , NDC_QTY
                    , HCPCS_RATE
                    , SRVCNG_PRVDR_NUM
                    , SRVCNG_PRVDR_NPI_NUM
                    , SRVCNG_PRVDR_TXNMY_CD
                    , SRVCNG_PRVDR_TYPE_CD
                    , SRVCNG_PRVDR_SPCLTY_CD
                    , PRVDR_FAC_TYPE_CD
                    , ACTL_SRVC_QTY
                    , ALOWD_SRVC_QTY
                    , REV_CD
                    , REV_CHRG_AMT
                    , ALOWD_AMT
                    , MDCD_PD_AMT
                    , OTHR_INSRNC_AMT
                    , MDCD_FFS_EQUIV_AMT
                    , TPL_AMT
                    , LINE_NUM
                from
                    (SELECT * FROM LTL)
        """
        self.runner.append(self.st_fil_type, z)
