# --------------------------------------------------------------------------------- }
#
#
#
#
# --------------------------------------------------------------------------------- }
from taf.OT.OT_Runner import OT_Runner
from taf.OT.OT_Metadata import OT_Metadata
from taf.TAF_Closure import TAF_Closure
from taf.TAF_Metadata import TAF_Metadata


class OTL():

    # --------------------------------------------------------------------------------- }
    #
    #
    #
    #
    # --------------------------------------------------------------------------------- }
    def create(self, runner: OT_Runner):

        z = f"""
            create or replace temporary view OTL as

            select

                 {runner.DA_RUN_ID} as DA_RUN_ID,
                 {runner.get_link_key_line()} as OT_LINK_KEY,
                '{runner.version}' as OT_VRSN,
                '{runner.TAF_FILE_DATE}' as OT_FIL_DT

                , TMSIS_RUN_ID_LINE as TMSIS_RUN_ID
                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM_LINE', new='MSIS_IDENT_NUM') }
                , NEW_SUBMTG_STATE_CD_LINE as SUBMTG_STATE_CD

                , { TAF_Closure.var_set_type3('ORGNL_CLM_NUM_LINE', cond1='~', new='ORGNL_CLM_NUM') }
                , { TAF_Closure.var_set_type3('ADJSTMT_CLM_NUM_LINE', cond1='~', new='ADJSTMT_CLM_NUM') }
                , { TAF_Closure.var_set_type1('ORGNL_LINE_NUM') }
                , { TAF_Closure.var_set_type1('ADJSTMT_LINE_NUM') }
                , case when ADJDCTN_DT_LINE < to_date('1600-01-01') then to_date('1599-12-31') else nullif(ADJDCTN_DT_LINE, to_date('1960-01-01')) end as ADJDCTN_DT
                , LINE_ADJSTMT_IND_CLEAN as LINE_ADJSTMT_IND
                , { TAF_Closure.var_set_rsn('ADJSTMT_LINE_RSN_CD') }
                , { TAF_Closure.var_set_type1('CLL_STUS_CD') }
                , case when SRVC_BGNNG_DT_LINE < to_date('1600-01-01') then to_date('1599-12-31') else nullif(SRVC_BGNNG_DT_LINE, to_date('1960-01-01')) end as SRVC_BGNNG_DT
                , case when SRVC_ENDG_DT_LINE < to_date('1600-01-01') then to_date('1599-12-31') else nullif(SRVC_ENDG_DT_LINE, to_date('1960-01-01')) end as SRVC_ENDG_DT
                , { TAF_Closure.var_set_type1('REV_CD', lpad=4) }
                , { TAF_Closure.var_set_fillpr('PRCDR_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.fix_old_dates('PRCDR_CD_DT') }
                , { TAF_Closure.var_set_proc('PRCDR_CD_IND') }
                , { TAF_Closure.var_set_type1('PRCDR_1_MDFR_CD', lpad=2) }
                , case when lpad(IMNZTN_TYPE_CD, 2, '0') = '88' then NULL
                    else { TAF_Closure.var_set_type5('IMNZTN_type_cd', lpad=2, lowerbound=0, upperbound=29, multiple_condition='YES') }
                , { TAF_Closure.var_set_type6('BILL_AMT', cond1='888888888.88', cond2='9999999999.99', cond3='999999.99', cond4='999999') }
                , { TAF_Closure.var_set_type6('ALOWD_AMT', cond1='99999999.00', cond2='888888888.88', cond3='9999999999.99') }
                , { TAF_Closure.var_set_type6('COPAY_AMT', cond1='88888888888.00', cond2='888888888.88') }
                , { TAF_Closure.var_set_type6('TPL_AMT', cond1='88888888.88') }
                , { TAF_Closure.var_set_type6('MDCD_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('MDCD_FFS_EQUIV_AMT', cond1='88888888888.80', cond2='888888888.88', cond3='999999.99') }
                , { TAF_Closure.var_set_type6('MDCR_PD_AMT', cond1='888888888.88', cond2='8888888.88', cond3='88888888888.00', cond4='99999999999.00', cond5='88888888888.88', cond6='9999999999.99') }
                , { TAF_Closure.var_set_type6('OTHR_INSRNC_AMT',   cond1='8888888888.00', cond2='888888888.88', cond3='88888888888.88') }
                , { TAF_Closure.var_set_type6('OTHR_TOC_RX_CLM_ACTL_QTY', new='ACTL_SRVC_QTY', cond1='999999.000', cond2='888888.000', cond3='999999.99') }
                , { TAF_Closure.var_set_type6('OTHR_TOC_RX_CLM_ALOWD_QTY', new='ALOWD_SRVC_QTY', cond1='999999.000', cond2='888888.000', cond3='888888.880', cond4='99999.999', cond5='99999') }
                , { TAF_Closure.var_set_tos('TOS_CD') }
                , { TAF_Closure.var_set_type5('BNFT_TYPE_CD', lpad=3, lowerbound='001', upperbound='108') }
                , { TAF_Closure.var_set_type2('HCBS_SRVC_CD', 0, cond1=1, cond2=2, cond3=3, cond4=4, cond5=5, cond6=6, cond7=7) }
                , { TAF_Closure.var_set_type1('HCBS_TXNMY', lpad=5) }
                , { TAF_Closure.var_set_type1('SRVCNG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('PRSCRBNG_PRVDR_NPI_NUM', new='SRVCNG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_taxo('SRVCNG_PRVDR_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_prtype('SRVCNG_PRVDR_TYPE_CD') }
                , { TAF_Closure.var_set_spclty('SRVCNG_PRVDR_SPCLTY_CD') }
                , { TAF_Closure.var_set_type4('TOOTH_DSGNTN_SYS_CD', 'YES', cond1='JO', cond2='JP') }
                , { TAF_Closure.var_set_type1('TOOTH_NUM') }
                , case when lpad(TOOTH_ORAL_CVTY_AREA_DSGNTD_CD, 2, '0') in ('20', '30', '40') then lpad(TOOTH_ORAL_CVTY_AREA_DSGNTD_CD, 2, '0')
                    else { TAF_Closure.var_set_type5('TOOTH_ORAL_CVTY_AREA_DSGNTD_CD', lpad=2, lowerbound=0, upperbound=10, multiple_condition='YES') }
                , { TAF_Closure.var_set_type4('TOOTH_SRFC_CD', 'YES', cond1='B', cond2='D', cond3='F', cond4='I', cond5='L', cond6='M', cond7='O') }
                , { TAF_Closure.var_set_type2('CMS_64_FED_REIMBRSMT_CTGRY_CD', 2, cond1='01', cond2='02', cond3='03', cond4='04') }
                , case when XIX_SRVC_CTGRY_CD in { tuple(TAF_Metadata.XIX_SRVC_CTGRY_CD_values) } then XIX_SRVC_CTGRY_CD
                else null end as XIX_SRVC_CTGRY_CD
                , case when XXI_SRVC_CTGRY_CD in { tuple(TAF_Metadata.XXI_SRVC_CTGRY_CD_values) } then XXI_SRVC_CTGRY_CD
                    else null end as XXI_SRVC_CTGRY_CD
                , { TAF_Closure.var_set_type1('STATE_NOTN_TXT') }
                , { TAF_Closure.var_set_fills('NDC_CD', cond1='0', cond2='8', cond3='9', cond4='#', spaces=True) }
                , { TAF_Closure.var_set_type1('PRCDR_2_MDFR_CD', lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_3_MDFR_CD', lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_4_MDFR_CD', lpad=2) }
                , { TAF_Closure.var_set_type3('HCPCS_RATE', cond1='0.0000000000', cond2='0.000000000000', spaces=False) }
                , { TAF_Closure.var_set_type2('SELF_DRCTN_TYPE_CD', 3, cond1='000', cond2='001', cond3='002', cond4='003') }
                , { TAF_Closure.var_set_type1('PRE_AUTHRZTN_NUM') }
                , { TAF_Closure.var_set_type4('UOM_CD', 'YES', cond1='F2', cond2='ML', cond3='GR', cond4='UN', cond5='ME') }
                , { TAF_Closure.var_set_type6('NDC_QTY', cond1='999999', cond2='999999.998', cond3='888888.000', cond4='888888.880', cond5='88888.888', cond6='888888.888') }
                , RN as LINE_NUM

            from (
                select
                    *,
                    case when LINE_ADJSTMT_IND is NOT NULL and
                    trim(LINE_ADJSTMT_IND)  in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(LINE_ADJSTMT_IND) else NULL end as LINE_ADJSTMT_IND_CLEAN
                from
                    OTHR_TOC_LINE
                ) H
            """

        runner.append(type(self).__name__, z)

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def build(self, runner: OT_Runner):

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_otl
                SELECT
                    { OT_Metadata.finalFormatter(OT_Metadata.line_columns) }
                FROM
                    (SELECT * FROM OTL)
        """

        runner.append(type(self).__name__, z)