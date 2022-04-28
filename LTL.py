# --------------------------------------------------------------------------------- }
#
#
#
#
# --------------------------------------------------------------------------------- }
from taf.LT.LT_Runner import LT_Runner
from taf.TAF_Closure import TAF_Closure
from taf.TAF_Metadata import TAF_Metadata


class LTL():

    # --------------------------------------------------------------------------------- }
    #
    #
    #
    #
    # --------------------------------------------------------------------------------- }
    def create(self, runner: LT_Runner):

        z = f"""
            create or replace temporary view LTL as

            select

                 {runner.DA_RUN_ID} as DA_RUN_ID,
                 {runner.get_link_key_line()} as LT_LINK_KEY,
                '{runner.version}' as LT_VRSN,
                '{runner.TAF_FILE_DATE}' as LT_FIL_DT

                , TMSIS_RUN_ID_LINE as TMSIS_RUN_ID
                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM_LINE', new='MSIS_IDENT_NUM') }
                , NEW_SUBMTG_STATE_CD_LINE as SUBMTG_STATE_CD

                , { TAF_Closure.var_set_type3('ORGNL_CLM_NUM_LINE', cond1='~', new='ORGNL_CLM_NUM') }
                , { TAF_Closure.var_set_type3('ADJSTMT_CLM_NUM_LINE', cond1='~', new='ADJSTMT_CLM_NUM') }
                , { TAF_Closure.var_set_type1('ADJSTMT_LINE_NUM') }
                , { TAF_Closure.var_set_type1('ORGNL_LINE_NUM') }

                , case when (ADJDCTN_DT_LINE < to_date('1600-01-01')) then to_date('1599-12-31') else nullif(ADJDCTN_DT_LINE, to_date('1960-01-01')) end as ADJDCTN_DT
                ,LINE_ADJSTMT_IND_CLEAN as LINE_ADJSTMT_IND

                , { TAF_Closure.var_set_tos('TOS_CD') }

                , case when lpad(IMNZTN_TYPE_CD,2,'0') = '88' then NULL
                    else  { TAF_Closure.var_set_type5('IMNZTN_TYPE_CD', lpad=2, lowerbound=0, upperbound='29', multiple_condition=True) }
                , { TAF_Closure.var_set_type2('CMS_64_FED_REIMBRSMT_CTGRY_CD', 2, cond1='01',  cond2='02', cond3='03', cond4='04') }

                , case when XIX_SRVC_CTGRY_CD in { tuple(TAF_Metadata.XIX_SRVC_CTGRY_CD_values) } then XIX_SRVC_CTGRY_CD else null end as XIX_SRVC_CTGRY_CD
                , case when XXI_SRVC_CTGRY_CD in { tuple(TAF_Metadata.XXI_SRVC_CTGRY_CD_values) } then XXI_SRVC_CTGRY_CD else null end as XXI_SRVC_CTGRY_CD

                , { TAF_Closure.var_set_type1('CLL_STUS_CD') }

                , case when SRVC_BGNNG_DT < to_date('1600-01-01') then to_date('1599-12-31') else nullif(SRVC_BGNNG_DT, to_date('1960-01-01')) end as SRVC_BGNNG_DT
                , case when SRVC_ENDG_DT < to_date('1600-01-01') then to_date('1599-12-31') else nullif(SRVC_ENDG_DT, to_date('1960-01-01')) end as SRVC_ENDG_DT

                , { TAF_Closure.var_set_type5('BNFT_TYPE_CD', lpad=3, lowerbound='001', upperbound='108') }

                , { TAF_Closure.var_set_type2('BLG_UNIT_CD', 2, cond1='01',  cond2='02', cond3='03', cond4='04', cond5='05', cond6='06', cond7='07') }

                , { TAF_Closure.var_set_type6('IP_LT_ACTL_SRVC_QTY', new='ACTL_SRVC_QTY', cond1='999999', cond2='88888.888', cond3='99999.990') }
                , { TAF_Closure.var_set_type6('IP_LT_ALOWD_SRVC_QTY', new='ALOWD_SRVC_QTY', cond1='888888.89', cond2='88888.888') }
                , { TAF_Closure.var_set_type1('REV_CD', lpad=4) }
                , { TAF_Closure.var_set_type6('REV_CHRG_AMT', cond1='88888888888.88', cond2='99999999.9', cond3='888888888.88', cond4='8888888888.88', cond5='88888888.88') }
                , { TAF_Closure.var_set_type1('SRVCNG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('SRVCNG_PRVDR_NPI_NUM', new='SRVCNG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_taxo('SRVCNG_PRVDR_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_prtype('SRVCNG_PRVDR_TYPE_CD') }
                , { TAF_Closure.var_set_spclty('SRVCNG_PRVDR_SPCLTY_CD') }

                , case when PRVDR_FAC_TYPE_CD in ('100000000', '170000000', '250000000', '260000000', '270000000', '280000000', '290000000', '300000000', '310000000', '320000000', '330000000', '340000000', '380000000') then PRVDR_FAC_TYPE_CD
                    else NULL end as PRVDR_FAC_TYPE_CD

                , { TAF_Closure.var_set_type6('NDC_QTY', cond1='999999', cond2='888888', cond3='88888.888', cond4='888888.888', cond5='999999.998', cond6='888888.880') }
                , { TAF_Closure.var_set_type1('HCPCS_RATE') }
                , { TAF_Closure.var_set_fills('NDC_CD', cond1='0', cond2='8', cond3='9', cond4='#', spaces=True) }
                , { TAF_Closure.var_set_type4('UOM_CD', True, cond1='F2', cond2='ML', cond3='GR', cond4='UN', cond5='ME') }
                , { TAF_Closure.var_set_type6('ALOWD_AMT', cond1='888888888.88', cond2='99999999.00', cond3='9999999999.99') }
                , { TAF_Closure.var_set_type6('MDCD_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('OTHR_INSRNC_AMT', cond1='888888888.88', cond2='88888888888.00', cond3='88888888888.88') }
                , { TAF_Closure.var_set_type6('MDCD_FFS_EQUIV_AMT', cond1='888888888.88', cond2='88888888888.80', cond3='999999.99') }
                , { TAF_Closure.var_set_type6('TPL_AMT', cond1='888888888.88', cond2='88888888888.80', cond3='999999.99') }

                ,RN as LINE_NUM

            FROM (
                select
                    *,
                    case when LINE_ADJSTMT_IND is NOT NULL and
                    trim(LINE_ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(LINE_ADJSTMT_IND) else NULL end as LINE_ADJSTMT_IND_CLEAN
                from
                    LT_LINE
                ) H
            """

        runner.append(type(self).__name__, z)
