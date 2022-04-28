# --------------------------------------------------------------------------------- }
#
#
#
#
# --------------------------------------------------------------------------------- }
from taf.RX.RX_Runner import RX_Runner
from taf.TAF_Closure import TAF_Closure
from taf.TAF_Metadata import TAF_Metadata


class RXL():

    # --------------------------------------------------------------------------------- }
    #
    #
    #
    #
    # --------------------------------------------------------------------------------- }
    def create(self, runner: RX_Runner):

        z = f"""
            create or replace temporary view RXL as

            select

                 {runner.DA_RUN_ID} as DA_RUN_ID,
                 {runner.get_link_key_line()} as RX_LINK_KEY,
                '{runner.version}' as RX_VRSN,
                '{runner.TAF_FILE_DATE}' as RX_FIL_DT

                ,TMSIS_RUN_ID_LINE as TMSIS_RUN_ID

                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM_LINE',new='MSIS_IDENT_NUM') }
                ,NEW_SUBMTG_STATE_CD_LINE as SUBMTG_STATE_CD

                , { TAF_Closure.var_set_type3('ORGNL_CLM_NUM_LINE',cond1='~',new='ORGNL_CLM_NUM') }
                , { TAF_Closure.var_set_type3('ADJSTMT_CLM_NUM_LINE',cond1='~',new='ADJSTMT_CLM_NUM') }
                , { TAF_Closure.var_set_type1('ORGNL_LINE_NUM')  }
                , { TAF_Closure.var_set_type1('ADJSTMT_LINE_NUM') }

                ,case when ADJDCTN_DT_LINE < to_date('1600-01-01') then to_date('1599-12-31') else nullif(ADJDCTN_DT_LINE, to_date('01JAN1960')) end as ADJDCTN_DT
                ,LINE_ADJSTMT_IND_CLEAN as LINE_ADJSTMT_IND

                , { TAF_Closure.var_set_tos('TOS_CD') }
                , { TAF_Closure.var_set_fills('NDC_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type4('UOM_CD', True, cond1='F2',cond2='ML',cond3='GR',cond4='UN',cond5='ME',cond6='EA',cond7='GM') }
                , { TAF_Closure.var_set_type6('suply_days_cnt', cond1='8888', cond2='999', cond3='0') }
                , { TAF_Closure.var_set_type5('NEW_REFL_IND',lpad='2',lowerbound='0',upperbound='98') }
                , { TAF_Closure.var_set_type2('BRND_GNRC_IND', 0, cond1='0', cond2='1', cond3='2', cond4='3', cond5='4') }
                , { TAF_Closure.var_set_type6('dspns_fee_amt', cond1='88888.88')  }
                ,case when trim(DRUG_UTLZTN_CD) is not NULL then upper(DRUG_UTLZTN_CD)
                    else NULL
                    end as DRUG_UTLZTN_CD

                , { TAF_Closure.var_set_type6('dtl_mtrc_dcml_qty', cond1='999999.999') }

                ,case when lpad(CMPND_DSG_FORM_CD, 2, '0') in ('08','09') then NULL
                    else { TAF_Closure.var_set_type5('CMPND_DSG_FORM_CD', lpad=2, lowerbound=1, upperbound=18, multiple_condition=True) }

                , { TAF_Closure.var_set_type2('REBT_ELGBL_IND', 0, cond1='0', cond2='1', cond3='2') }
                , { TAF_Closure.var_set_type5('IMNZTN_TYPE_CD', lpad=2, lowerbound=0, upperbound=29) }
                , { TAF_Closure.var_set_type5('BNFT_TYPE_CD', lpad=3, lowerbound='001', upperbound='108') }
                , { TAF_Closure.var_set_type6('othr_toc_rx_clm_alowd_qty', new='alowd_srvc_qty', cond1='99999', cond2='99999.999', cond3='888888.000', cond4='999999', cond5='888888.880') }
                , { TAF_Closure.var_set_type6('othr_toc_rx_clm_actl_qty', new='actl_srvc_qty', cond1='999999.99', cond2='888888', cond3='999999', cond4='0') }
                , { TAF_Closure.var_set_type2('CMS_64_FED_REIMBRSMT_CTGRY_CD',2, cond1='01',cond2='02',cond3='03',cond4='04') }

                ,case when XIX_SRVC_CTGRY_CD in { tuple(TAF_Metadata.XIX_SRVC_CTGRY_CD_values) } then XIX_SRVC_CTGRY_CD
                    else null end as XIX_SRVC_CTGRY_CD
                ,case when XXI_SRVC_CTGRY_CD in { tuple(TAF_Metadata.XXI_SRVC_CTGRY_CD_values) } then XXI_SRVC_CTGRY_CD
                    else null end as XXI_SRVC_CTGRY_CD

                , { TAF_Closure.var_set_type1('CLL_STUS_CD') }
                , { TAF_Closure.var_set_type6('bill_amt', cond1='9999999999.99', cond2='999999.99', cond3='999999', cond4='888888888.88') }
                , { TAF_Closure.var_set_type6('alowd_amt', cond1='9999999999.99', cond2='888888888.88', cond3='99999999.00') }
                , { TAF_Closure.var_set_type6('copay_amt', cond1='888888888.88', cond2='88888888888.00') }
                , { TAF_Closure.var_set_type6('tpl_amt', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('mdcd_pd_amt', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('mdcr_pd_amt', cond1='88888888888.88', cond2='99999999999.00', cond3='888888888.88', cond4='88888888888.00', cond5='8888888.88', cond6='9999999999.99') }
                , { TAF_Closure.var_set_type6('mdcd_ffs_equiv_amt', cond1='999999.99', cond2='888888888.88', cond3='88888888888.80') }
                , { TAF_Closure.var_set_type6('mdcr_coinsrnc_pd_amt', cond1='88888888888.00', cond2='888888888.88') }
                , { TAF_Closure.var_set_type6('mdcr_ddctbl_amt', cond1='88888888888.00', cond2='888888888.88') }
                , { TAF_Closure.var_set_type6('othr_insrnc_amt', cond1='88888888888.00', cond2='88888888888.88', cond3='888888888.88') }
                , { TAF_Closure.var_set_type1('RSN_SRVC_CD',upper=True) }
                , { TAF_Closure.var_set_type1('PROF_SRVC_CD',upper=True) }
                , { TAF_Closure.var_set_type1('RSLT_SRVC_CD',upper=True) }
                ,RN as LINE_NUM

            from (
                select
                    *,
                    case when LINE_ADJSTMT_IND is NOT NULL and
                    trim(LINE_ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(LINE_ADJSTMT_IND) else NULL end as LINE_ADJSTMT_IND_CLEAN
                from
                    RX_LINE
                ) H
            """

        runner.append(type(self).__name__, z)
