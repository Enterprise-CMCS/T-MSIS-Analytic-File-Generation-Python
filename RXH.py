# --------------------------------------------------------------------------------- }
#
#
#
#
# --------------------------------------------------------------------------------- }
from taf.RX.RX_Runner import RX_Runner
from taf.TAF_Closure import TAF_Closure


class RXH():

    # --------------------------------------------------------------------------------- }
    #
    #
    #
    #
    # --------------------------------------------------------------------------------- }
    def create(self, runner: RX_Runner):

        z = f"""
            create or replace temporary view RXH as

            select

                 {runner.DA_RUN_ID} as DA_RUN_ID,
                 {runner.get_link_key()} as RX_LINK_KEY,
                '{runner.version}' as RX_VRSN,
                '{runner.TAF_FILE_DATE}' as RX_FIL_DT

                , tmsis_run_id
                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM') }
                , new_submtg_state_cd as submtg_state_cd

                , { TAF_Closure.var_set_type3('orgnl_clm_num', cond1='~') }
                , { TAF_Closure.var_set_type3('adjstmt_clm_num', cond1='~') }
                , ADJSTMT_IND_CLEAN as ADJSTMT_IND
                , { TAF_Closure.var_set_rsn('ADJSTMT_RSN_CD') }

                , case when (ADJDCTN_DT < to_date('1600-01-01')) then to_date('1599-12-31') else nullif(ADJDCTN_DT, to_date('1960-01-01')) end as ADJDCTN_DT
                , { TAF_Closure.fix_old_dates('mdcd_pd_dt') }

                , rx_fill_dt

                , { TAF_Closure.fix_old_dates('prscrbd_dt') }
                , { TAF_Closure.var_set_type2('CMPND_DRUG_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2('SECT_1115A_DEMO_IND', 0, cond1='0', cond2='1') }

                , case when upper(clm_type_cd) in ('1', '2', '3', '4', '5', 'A', 'B', 'C', 'D', 'E', 'U', 'V', 'W', 'X', 'Y', 'Z') then upper(clm_type_cd)
                    else NULL
                    end as clm_type_cd

                , case when lpad(pgm_type_cd, 2, '0') in ('06', '09') then NULL
                    else { TAF_Closure.var_set_type5('pgm_type_cd', lpad=2, lowerbound=0, upperbound=17, multiple_condition=True) }

                , { TAF_Closure.var_set_type1('MC_PLAN_ID') }
                , { TAF_Closure.var_set_type1('ELGBL_LAST_NAME', upper=True) }
                , { TAF_Closure.var_set_type1('ELGBL_1ST_NAME', upper=True) }
                , { TAF_Closure.var_set_type1('ELGBL_MDL_INITL_NAME', upper=True) }
                , { TAF_Closure.fix_old_dates('BIRTH_DT') }
                , { TAF_Closure.var_set_type5('wvr_type_cd', lpad=2, lowerbound=1, upperbound=33) }
                , { TAF_Closure.var_set_type1('WVR_ID') }
                , { TAF_Closure.var_set_type2('srvc_trkng_type_cd', 2, cond1='00', cond2='01', cond3='02', cond4='03', cond5='04', cond6='05', cond7='06') }
                , { TAF_Closure.var_set_type6('SRVC_TRKNG_PYMT_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type2('OTHR_INSRNC_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2('othr_tpl_clctn_cd', 3, cond1='000', cond2='001', cond3='002', cond4='003', cond5='004', cond6='005', cond7='006', cond8='007') }
                , { TAF_Closure.var_set_type2('FIXD_PYMT_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type4('FUNDNG_CD', True, cond1='A', cond2='B', cond3='C', cond4='D', cond5='E', cond6='F', cond7='G', cond8='H', cond9='I') }
                , { TAF_Closure.var_set_type2('fundng_src_non_fed_shr_cd', 2, cond1='01', cond2='02', cond3='03', cond4='04', cond5='05', cond6='06') }
                , { TAF_Closure.var_set_type2('BRDR_STATE_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2('XOVR_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type1('MDCR_HICN_NUM') }
                , { TAF_Closure.var_set_type1('MDCR_BENE_ID') }
                , { TAF_Closure.var_set_type1('BLG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('BLG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_taxo('BLG_PRVDR_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_spclty('BLG_PRVDR_SPCLTY_CD') }
                , { TAF_Closure.var_set_type1('PRSCRBNG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('SRVCNG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_type1('DSPNSNG_PD_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_type1('DSPNSNG_PD_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('PRVDR_LCTN_ID') }
                , { TAF_Closure.var_set_type2('PYMT_LVL_IND', 0, cond1='1', cond2='2') }
                , { TAF_Closure.var_set_type6('tot_bill_amt', cond1='999999.99', cond2='69999999999.93', cond3='999999.00', cond4='888888888.88', cond5='9999999.99', cond6='99999999.90') }
                , { TAF_Closure.var_set_type6('tot_alowd_amt', cond1='888888888.88', cond2='99999999.00') }
                , { TAF_Closure.var_set_type6('tot_mdcd_pd_amt', cond1='999999.99', cond2='888888888.88') }
                , { TAF_Closure.var_set_type6('tot_copay_amt', cond1='88888888888.00', cond2='888888888.88', cond3='9999999.99') }
                , { TAF_Closure.var_set_type6('tot_tpl_amt', cond1='999999.99', cond2='888888888.88') }
                , { TAF_Closure.var_set_type6('tot_othr_insrnc_amt', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('tot_mdcr_ddctbl_amt', cond1='99999', cond2='88888888888.00', cond3='888888888.88') }
                , { TAF_Closure.var_set_type6('tot_mdcr_coinsrnc_amt', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TP_COINSRNC_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TP_COPMT_PD_AMT', cond1='99999999999.00', cond2='888888888.88', cond3='888888888.00', cond4='88888888888.00') }
                , { TAF_Closure.var_set_type6('bene_coinsrnc_amt', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('bene_copmt_amt', cond1='88888888888.00', cond2='888888888.88', cond3='888888888.00') }
                , { TAF_Closure.var_set_type6('bene_ddctbl_amt', cond1='88888888888.00', cond2='888888888.88', cond3='888888888.00') }
                , { TAF_Closure.var_set_type2('COPAY_WVD_IND', 0, cond1='0', cond2='1') }
                , cll_cnt
                , num_cll

            from (
                select
                    *,
                    case when ADJSTMT_IND is NOT NULL and
                    trim(ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(ADJSTMT_IND) else NULL end as ADJSTMT_IND_CLEAN
                from
                    RX_HEADER
                ) H
            """

        runner.append(type(self).__name__, z)
