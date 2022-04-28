# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
from taf.LT.LT_Runner import LT_Runner
from taf.TAF_Closure import TAF_Closure


class LTH():

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self, runner: LT_Runner):

        z = f"""
            create or replace temporary view LTH as

            select

                 {runner.DA_RUN_ID} as DA_RUN_ID,
                {runner.get_link_key()} as LT_LINK_KEY,
                '{runner.version}' as LT_VRSN,
                '{runner.TAF_FILE_DATE}' as LT_FIL_DT

                , TMSIS_RUN_ID
                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM') }
                , NEW_SUBMTG_STATE_CD as SUBMTG_STATE_CD

                , { TAF_Closure.var_set_type3(var='ORGNL_CLM_NUM', cond1='~') }
                , { TAF_Closure.var_set_type3(var='ADJSTMT_CLM_NUM', cond1='~') }
                , ADJSTMT_IND_CLEAN as ADJSTMT_IND
                , { TAF_Closure.var_set_rsn('ADJSTMT_RSN_CD') }
                , case when SRVC_BGNNG_DT < '1600-01-01' then '1599-12-31' else nullif('SRVC_BGNNG_DT', to_date('1960-01-01')) end as SRVC_BGNNG_DT
                , nullif('SRVC_ENDG_DT', to_date('1960-01-01')) as SRVC_ENDG_DT
                , { TAF_Closure.fix_old_dates('ADMSN_DT') }
                , { TAF_Closure.var_set_type5(var='ADMSN_HR_NUM', lpad=2, lowerbound=0, upperbound=23) }
                , { TAF_Closure.fix_old_dates('DSCHRG_DT') }
                , { TAF_Closure.var_set_type5(var='DSCHRG_HR_NUM', lpad=2, lowerbound=0, upperbound=23) }
                , case when ADJDCTN_DT < '1600-01-01' then '1599-12-31' else nullif('ADJDCTN_DT', to_date('1960-01-01')) end as ADJDCTN_DT
                , { TAF_Closure.fix_old_dates('MDCD_PD_DT') }
                , { TAF_Closure.var_set_type2(var='SECT_1115A_DEMO_IND', lpad=0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type1(var='BILL_TYPE_CD') }
                , case
                   when upper(CLM_TYPE_CD) in ('1', '2', '3', '4', '5', 'A', 'B', 'C', 'D', 'E', 'U', 'V', 'W', 'X', 'Y', 'Z') then upper(CLM_TYPE_CD)
                   else NULL
                   end as CLM_TYPE_CD
                , case when lpad(pgm_type_cd, 2, '0') in ('06', '09') then NULL
                   else { TAF_Closure.var_set_type5(var='pgm_type_cd', lpad=2, lowerbound=0, upperbound=17, multiple_condition='YES') }
                , { TAF_Closure.var_set_type1(var='MC_PLAN_ID') }
                , { TAF_Closure.var_set_type1(var='ELGBL_LAST_NAME', upper='YES') }
                , { TAF_Closure.var_set_type1(var='ELGBL_1ST_NAME', upper='YES') }
                , { TAF_Closure.var_set_type1(var='ELGBL_MDL_INITL_NAME', upper='YES') }
                , { TAF_Closure.fix_old_dates('BIRTH_DT') }
                , case when lpad(wvr_type_cd, 2, '0') = '88' then NULL
                   else { TAF_Closure.var_set_type5(var='wvr_type_cd', lpad=2, lowerbound='1', upperbound='33', multiple_condition='YES') }
                , { TAF_Closure.var_set_type1(var='WVR_ID') }
                , { TAF_Closure.var_set_type2(var='SRVC_TRKNG_TYPE_CD', lpad=2, cond1='00', cond2='01', cond3='02', cond4='03', cond5='04', cond6='05', cond7='06') }
                , { TAF_Closure.var_set_type6('SRVC_TRKNG_PYMT_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type2(var='OTHR_INSRNC_IND', lpad=0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2(var='OTHR_TPL_CLCTN_CD', lpad=3, cond1='000', cond2='001', cond3='002', cond4='003', cond5='004', cond6='005', cond7='006', cond8='007') }
                , { TAF_Closure.var_set_type2('FIXD_PYMT_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type4('FUNDNG_CD', 'YES', cond1='A', cond2='B', cond3='C', cond4='D', cond5='E', cond6='F', cond7='G', cond8='H', cond9='I') }
                , { TAF_Closure.var_set_type2('fundng_src_non_fed_shr_cd', 2, cond1='01', cond2='02', cond3='03', cond4='04', cond5='05', cond6='06') }
                , { TAF_Closure.var_set_type2(var='BRDR_STATE_IND', lpad=0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2(var='XOVR_IND', lpad=0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type1(var='MDCR_HICN_NUM') }
                , { TAF_Closure.var_set_type1(var='MDCR_BENE_ID') }
                , { TAF_Closure.var_set_type1(var='PTNT_CNTL_NUM') }
                , { TAF_Closure.var_set_type2(var='HLTH_CARE_ACQRD_COND_CD', lpad=0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_ptstatus('PTNT_STUS_CD') }
                , { TAF_Closure.var_set_fills('ADMTG_DGNS_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2(var='ADMTG_DGNS_CD_IND', lpad=0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_fills('DGNS_1_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2(var='DGNS_1_CD_IND', lpad=0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_1_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_2_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2(var='DGNS_2_CD_IND', lpad=0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_2_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_3_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2(var='DGNS_3_CD_IND', lpad=0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_3_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_4_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2(var='DGNS_4_CD_IND', lpad=0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_4_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_5_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2(var='DGNS_5_CD_IND', lpad=0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_5_CD_IND') }
                , { TAF_Closure.var_set_type6('NCVRD_DAYS_CNT', cond1='88888') }
                , { TAF_Closure.var_set_type6('NCVRD_CHRGS_AMT', cond1='888888888.88') }
                , MDCD_CVRD_IP_DAYS_CNT
                , { TAF_Closure.var_set_type6('ICF_IID_DAYS_CNT', cond1='8888') }
                , { TAF_Closure.var_set_type6('LVE_DAYS_CNT', cond1='88888') }
                , { TAF_Closure.var_set_type6('NRSNG_FAC_DAYS_CNT', cond1='88888') }
                , { TAF_Closure.var_set_type1(var='ADMTG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_type1(var='ADMTG_PRVDR_NUM') }
                , { TAF_Closure.var_set_spclty(var='ADMTG_PRVDR_SPCLTY_CD') }
                , { TAF_Closure.var_set_taxo('ADMTG_PRVDR_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_prtype(var='ADMTG_PRVDR_TYPE_CD') }
                , { TAF_Closure.var_set_type1(var='BLG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_type1(var='BLG_PRVDR_NUM') }
                , { TAF_Closure.var_set_taxo('BLG_PRVDR_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_prtype(var='BLG_PRVDR_TYPE_CD') }
                , { TAF_Closure.var_set_spclty(var='BLG_PRVDR_SPCLTY_CD') }
                , { TAF_Closure.var_set_type1(var='RFRG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1(var='RFRG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_prtype(var='RFRG_PRVDR_TYPE_CD') }
                , { TAF_Closure.var_set_spclty(var='RFRG_PRVDR_SPCLTY_CD') }
                , { TAF_Closure.var_set_type1(var='PRVDR_LCTN_ID') }
                , { TAF_Closure.var_set_type6('DAILY_RATE', cond1='88888.80', cond2='88888.00', cond3='88888.88') }
                , { TAF_Closure.var_set_type2(var='PYMT_LVL_IND', lpad=0, cond1='1', cond2='2') }
                , { TAF_Closure.var_set_type6('LTC_RCP_LBLTY_AMT', cond1='9999999999.99', cond2='888888888.88') }
                , { TAF_Closure.var_set_type6('MDCR_PD_AMT', cond1='9999999999.99', cond2='888888888.88', cond3='88888888888.00', cond4='88888888888.88', cond5='8888888.88', cond6='99999999999.00') }
                , { TAF_Closure.var_set_type6('TOT_BILL_AMT', cond1='9999999.99', cond2='888888888.88', cond3='99999999.90', cond4='999999.99', cond5='999999') }
                , { TAF_Closure.var_set_type6('TOT_ALOWD_AMT', cond1='888888888.88', cond2='99999999.00') }
                , { TAF_Closure.var_set_type6('TOT_MDCD_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TOT_MDCR_DDCTBL_AMT', cond1='888888888.88', cond2='99999', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('TOT_MDCR_COINSRNC_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TOT_TPL_AMT', cond1='888888888.88', cond2='999999.99') }
                , { TAF_Closure.var_set_type6('TOT_OTHR_INSRNC_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TP_COINSRNC_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TP_COPMT_PD_AMT', cond1='88888888888', cond2='888888888.00', cond3='888888888.88', cond4='99999999999.00') }
                , { TAF_Closure.var_set_type2(var='MDCR_CMBND_DDCTBL_IND', lpad=0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2(var='MDCR_REIMBRSMT_TYPE_CD', lpad=2, cond1='01', cond2='02', cond3='03', cond4='04', cond5='05', cond6='06', cond7='07', cond8='08', cond9='09') }
                , { TAF_Closure.var_set_type6('BENE_COINSRNC_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('BENE_COPMT_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('BENE_DDCTBL_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type2(var='COPAY_WVD_IND', lpad=0, cond1='0', cond2='1') }
                , { TAF_Closure.fix_old_dates('OCRNC_01_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_01_CD_END_DT') }
                , { TAF_Closure.var_set_type1(var='OCRNC_01_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_02_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_02_CD_END_DT') }
                , { TAF_Closure.var_set_type1(var='OCRNC_02_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_03_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_03_CD_END_DT') }
                , { TAF_Closure.var_set_type1(var='OCRNC_03_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_04_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_04_CD_END_DT') }
                , { TAF_Closure.var_set_type1(var='OCRNC_04_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_05_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_05_CD_END_DT') }
                , { TAF_Closure.var_set_type1(var='OCRNC_05_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_06_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_06_CD_END_DT') }
                , { TAF_Closure.var_set_type1(var='OCRNC_06_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_07_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_07_CD_END_DT') }
                , { TAF_Closure.var_set_type1(var='OCRNC_07_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_08_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_08_CD_END_DT') }
                , { TAF_Closure.var_set_type1(var='OCRNC_08_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_09_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_09_CD_END_DT') }
                , { TAF_Closure.var_set_type1(var='OCRNC_09_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_10_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_10_CD_END_DT') }
                , { TAF_Closure.var_set_type1(var='OCRNC_10_CD') }
                , { TAF_Closure.var_set_type2(var='SPLIT_CLM_IND', lpad=0, cond1=0, cond2=1) }

                ,CLL_CNT
                ,NUM_CLL

                ,ACCOMMODATION_PAID as ACMDTN_PD
                ,ANCILLARY_PAID as ANCLRY_PD
                ,CVRD_MH_DAYS_OVER_65 as CVRD_MH_DAYS_OVR_65
                ,CVRD_MH_DAYS_UNDER_21
                ,LT_MH_DX_IND
                ,LT_SUD_DX_IND
                ,LT_MH_TAXONOMY_IND as LT_MH_TXNMY_IND
                ,LT_SUD_TAXONOMY_IND as LT_SUD_TXNMY_IND
                ,nullif(IAP_CONDITION_IND, IAP_CONDITION_IND) as IAP_COND_IND
                ,nullif(PRIMARY_HIERARCHICAL_CONDITION, PRIMARY_HIERARCHICAL_CONDITION) as PRMRY_HIRCHCL_COND

            FROM (
                select
                    *,
                    case when ADJSTMT_IND is NOT NULL and
                    trim(ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(ADJSTMT_IND) else NULL end as ADJSTMT_IND_CLEAN
                from
                    LT_HEADER_GROUPER
                ) H
            """

        runner.append(type(self).__name__, z)
