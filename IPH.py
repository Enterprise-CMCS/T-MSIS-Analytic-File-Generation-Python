# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
from pyspark.sql.functions import regexp_replace


from taf.IP.IP_Runner import IP_Runner
from taf.TAF_Closure import TAF_Closure


class IPH():

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self, runner: IP_Runner):

        z = f"""
            create or replace temporary view IPH as

            select

                {runner.DA_RUN_ID} as DA_RUN_ID,
                {runner.get_link_key()} as IP_LINK_KEY,
                '{runner.version}' as IP_VRSN,
                '{runner.TAF_FILE_DATE}' as IP_FIL_DT

                , TMSIS_RUN_ID
                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM') }
                , NEW_SUBMTG_STATE_CD as SUBMTG_STATE_CD

                , { TAF_Closure.var_set_type3('orgnl_clm_num', cond1='~') }
                , { TAF_Closure.var_set_type3('adjstmt_clm_num', cond1='~') }

                , ADJSTMT_IND_CLEAN as ADJSTMT_IND
                , { TAF_Closure.var_set_rsn('ADJSTMT_RSN_CD') }

                , { TAF_Closure.fix_old_dates('ADMSN_DT') }
                , { TAF_Closure.var_set_type5('ADMSN_HR_NUM', lpad=2, lowerbound=0, upperbound=23) }

                , nullif(DSCHRG_DT, to_date(to_date('1960-01-01'))) as DSCHRG_DT

                , { TAF_Closure.var_set_type5('DSCHRG_HR_NUM', lpad=2, lowerbound=0, upperbound=23) }

                , case when ADJDCTN_DT < to_date('1600-01-01') then to_date('1599-12-31') else nullif(ADJDCTN_DT, to_date('1960-01-01')) end as ADJDCTN_DT

                , { TAF_Closure.fix_old_dates('MDCD_PD_DT') }
                , { TAF_Closure.var_set_type2('ADMSN_TYPE_CD', 0, cond1='1', cond2='2', cond3='3', cond4='4', cond5='5') }
                , { TAF_Closure.var_set_type2('HOSP_TYPE_CD', 2, cond1='00', cond2='01', cond3='02', cond4='03', cond5='04', cond6='05', cond7='06', cond8='07', cond9='08') }
                , { TAF_Closure.var_set_type2('SECT_1115A_DEMO_IND', 0, cond1='0', cond2='1') }

                , case when upper(clm_type_cd) in('1', '2', '3', '4', '5', 'A', 'B', 'C', 'D', 'E', 'U', 'V', 'W', 'X', 'Y', 'Z') then upper(clm_type_cd)
                    else NULL end as clm_type_cd

                , { TAF_Closure.var_set_type1('BILL_TYPE_CD') }

                , case when lpad(pgm_type_cd, 2, '0') in ('06', '09') then NULL
                    else { TAF_Closure.var_set_type5('pgm_type_cd', lpad=2, lowerbound=0, upperbound=17, multiple_condition='YES') }

                , { TAF_Closure.var_set_type1('MC_PLAN_ID') }
                , { TAF_Closure.var_set_type1('ELGBL_LAST_NAME', upper='YES') }
                , { TAF_Closure.var_set_type1('ELGBL_1ST_NAME', upper='YES') }
                , { TAF_Closure.var_set_type1('ELGBL_MDL_INITL_NAME', upper='YES') }
                , { TAF_Closure.fix_old_dates('BIRTH_DT') }

                , case when lpad(wvr_type_cd, 2, '0') = '88' then NULL
                    else { TAF_Closure.var_set_type5('wvr_type_cd', lpad=2, lowerbound=1, upperbound=33, multiple_condition='YES') }

                , { TAF_Closure.var_set_type1('WVR_ID') }
                , { TAF_Closure.var_set_type2('srvc_trkng_type_cd', 2, cond1='00', cond2='01', cond3='02', cond4='03', cond5='04', cond6='05', cond7='06') }
                , { TAF_Closure.var_set_type6('SRVC_TRKNG_PYMT_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type2('OTHR_INSRNC_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2('othr_tpl_clctn_cd', 3, cond1='000', cond2='001', cond3='002', cond4='003', cond5='004', cond6='005', cond7='006', cond8='007') }
                , { TAF_Closure.var_set_type2('FIXD_PYMT_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type4('FUNDNG_CD', 'YES', cond1='A', cond2='B', cond3='C', cond4='D', cond5='E', cond6='F', cond7='G', cond8='H', cond9='I') }
                , { TAF_Closure.var_set_type2('fundng_src_non_fed_shr_cd', 2, cond1='01', cond2='02', cond3='03', cond4='04', cond5='05', cond6='06') }
                , { TAF_Closure.var_set_type2('BRDR_STATE_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2('XOVR_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type1('MDCR_HICN_NUM') }
                , { TAF_Closure.var_set_type1('MDCR_BENE_ID') }
                , { TAF_Closure.var_set_type1('PTNT_CNTL_NUM') }
                , { TAF_Closure.var_set_type2('HLTH_CARE_ACQRD_COND_CD', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_ptstatus('PTNT_STUS_CD') }

                , case when BIRTH_WT_GRMS_QTY <= 0 or BIRTH_WT_GRMS_QTY in (888889.000, 88888.888, 888888.000) then NULL
                    else cast(BIRTH_WT_GRMS_QTY as decimal(9, 3)) end as BIRTH_WT_GRMS_QTY

                , { TAF_Closure.var_set_fills('ADMTG_DGNS_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('ADMTG_DGNS_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_fills('DGNS_1_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_1_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_1_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_2_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_2_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_2_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_3_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_3_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_3_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_4_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_4_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_4_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_5_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_5_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_5_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_6_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_6_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_6_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_7_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_7_CD_IND', 0, cond1='1', cond2=2, cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_7_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_8_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_8_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_8_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_9_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_9_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_9_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_10_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_10_CD_IND', 0, cond1=1, cond2=2, cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_10_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_11_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_11_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_11_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_12_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_12_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_12_CD_IND') }

                , DRG_CD

                , { TAF_Closure.var_set_type1('DRG_CD_IND') }
                , { TAF_Closure.var_set_type1('DRG_DESC') }
                , { TAF_Closure.fix_old_dates('PRCDR_1_CD_DT') }
                , { TAF_Closure.var_set_fillpr('PRCDR_1_CD', cond1='0', cond2='8', cond3='9', cond4='#', spaces='YES') }
                , { TAF_Closure.var_set_proc('PRCDR_1_CD_IND') }
                , { TAF_Closure.fix_old_dates('PRCDR_2_CD_DT') }
                , { TAF_Closure.var_set_fillpr('PRCDR_2_CD', cond1='0', cond2='8', cond3='9', cond4='#', spaces='YES') }
                , { TAF_Closure.var_set_proc('PRCDR_2_CD_IND') }
                , { TAF_Closure.fix_old_dates('PRCDR_3_CD_DT') }
                , { TAF_Closure.var_set_fillpr('PRCDR_3_CD', cond1='0', cond2='8', cond3='9', cond4='#', spaces='YES') }
                , { TAF_Closure.var_set_proc('PRCDR_3_CD_IND') }
                , { TAF_Closure.fix_old_dates('PRCDR_4_CD_DT') }
                , { TAF_Closure.var_set_fillpr('PRCDR_4_CD', cond1='0', cond2='8', cond3='9', cond4='#', spaces='YES') }
                , { TAF_Closure.var_set_proc('PRCDR_4_CD_IND') }
                , { TAF_Closure.fix_old_dates('PRCDR_5_CD_DT') }
                , { TAF_Closure.var_set_fillpr('PRCDR_5_CD', cond1='0', cond2='8', cond3='9', cond4='#', spaces='YES') }
                , { TAF_Closure.var_set_proc('PRCDR_5_CD_IND') }
                , { TAF_Closure.fix_old_dates('PRCDR_6_CD_DT') }
                , { TAF_Closure.var_set_fillpr('PRCDR_6_CD', cond1='0', cond2='8', cond3='9', cond4='#', spaces='YES') }
                , { TAF_Closure.var_set_proc('PRCDR_6_CD_IND') }
                , { TAF_Closure.var_set_type6('NCVRD_DAYS_CNT', cond1='88888') }
                , { TAF_Closure.var_set_type6('NCVRD_CHRGS_AMT', cond1='88888888888.00') }
                , { TAF_Closure.var_set_type6('MDCD_CVRD_IP_DAYS_CNT', cond1='88888') }
                , { TAF_Closure.var_set_type6('OUTLIER_DAYS_CNT', cond1='888') }

                , case when lpad(OUTLIER_CD, 2, '0') in ('03', '04', '05') then NULL
                    else { TAF_Closure.var_set_type5('outlier_cd', lpad=2, lowerbound=0, upperbound=10, multiple_condition='YES') }

                , { TAF_Closure.var_set_type1('ADMTG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_type1('ADMTG_PRVDR_NUM') }
                , { TAF_Closure.var_set_spclty('ADMTG_PRVDR_SPCLTY_CD') }
                , { TAF_Closure.var_set_taxo('ADMTG_PRVDR_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_prtype('admtg_prvdr_type_cd') }
                , { TAF_Closure.var_set_type1('BLG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('BLG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_taxo('BLG_PRVDR_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_prtype('blg_prvdr_type_cd') }
                , { TAF_Closure.var_set_spclty('BLG_PRVDR_SPCLTY_CD') }
                , { TAF_Closure.var_set_type1('RFRG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('RFRG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_prtype('rfrg_prvdr_type_cd') }
                , { TAF_Closure.var_set_spclty('RFRG_PRVDR_SPCLTY_CD') }

                , { TAF_Closure.var_set_type1('PRVDR_LCTN_ID') }
                , { TAF_Closure.var_set_type2('PYMT_LVL_IND', 0, cond1='1', cond2='2') }
                , { TAF_Closure.var_set_type6('TOT_BILL_AMT', cond1='888888888.88', cond2='99999999.90', cond3='9999999.99', cond4='999999.99', cond5='999999.00') }
                , { TAF_Closure.var_set_type6('TOT_ALOWD_AMT', cond1='888888888.88', cond2='99999999.00') }
                , { TAF_Closure.var_set_type6('TOT_MDCD_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TOT_COPAY_AMT', cond1='9999999.99', cond2='888888888.88', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('TOT_TPL_AMT', cond1='888888888.88', cond2='999999.99') }
                , { TAF_Closure.var_set_type6('TOT_OTHR_INSRNC_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TP_COINSRNC_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TP_COPMT_PD_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00', cond4='99999999999.00') }
                , { TAF_Closure.var_set_type6('MDCD_DSH_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('DRG_OUTLIER_AMT', cond1='888888888.88') }

                --- regexp_count does not exist within databricks sql
                , case
                    when (length(drg_rltv_wt_num) - coalesce(length(regexp_replace(drg_rltv_wt_num, "[^0-9.]", "")), 0)) > 0
                      or (length(drg_rltv_wt_num) - coalesce(length(regexp_replace(drg_rltv_wt_num, "[.]", "")), 0)) > 1
                        then NULL
                    when cast(drg_rltv_wt_num as numeric(8, 0)) > 9999999
                      THEN NULL
                    else cast(drg_rltv_wt_num as numeric(11, 4))
                    end as DRG_RLTV_WT_NUM

                , { TAF_Closure.var_set_type6('MDCR_PD_AMT', cond1='888888888.88', cond2='8888888.88', cond3='88888888888.00', cond4='88888888888.88', cond5='99999999999.00', cond6='9999999999.99') }
                , { TAF_Closure.var_set_type6('TOT_MDCR_DDCTBL_AMT', cond1='888888888.88', cond2='99999', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('TOT_MDCR_COINSRNC_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type2('MDCR_CMBND_DDCTBL_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2('mdcr_reimbrsmt_type_cd', 2, cond1='01', cond2='02', cond3='03', cond4='04', cond5='05', cond6='06', cond7='07', cond8='08', cond9='09') }
                , { TAF_Closure.var_set_type6('BENE_COINSRNC_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('BENE_COPMT_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('BENE_DDCTBL_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type2('COPAY_WVD_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.fix_old_dates('OCRNC_01_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_01_CD_END_DT') }
                , { TAF_Closure.var_set_type1('OCRNC_01_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_02_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_02_CD_END_DT') }
                , { TAF_Closure.var_set_type1('OCRNC_02_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_03_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_03_CD_END_DT') }
                , { TAF_Closure.var_set_type1('OCRNC_03_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_04_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_04_CD_END_DT') }
                , { TAF_Closure.var_set_type1('OCRNC_04_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_05_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_05_CD_END_DT') }
                , { TAF_Closure.var_set_type1('OCRNC_05_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_06_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_06_CD_END_DT') }
                , { TAF_Closure.var_set_type1('OCRNC_06_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_07_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_07_CD_END_DT') }
                , { TAF_Closure.var_set_type1('OCRNC_07_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_08_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_08_CD_END_DT') }
                , { TAF_Closure.var_set_type1('OCRNC_08_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_09_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_09_CD_END_DT') }
                , { TAF_Closure.var_set_type1('OCRNC_09_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_10_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_10_CD_END_DT') }
                , { TAF_Closure.var_set_type1('OCRNC_10_CD') }
                , { TAF_Closure.var_set_type2('SPLIT_CLM_IND', 0, cond1='0', cond2='1') }

                , CLL_CNT
                , NUM_CLL

                , IP_MH_DX_IND
                , IP_SUD_DX_IND
                , IP_MH_TAXONOMY_IND as IP_MH_TXNMY_IND
                , IP_SUD_TAXONOMY_IND as IP_SUD_TXNMY_IND
                , null::char(3) as MAJ_DGNSTC_CTGRY
                , cast(nullif(IAP_CONDITION_IND, IAP_CONDITION_IND) as char(6)) as IAP_COND_IND
                , cast(nullif(PRIMARY_HIERARCHICAL_CONDITION, PRIMARY_HIERARCHICAL_CONDITION) as char(9)) as PRMRY_HIRCHCL_COND,

                from_utc_timestamp(current_date(), 'EST') as REC_ADD_TS,
                from_utc_timestamp(current_date(), 'EST') as REC_UPDT_TS,
                case
                    when SRVC_ENDG_DT_DRVD < to_date('1600-01-01') then to_date('1599-12-31')
                    else SRVC_ENDG_DT_DRVD
                end as SRVC_ENDG_DT_DRVD,
                case
                    when SRVC_ENDG_DT_CD is NOT NULL
                    and trim(SRVC_ENDG_DT_CD) in ('1', '2', '3', '4', '5') then trim(SRVC_ENDG_DT_CD)
                    else NULL
                end as SRVC_ENDG_DT_CD

            FROM (
                select
                    *,
                    case when ADJSTMT_IND is NOT NULL and
                    trim(ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(ADJSTMT_IND) else NULL end as ADJSTMT_IND_CLEAN
                from
                    IP_HEADER_GROUPER
                ) H
            """

        runner.append(type(self).__name__, z)
