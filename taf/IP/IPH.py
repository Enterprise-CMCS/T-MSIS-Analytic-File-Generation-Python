from taf.IP.IP_Runner import IP_Runner
from taf.IP.IP_Metadata import IP_Metadata
from taf.TAF_Closure import TAF_Closure


class IPH:
    """
    The IP TAF are comprised of two files – a claim header-level file and a claim line-level file.
    The claims included in these files are active, final-action, non-voided, non-denied claims.
    Only claim header records with a date in the TAF month/year, along with their associated claim
    line records, are included. Both files can be linked together using a unique key that is constructed
    based on various claim header and claim line data elements. The two IP TAF are produced for each
    calendar month for which data are reported.
    """

    def create(self, runner: IP_Runner):
        """
        Create the IP segment header.
        """

        z = f"""
            create or replace temporary view IPH as

            select

                {runner.DA_RUN_ID} as DA_RUN_ID,
                {runner.get_link_key()} as IP_LINK_KEY,
                '{runner.VERSION}' as IP_VRSN,
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

                , case
                    when DSCHRG_DT=to_date('1960-01-01') then NULL
                    else DSCHRG_DT
                  end as DSCHRG_DT

                , { TAF_Closure.var_set_type5('DSCHRG_HR_NUM', lpad=2, lowerbound=0, upperbound=23) }

                , case
                    when (ADJDCTN_DT < to_date('1600-01-01')) then to_date('1599-12-31')
                    when ADJDCTN_DT=to_date('1960-01-01') then NULL
                    else ADJDCTN_DT
                  end as ADJDCTN_DT

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
                ,rfrg_prvdr_type_cd
                ,RFRG_PRVDR_SPCLTY_CD

                , { TAF_Closure.var_set_type1('PRVDR_LCTN_ID') }
                , { TAF_Closure.var_set_type2('PYMT_LVL_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_type6('TOT_BILL_AMT', cond1='888888888.88', cond2='99999999.90', cond3='9999999.99', cond4='999999.99', cond5='999999.00') }
                , { TAF_Closure.var_set_type6('TOT_ALOWD_AMT', cond1='888888888.88', cond2='99999999.00') }
                , { TAF_Closure.var_set_type6('TOT_MDCD_PD_AMT', cond1='888888888.88') }
                ,TOT_COPAY_AMT
                , { TAF_Closure.var_set_type6('TOT_TPL_AMT', cond1='888888888.88', cond2='999999.99') }
                , { TAF_Closure.var_set_type6('TOT_OTHR_INSRNC_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TP_COINSRNC_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TP_COPMT_PD_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00', cond4='99999999999.00') }
                , { TAF_Closure.var_set_type6('MDCD_DSH_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('DRG_OUTLIER_AMT', cond1='888888888.88') }

                , floor(drg_rltv_wt_num * 10000) / 10000 AS drg_rltv_wt_num

                , { TAF_Closure.var_set_type6('MDCR_PD_AMT', cond1='888888888.88', cond2='8888888.88', cond3='88888888888.00', cond4='88888888888.88', cond5='99999999999.00', cond6='9999999999.99') }
                , { TAF_Closure.var_set_type6('TOT_MDCR_DDCTBL_AMT', cond1='888888888.88', cond2='99999', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('TOT_MDCR_COINSRNC_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type2('MDCR_CMBND_DDCTBL_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2('mdcr_reimbrsmt_type_cd', 2, cond1='01', cond2='02', cond3='03', cond4='04', cond5='05', cond6='06', cond7='07', cond8='08', cond9='09') }
                , { TAF_Closure.var_set_type6('TOT_BENE_COINSRNC_PD_AMT',new='BENE_COINSRNC_AMT',cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('TOT_BENE_COPMT_PD_AMT',new='BENE_COPMT_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('TOT_BENE_DDCTBL_PD_AMT',new='BENE_DDCTBL_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , COPAY_WVD_IND
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
                , cast(nullif(PRIMARY_HIERARCHICAL_CONDITION, PRIMARY_HIERARCHICAL_CONDITION) as char(9)) as PRMRY_HIRCHCL_COND

                , from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                , from_utc_timestamp(current_timestamp(), 'EST') as REC_UPDT_TS             --this must be equal to REC_ADD_TS for CCW pipeline

                , { TAF_Closure.fix_old_dates('SRVC_ENDG_DT_DRVD')}
                , { TAF_Closure.var_set_type2('SRVC_ENDG_DT_CD',0,cond1='1',cond2='2',cond3='3',cond4='4',cond5='5') }
                , { TAF_Closure.var_set_taxo('BLG_PRVDR_NPPES_TXNMY_CD',cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X',
                                    cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY')}

                , DGNS_1_CCSR_DFLT_CTGRY_CD
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
        runner.append("IP", z)

    def build(self, runner: IP_Runner):
        """
        Build the SQL query for the IP header segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_iph
                SELECT
                    { IP_Metadata.finalFormatter(IP_Metadata.header_columns) }
                FROM (
                    SELECT h.*
                        ,fasc.fed_srvc_ctgry_cd
                    FROM IPH AS h
                        LEFT JOIN IP_HDR_ROLLED AS fasc
                            ON h.ip_link_key = fasc.ip_link_key
                )
        """
        runner.append(type(self).__name__, z)


# -----------------------------------------------------------------------------
# CC0 1.0 Universal

# Statement of Purpose

# The laws of most jurisdictions throughout the world automatically confer
# exclusive Copyright and Related Rights (defined below) upon the creator and
# subsequent owner(s) (each and all, an "owner") of an original work of
# authorship and/or a database (each, a "Work").

# Certain owners wish to permanently relinquish those rights to a Work for the
# purpose of contributing to a commons of creative, cultural and scientific
# works ("Commons") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Commons to promote the ideal of a free culture and the
# further production of creative, cultural and scientific works, or to gain
# reputation or greater distribution for their Work in part through the use and
# efforts of others.

# For these and/or other purposes and motivations, and without any expectation
# of additional consideration or compensation, the person associating CC0 with a
# Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
# and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
# and publicly distribute the Work under its terms, with knowledge of his or her
# Copyright and Related Rights in the Work and the meaning and intended legal
# effect of CC0 on those rights.

# 1. Copyright and Related Rights. A Work made available under CC0 may be
# protected by copyright and related or neighboring rights ("Copyright and
# Related Rights"). Copyright and Related Rights include, but are not limited
# to, the following:

#   i. the right to reproduce, adapt, distribute, perform, display, communicate,
#   and translate a Work

#   ii. moral rights retained by the original author(s) and/or performer(s)

#   iii. publicity and privacy rights pertaining to a person's image or likeness
#   depicted in a Work

#   iv. rights protecting against unfair competition in regards to a Work,
#   subject to the limitations in paragraph 4(a), below

#   v. rights protecting the extraction, dissemination, use and reuse of data in
#   a Work

#   vi. database rights (such as those arising under Directive 96/9/EC of the
#   European Parliament and of the Council of 11 March 1996 on the legal
#   protection of databases, and under any national implementation thereof,
#   including any amended or successor version of such directive) and

#   vii. other similar, equivalent or corresponding rights throughout the world
#   based on applicable law or treaty, and any national implementations thereof.

# 2. Waiver. To the greatest extent permitted by, but not in contravention of,
# applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
# unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
# and Related Rights and associated claims and causes of action, whether now
# known or unknown (including existing as well as future claims and causes of
# action), in the Work (i) in all territories worldwide, (ii) for the maximum
# duration provided by applicable law or treaty (including future time
# extensions), (iii) in any current or future medium and for any number of
# copies, and (iv) for any purpose whatsoever, including without limitation
# commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
# the Waiver for the benefit of each member of the public at large and to the
# detriment of Affirmer's heirs and successors, fully intending that such Waiver
# shall not be subject to revocation, rescission, cancellation, termination, or
# any other legal or equitable action to disrupt the quiet enjoyment of the Work
# by the public as contemplated by Affirmer's express Statement of Purpose.

# 3. Public License Fallback. Should any part of the Waiver for any reason be
# judged legally invalid or ineffective under applicable law, then the Waiver
# shall be preserved to the maximum extent permitted taking into account
# Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
# is so judged Affirmer hereby grants to each affected person a royalty-free,
# non transferable, non sublicensable, non exclusive, irrevocable and
# unconditional license to exercise Affirmer's Copyright and Related Rights in
# the Work (i) in all territories worldwide, (ii) for the maximum duration
# provided by applicable law or treaty (including future time extensions), (iii)
# in any current or future medium and for any number of copies, and (iv) for any
# purpose whatsoever, including without limitation commercial, advertising or
# promotional purposes (the "License"). The License shall be deemed effective as
# of the date CC0 was applied by Affirmer to the Work. Should any part of the
# License for any reason be judged legally invalid or ineffective under
# applicable law, such partial invalidity or ineffectiveness shall not
# invalidate the remainder of the License, and in such case Affirmer hereby
# affirms that he or she will not (i) exercise any of his or her remaining
# Copyright and Related Rights in the Work or (ii) assert any associated claims
# and causes of action with respect to the Work, in either case contrary to
# Affirmer's express Statement of Purpose.

# 4. Limitations and Disclaimers.

#   a. No trademark or patent rights held by Affirmer are waived, abandoned,
#   surrendered, licensed or otherwise affected by this document.

#   b. Affirmer offers the Work as-is and makes no representations or warranties
#   of any kind concerning the Work, express, implied, statutory or otherwise,
#   including without limitation warranties of title, merchantability, fitness
#   for a particular purpose, non infringement, or the absence of latent or
#   other defects, accuracy, or the present or absence of errors, whether or not
#   discoverable, all to the greatest extent permissible under applicable law.

#   c. Affirmer disclaims responsibility for clearing rights of other persons
#   that may apply to the Work or any use thereof, including without limitation
#   any person's Copyright and Related Rights in the Work. Further, Affirmer
#   disclaims responsibility for obtaining any necessary consents, permissions
#   or other rights required for any use of the Work.

#   d. Affirmer understands and acknowledges that Creative Commons is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecommons.org/publicdomain/zero/1.0/>
