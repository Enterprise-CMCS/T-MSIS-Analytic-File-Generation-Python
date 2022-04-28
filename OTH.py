# --------------------------------------------------------------------------------- }
#
#
#
#
# --------------------------------------------------------------------------------- }
from taf.OT.OT_Runner import OT_Runner
from taf.OT.OT_Metadata import OT_Metadata
from taf.TAF_Closure import TAF_Closure


class OTH():

    # --------------------------------------------------------------------------------- }
    #
    #
    #
    #
    # --------------------------------------------------------------------------------- }
    def create(self, runner: OT_Runner):

        # ORDER VARIABLES AND UPCASE', LEFT PAD WITH ZEROS AND RESET COALESCE VALUES HEADER FILE }

        z = f"""
            create or replace temporary view OTH as

            select

                 {runner.DA_RUN_ID} as DA_RUN_ID,
                 {runner.get_link_key()} as OT_LINK_KEY,
                '{runner.version}' as OT_VRSN,
                '{runner.TAF_FILE_DATE}' as OT_FIL_DT

                , TMSIS_RUN_ID
                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM') }
                , NEW_SUBMTG_STATE_CD as SUBMTG_STATE_CD

                , { TAF_Closure.var_set_type3('orgnl_clm_num', cond1='~') }
                , { TAF_Closure.var_set_type3('adjstmt_clm_num', cond1='~') }

                , ADJSTMT_IND_CLEAN as ADJSTMT_IND
                , { TAF_Closure.var_set_rsn('ADJSTMT_RSN_CD') }

                , case when (SRVC_BGNNG_DT_HEADER < to_date('1600-01-01')) then to_date('1599-12-31') else nullif(SRVC_BGNNG_DT_HEADER, to_date('1960-01-01')) end as SRVC_BGNNG_DT
                , nullif(SRVC_ENDG_DT_HEADER, to_date('1960-01-01')) as SRVC_ENDG_DT
                , case when (ADJDCTN_DT< to_date('1600-01-01')) then to_date('1599-12-31') else nullif(ADJDCTN_DT, to_date('1960-01-01')) end as ADJDCTN_DT

                , { TAF_Closure.fix_old_dates('MDCD_PD_DT') }
                , { TAF_Closure.var_set_type2('SECT_1115A_DEMO_IND', 0, cond1='0', cond2='1') }
                , case when upper(clm_type_cd) in('1', '2', '3', '4', '5', 'A', 'B', 'C', 'D', 'E', 'U', 'V', 'W', 'X', 'Y', 'Z') then upper(clm_type_cd) else NULL end as clm_type_cd
                , { TAF_Closure.var_set_type1('BILL_TYPE_CD') }
                , case when lpad(pgm_type_cd, 2, '0') in ('06', '09') then NULL else { TAF_Closure.var_set_type5('pgm_type_cd', lpad=2, lowerbound=0, upperbound=17, multiple_condition='YES') }
                , { TAF_Closure.var_set_type1('MC_PLAN_ID') }
                , { TAF_Closure.var_set_type1('ELGBL_LAST_NAME', upper='YES') }
                , { TAF_Closure.var_set_type1('ELGBL_1ST_NAME', upper='YES') }
                , { TAF_Closure.var_set_type1('ELGBL_MDL_INITL_NAME', upper='YES') }
                , { TAF_Closure.fix_old_dates('BIRTH_DT') }

                , case when lpad(wvr_type_cd, 2, '0') = '88' then NULL else { TAF_Closure.var_set_type5('wvr_type_cd', lpad=2,lowerbound=1,upperbound=33,multiple_condition='YES') }

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
                , { TAF_Closure.var_set_type2('HLTH_CARE_ACQRD_COND_CD', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_fills('DGNS_1_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_1_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_1_CD_IND') }
                , { TAF_Closure.var_set_fills('DGNS_2_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type2('DGNS_2_CD_IND', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_poa('DGNS_POA_2_CD_IND') }
                , { TAF_Closure.var_set_type1('SRVC_PLC_CD', lpad=2) }
                , { TAF_Closure.var_set_type1('PRVDR_LCTN_ID') }
                , { TAF_Closure.var_set_type1('BLG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('BLG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_taxo('BLG_PRVDR_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_prtype('blg_prvdr_type_cd') }
                , { TAF_Closure.var_set_spclty('BLG_PRVDR_SPCLTY_CD') }
                , { TAF_Closure.var_set_type1('RFRG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('RFRG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_taxo('RFRG_PRVDR_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_prtype('rfrg_prvdr_type_cd') }
                , { TAF_Closure.var_set_spclty('RFRG_PRVDR_SPCLTY_CD') }
                , { TAF_Closure.var_set_type1('PRVDR_UNDER_DRCTN_NPI_NUM') }
                , { TAF_Closure.var_set_taxo('PRVDR_UNDER_DRCTN_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_type1('PRVDR_UNDER_SPRVSN_NPI_NUM') }
                , { TAF_Closure.var_set_taxo('PRVDR_UNDER_SPRVSN_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_type2('HH_PRVDR_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type1('HH_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_type1('HH_ENT_NAME') }
                , { TAF_Closure.var_set_type1('RMTNC_NUM') }
                , { TAF_Closure.var_set_type6('DAILY_RATE', cond1='88888', cond2='88888.80', cond3='88888.88') }
                , { TAF_Closure.var_set_type2('PYMT_LVL_IND', 0, cond1='1', cond2='2') }
                , { TAF_Closure.var_set_type6('TOT_BILL_AMT', cond1='999999.00', cond2='888888888.88', cond3='9999999.99', cond4='99999999.90', cond5='999999.99') }
                , { TAF_Closure.var_set_type6('TOT_ALOWD_AMT', cond1='99999999', cond2='888888888.88') }
                , { TAF_Closure.var_set_type6('TOT_MDCD_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TOT_COPAY_AMT', cond1='888888888.88', cond2='9999999.99', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('TOT_MDCR_DDCTBL_AMT', cond1='888888888.88', cond2='99999', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('TOT_MDCR_COINSRNC_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TOT_TPL_AMT', cond1='888888888.88', cond2='999999.99') }
                , { TAF_Closure.var_set_type6('TOT_OTHR_INSRNC_AMT', cond1='888888888.8', cond2='888888888.88') }
                , { TAF_Closure.var_set_type6('TP_COINSRNC_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('TP_COPMT_PD_AMT', cond1='888888888.88', cond2='888888888', cond3='888888888.00', cond4='99999999999.00') }
                , { TAF_Closure.var_set_type2('MDCR_CMBND_DDCTBL_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.var_set_type2('mdcr_reimbrsmt_type_cd', 2, cond1='01', cond2='02', cond3='03', cond4='04', cond5='05', cond6='06', cond7='07', cond8='08', cond9='09') }
                , { TAF_Closure.var_set_type6('BENE_COINSRNC_AMT', cond1='888888888.88', cond2='888888888', cond3='88888888888.00') }
                , { TAF_Closure.fix_old_dates('BENE_COINSRNC_PD_DT') }
                , { TAF_Closure.var_set_type6('BENE_COPMT_AMT',  cond1='888888888.88', cond2='888888888', cond3='88888888888.00') }
                , { TAF_Closure.fix_old_dates('BENE_COPMT_PD_DT') }
                , { TAF_Closure.var_set_type6('BENE_DDCTBL_AMT',  cond1='888888888.88', cond2='888888888', cond3='88888888888.00') }
                , { TAF_Closure.fix_old_dates('BENE_DDCTBL_PD_DT') }
                , { TAF_Closure.var_set_type2('COPAY_WVD_IND', 0, cond1='0', cond2='1') }
                , { TAF_Closure.fix_old_dates('CPTATD_AMT_RQSTD_DT') }
                , { TAF_Closure.var_set_type6('CPTATD_PYMT_RQSTD_AMT', 	cond1='888888888.88', cond2='888888888') }
                , { TAF_Closure.var_set_type1('OCRNC_01_CD') }
                , { TAF_Closure.var_set_type1('OCRNC_02_CD') }
                , { TAF_Closure.var_set_type1('OCRNC_03_CD') }
                , { TAF_Closure.var_set_type1('OCRNC_04_CD') }
                , { TAF_Closure.var_set_type1('OCRNC_05_CD') }
                , { TAF_Closure.var_set_type1('OCRNC_06_CD') }
                , { TAF_Closure.var_set_type1('OCRNC_07_CD') }
                , { TAF_Closure.var_set_type1('OCRNC_08_CD') }
                , { TAF_Closure.var_set_type1('OCRNC_09_CD') }
                , { TAF_Closure.var_set_type1('OCRNC_10_CD') }
                , { TAF_Closure.fix_old_dates('OCRNC_01_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_02_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_03_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_04_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_05_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_06_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_07_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_08_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_09_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_10_CD_EFCTV_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_01_CD_END_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_02_CD_END_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_03_CD_END_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_04_CD_END_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_05_CD_END_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_06_CD_END_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_07_CD_END_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_08_CD_END_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_09_CD_END_DT') }
                , { TAF_Closure.fix_old_dates('OCRNC_10_CD_END_DT') }

                , CLL_CNT
                , NUM_CLL

                , OTHR_TOC_MH_DX_IND as OT_MH_DX_IND
                , OTHR_TOC_SUD_DX_IND as OT_SUD_DX_IND
                , OTHR_TOC_MH_TAXONOMY_IND as OT_MH_TAXONOMY_IND
                , OTHR_TOC_SUD_TAXONOMY_IND as OT_SUD_TAXONOMY_IND

                , cast(nullif(IAP_CONDITION_IND, IAP_CONDITION_IND) as char(6)) as IAP_COND_IND
                , cast(nullif(PRIMARY_HIERARCHICAL_CONDITION, PRIMARY_HIERARCHICAL_CONDITION) as char(9)) as PRMRY_HIRCHCL_COND

            from (
                select
                    *,
                    case when ADJSTMT_IND is NOT NULL and
                    trim(ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(ADJSTMT_IND) else NULL end as ADJSTMT_IND_CLEAN
                from
                    OTHR_TOC_HEADER_GROUPER
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
                INSERT INTO {runner.DA_SCHEMA}.taf_oth
                SELECT
                    { OT_Metadata.finalFormatter(OT_Metadata.header_columns) }
                FROM
                    (SELECT * FROM OTH)
        """

        runner.append(type(self).__name__, z)