# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
from taf.LT.LT_Runner import LT_Runner
from taf.LT.LT_Metadata import LT_Metadata
from taf.TAF_Closure import TAF_Closure


class LTH:

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
                , { TAF_Closure.var_set_type6('BENE_COINSRNC_AMT',new='BENE_COINSRNC_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('BENE_COPMT_AMT', new='BENE_COPMT_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('BENE_DDCTBL_AMT', new='BENE_DDCTBL_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
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
                ,to_timestamp('{runner.DA_RUN_ID}', 'yyyyMMddHHmmss') as REC_ADD_TS
                ,cast(NULL as timestamp) as REC_UPDT_TS
                ,{ TAF_Closure.var_set_taxo('BLG_PRVDR_NPPES_TXNMY_CD',cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X',
                                                cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                ,DGNS_1_CCSR_DFLT_CTGRY_CD
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

        runner.append("LT", z)

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def build(self, runner: LT_Runner):

        z = f"""
                INSERT INTO {runner.DA_SCHEMA_DC}.taf_lth
                SELECT
                    { LT_Metadata.finalFormatter(LT_Metadata.header_columns) }
                FROM (
                    SELECT h.*
                        ,fasc.fed_srvc_ctgry_cd
                    FROM LTH AS h
                        LEFT JOIN LT_HDR_ROLLED AS fasc
                            ON h.lt_link_key = fasc.lt_link_key
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
