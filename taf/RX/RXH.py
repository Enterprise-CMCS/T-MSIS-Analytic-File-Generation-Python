from taf.RX.RX_Runner import RX_Runner
from taf.RX.RX_Metadata import RX_Metadata
from taf.TAF_Closure import TAF_Closure


class RXH:
    """
    The RX TAF are comprised of two files – a claim header-level file and a claim line-level file.
    The claims included in these files are active, non-voided, non-denied (at the header level),
    non-duplicate final action claims. Only claim header records meeting these inclusion criteria,
    along with their associated claim line records, are incorporated. Both files can be linked together
    using unique keys that are constructed based on various claim header and claim line data elements.
    The two RX TAF are generated for each calendar month for which data are reported.
    """

    def create(self, runner: RX_Runner):
        """
        Create the claim header-level segment.
        """

        z = f"""
            create or replace temporary view RXH as

            select

                 {runner.DA_RUN_ID} as DA_RUN_ID,
                 {runner.get_link_key()} as RX_LINK_KEY,
                '{runner.VERSION}' as RX_VRSN,
                '{runner.TAF_FILE_DATE}' as RX_FIL_DT

                , tmsis_run_id
                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM') }
                , new_submtg_state_cd as submtg_state_cd

                , { TAF_Closure.var_set_type3('orgnl_clm_num', cond1='~') }
                , { TAF_Closure.var_set_type3('adjstmt_clm_num', cond1='~') }
                , ADJSTMT_IND_CLEAN as ADJSTMT_IND
                , { TAF_Closure.var_set_rsn('ADJSTMT_RSN_CD') }

                , case
                    when (ADJDCTN_DT < to_date('1600-01-01')) then to_date('1599-12-31')
                    when ADJDCTN_DT=to_date('1960-01-01') then NULL
                    else ADJDCTN_DT
                  end as ADJDCTN_DT

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
                , { TAF_Closure.var_set_type6('BENE_COINSRNC_AMT',new='BENE_COINSRNC_AMT', cond1='888888888.88', cond2='888888888.00', cond3='88888888888.00') }
                , { TAF_Closure.var_set_type6('BENE_COPMT_AMT',new='BENE_COPMT_AMT',	cond1='88888888888.00', cond2='888888888.88', cond3='888888888.00') }
                , { TAF_Closure.var_set_type6('BENE_DDCTBL_AMT',new='BENE_DDCTBL_AMT', cond1='88888888888.00', cond2='888888888.88', cond3='888888888.00') }
                , { TAF_Closure.var_set_type2('COPAY_WVD_IND', 0, cond1='0', cond2='1') }
                , cll_cnt
                , num_cll
                ,from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                ,cast(NULL as timestamp) as REC_UPDT_TS
            from (
                select
                    *,
                    case when ADJSTMT_IND is NOT NULL and
                    trim(ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(ADJSTMT_IND) else NULL end as ADJSTMT_IND_CLEAN
                from
                    RX_HEADER
                )
            """

        runner.append("RX", z)

    def build(self, runner: RX_Runner):
        """
        Build the claim header-level segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_rxh
                SELECT
                    { RX_Metadata.finalFormatter(RX_Metadata.header_columns) }
                FROM (
                    SELECT h.*
                        ,fasc.fed_srvc_ctgry_cd
                    FROM RXH AS h
                        LEFT JOIN RX_HDR_ROLLED AS fasc
                            ON h.rx_link_key = fasc.rx_link_key
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
