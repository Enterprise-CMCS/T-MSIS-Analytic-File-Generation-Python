from taf.RX.RX_Runner import RX_Runner
from taf.RX.RX_Metadata import RX_Metadata
from taf.TAF_Closure import TAF_Closure
from taf.TAF_Metadata import TAF_Metadata


class RXL:
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
        Create the claim line-level segment.
        """

        z = f"""
            create or replace temporary view RXL as

            select

                 {runner.DA_RUN_ID} as DA_RUN_ID,
                 {runner.get_link_key_line()} as RX_LINK_KEY,
                '{runner.VERSION}' as RX_VRSN,
                '{runner.TAF_FILE_DATE}' as RX_FIL_DT

                ,TMSIS_RUN_ID_LINE as TMSIS_RUN_ID

                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM_LINE',new='MSIS_IDENT_NUM') }
                ,NEW_SUBMTG_STATE_CD_LINE as SUBMTG_STATE_CD

                , { TAF_Closure.var_set_type3('ORGNL_CLM_NUM_LINE',cond1='~',new='ORGNL_CLM_NUM') }
                , { TAF_Closure.var_set_type3('ADJSTMT_CLM_NUM_LINE',cond1='~',new='ADJSTMT_CLM_NUM') }
                , { TAF_Closure.var_set_type1('ORGNL_LINE_NUM')  }
                , { TAF_Closure.var_set_type1('ADJSTMT_LINE_NUM') }

                ,case when ADJDCTN_DT_LINE < to_date('1600-01-01') then to_date('1599-12-31') else nullif(ADJDCTN_DT_LINE, to_date('1960-01-01')) end as ADJDCTN_DT
                ,LINE_ADJSTMT_IND_CLEAN as LINE_ADJSTMT_IND

                , { TAF_Closure.var_set_tos('TOS_CD') }
                , { TAF_Closure.var_set_fills('NDC_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.var_set_type4('UOM_CD', True, cond1='F2',cond2='ML',cond3='GR',cond4='UN',cond5='ME',cond6='EA',cond7='GM') }
                , { TAF_Closure.var_set_type6('suply_days_cnt', cond1='8888', cond2='999', cond3='0') }
                , { TAF_Closure.var_set_type5('NEW_REFL_IND',lpad='2',lowerbound='0',upperbound='99') }
                , { TAF_Closure.var_set_type2('BRND_GNRC_IND', 0, cond1='0', cond2='1', cond3='2', cond4='3', cond5='4') }
                , { TAF_Closure.var_set_type6('DSPNS_FEE_SBMTD', cond1='88888.88')  }
                ,case when trim(DRUG_UTLZTN_CD) is not NULL then upper(DRUG_UTLZTN_CD)
                    else NULL
                    end as DRUG_UTLZTN_CD

                , { TAF_Closure.var_set_type6('dtl_mtrc_dcml_qty', cond1='999999.999') }

                ,case when lpad(CMPND_DSG_FORM_CD, 2, '0') in ('08','09') then NULL
                    else { TAF_Closure.var_set_type5('CMPND_DSG_FORM_CD', lpad=2, lowerbound=1, upperbound=18, multiple_condition=True) }

                , { TAF_Closure.var_set_type2('REBT_ELGBL_IND', 0, cond1='0', cond2='1', cond3='2') }
                ,IMNZTN_TYPE_CD
                ,BNFT_TYPE_CD
                , { TAF_Closure.var_set_type6('RX_QTY_ALOWD',cond1='99999', cond2='99999.999', cond3='888888.000', cond4='999999', cond5='888888.880') }
                , { TAF_Closure.var_set_type6('RX_QTY_ACTL',cond1='999999.99', cond2='888888', cond3='999999', cond4='0') }
                , { TAF_Closure.var_set_type2('FED_REIMBRSMT_CTGRY_CD',2, cond1='01',cond2='02',cond3='03',cond4='04') }

                ,XIX_SRVC_CTGRY_CD
                ,XXI_SRVC_CTGRY_CD

                , { TAF_Closure.var_set_type1('CLL_STUS_CD') }
                , { TAF_Closure.var_set_type6('bill_amt', cond1='9999999999.99', cond2='999999.99', cond3='999999', cond4='888888888.88') }
                , { TAF_Closure.var_set_type6('alowd_amt', cond1='9999999999.99', cond2='888888888.88', cond3='99999999.00') }
                , { TAF_Closure.var_set_type6('BENE_COPMT_PD_AMT',	cond1='888888888.88', cond2='88888888888.00') }
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
                , from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                , cast(NULL as timestamp) as REC_UPDT_TS
                ,RN as LINE_NUM
                , { TAF_Closure.var_set_type1('IHS_SVC_IND')}
                , INGRDNT_CST_SBMTD
                , INGRDNT_CST_PD_AMT
                , DSPNS_FEE_PD_AMT
                , PROFNL_SVC_FEE_SBMTD
                , PROFNL_SVC_FEE_PD_AMT
                , GME_PD_AMT
                , case when upper(lpad(trim(MBESCBES_SRVC_CTGRY),5,'0')) in {tuple(TAF_Metadata.MBESCBES_SRVC_CTGRY_values)}
                            then upper(lpad(trim(MBESCBES_SRVC_CTGRY),5,'0'))
                            else NULL end as MBESCBES_SRVC_CTGRY
                , case when replace(upper(trim(MBESCBES_FRM)),' ','') in {tuple(x.replace(" ","") for x in TAF_Metadata.MBESCBES_FRM_values)} then upper(trim(MBESCBES_FRM)) else NULL end as MBESCBES_FRM
                , { TAF_Closure.var_set_type2('MBESCBES_FRM_GRP', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_fillpr('PRCDR_CD', cond1='0', cond2='8', cond3='9', cond4='#', spaces='YES') }
                , { TAF_Closure.var_set_type1('PRCDR_1_MDFR_CD',lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_2_MDFR_CD',lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_3_MDFR_CD',lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_4_MDFR_CD',lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_5_MDFR_CD',lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_6_MDFR_CD',lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_7_MDFR_CD',lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_8_MDFR_CD',lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_9_MDFR_CD',lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_10_MDFR_CD',lpad=2) }
                , SDP_ALOWD_AMT
                , SDP_PD_AMT
                , { TAF_Closure.var_set_type1('UNIQ_DVC_ID') }
                , taf_classic_ind
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

        runner.append("RX", z)

        z = f"""
            create or replace temporary view RXL_classic as
                select *
                from RXL
                where TAF_Classic_ind = 1
        """
        runner.append("RX", z)

        z = f"""
            create or replace temporary view RXL_denied as
                select *
                from RXL
                where TAF_Classic_ind = 0
        """
        runner.append("RX", z)

    def build(self, runner: RX_Runner):
        """
        Build the claim line-level segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        input_table = {
            False:"RXL_classic",
            True:"RXL_Denied"
        }
        output_table = {
            False: "taf_rxl",
            True:  "taf_rxl_d"}

        for denied_flag in [False,True]:
            z = f"""
                    INSERT INTO {runner.DA_SCHEMA}.{output_table[denied_flag]}
                    SELECT
                        { RX_Metadata.finalFormatter(RX_Metadata.line_columns) }
                    FROM {input_table[denied_flag]}
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
