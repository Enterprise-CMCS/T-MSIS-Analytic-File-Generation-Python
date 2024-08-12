from taf.LT.LT_Runner import LT_Runner
from taf.LT.LT_Metadata import LT_Metadata
from taf.TAF_Closure import TAF_Closure
from taf.TAF_Metadata import TAF_Metadata


class LTL:
    """
    Each LT TAF is comprised of two files – a claim-header level file and a claim-line level file.
    The claims included in these files are active, non-voided, non-denied (at the header level),
    non-duplicate final action claims. Only claim header records meeting these inclusion criteria,
    along with their associated claim line records, are incorporated. Both files can be linked together
    using unique keys that are constructed based on various claim header and claim line data elements.
    The two LT TAF are generated for each calendar month in which the data are reported.
    """

    def create(self, runner: LT_Runner):
        """
        Create the LT claim-line level segment.
        """

        z = f"""
            create or replace temporary view LTL as

            select

                 {runner.DA_RUN_ID} as DA_RUN_ID,
                 {runner.get_link_key_line()} as LT_LINK_KEY,
                '{runner.VERSION}' as LT_VRSN,
                '{runner.TAF_FILE_DATE}' as LT_FIL_DT

                , TMSIS_RUN_ID_LINE as TMSIS_RUN_ID
                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM_LINE', new='MSIS_IDENT_NUM') }
                , NEW_SUBMTG_STATE_CD_LINE as SUBMTG_STATE_CD

                , { TAF_Closure.var_set_type3('ORGNL_CLM_NUM_LINE', cond1='~', new='ORGNL_CLM_NUM') }
                , { TAF_Closure.var_set_type3('ADJSTMT_CLM_NUM_LINE', cond1='~', new='ADJSTMT_CLM_NUM') }
                , { TAF_Closure.var_set_type1('ADJSTMT_LINE_NUM') }
                , { TAF_Closure.var_set_type1('ORGNL_LINE_NUM') }

                , case when (ADJDCTN_DT_LINE < to_date('1600-01-01')) then to_date('1599-12-31') else nullif(ADJDCTN_DT_LINE, to_date('1960-01-01')) end as ADJDCTN_DT
                ,upper(LINE_ADJSTMT_IND_CLEAN) as LINE_ADJSTMT_IND

                , { TAF_Closure.var_set_tos('TOS_CD') }

                , case when lpad(IMNZTN_TYPE_CD,2,'0') = '88' then NULL
                    else  { TAF_Closure.var_set_type5('IMNZTN_TYPE_CD', lpad=2, lowerbound=0, upperbound='29', multiple_condition=True) }
                , { TAF_Closure.var_set_type2('CMS_64_FED_REIMBRSMT_CTGRY_CD', 2, cond1='01',  cond2='02', cond3='03', cond4='04') }

                , case when XIX_SRVC_CTGRY_CD in { tuple(TAF_Metadata.XIX_SRVC_CTGRY_CD_values) } then XIX_SRVC_CTGRY_CD else NULL end as XIX_SRVC_CTGRY_CD
                , case when XXI_SRVC_CTGRY_CD in { tuple(TAF_Metadata.XXI_SRVC_CTGRY_CD_values) } then XXI_SRVC_CTGRY_CD else NULL end as XXI_SRVC_CTGRY_CD

                , { TAF_Closure.var_set_type1('CLL_STUS_CD') }

                , case when SRVC_BGNNG_DT < to_date('1600-01-01') then to_date('1599-12-31') else nullif(SRVC_BGNNG_DT, to_date('1960-01-01')) end as SRVC_BGNNG_DT
                , case when SRVC_ENDG_DT < to_date('1600-01-01') then to_date('1599-12-31') else nullif(SRVC_ENDG_DT, to_date('1960-01-01')) end as SRVC_ENDG_DT

                , { TAF_Closure.var_set_type5('BNFT_TYPE_CD', lpad=3, lowerbound='001', upperbound='108') }

                , { TAF_Closure.var_set_type2('BLG_UNIT_CD', 2, cond1='01',  cond2='02', cond3='03', cond4='04', cond5='05', cond6='06', cond7='07') }

                , { TAF_Closure.var_set_fills('NDC_CD',cond1='0',cond2='8',cond3='9',cond4='#', spaces='YES') }
                , { TAF_Closure.var_set_type4('UOM_CD','YES',cond1='F2', cond2='ML', cond3='GR', cond4='UN', cond5='ME') }
                , { TAF_Closure.var_set_type6('NDC_QTY', cond1='888888', cond2='888888.888', cond3='999999', cond4='88888.888', cond5='888888.880', cond6='999999.998') }
                , { TAF_Closure.var_set_type1(var='HCPCS_RATE') }
                , { TAF_Closure.var_set_type1('SRVCNG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('SRVCNG_PRVDR_NPI_NUM', new='SRVCNG_PRVDR_NPI_NUM') }
                ,SRVCNG_PRVDR_TXNMY_CD
                , { TAF_Closure.var_set_prtype('SRVCNG_PRVDR_TYPE_CD') }
                , { TAF_Closure.var_set_spclty('SRVCNG_PRVDR_SPCLTY_CD') }

                , case when PRVDR_FAC_TYPE_CD in ('100000000', '170000000', '250000000', '260000000', '270000000', '280000000', '290000000', '300000000', '310000000', '320000000', '330000000', '340000000', '380000000') then PRVDR_FAC_TYPE_CD
                    else NULL end as PRVDR_FAC_TYPE_CD

                , { TAF_Closure.var_set_type6('RC_QTY_ACTL', new='ACTL_SRVC_QTY',	cond1='88888.888', cond2='99999.990', cond3='999999') }
                , { TAF_Closure.var_set_type6('RC_QTY_ALOWD', new='ALOWD_SRVC_QTY',	cond1='88888.888', cond2='888888.890') }
                , { TAF_Closure.var_set_type1(var='REV_CD',lpad=4) }
                , { TAF_Closure.var_set_type6('REV_CHRG_AMT', cond1='8888888888.88', cond2='88888888.88', cond3='888888888.88', cond4='88888888888.88', cond5='99999999.90', cond6='9999999999.99') }
                , { TAF_Closure.var_set_type6('ALOWD_AMT', cond1='888888888.88', cond2='99999999.00', cond3='9999999999.99') }
                , { TAF_Closure.var_set_type6('MDCD_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('OTHR_INSRNC_AMT', cond1='888888888.88', cond2='88888888888.00', cond3='88888888888.88') }
                , { TAF_Closure.var_set_type6('MDCD_FFS_EQUIV_AMT', cond1='888888888.88', cond2='88888888888.80', cond3='999999.99') }
                , { TAF_Closure.var_set_type6('TPL_AMT', cond1='888888888.88', cond2='88888888888.80', cond3='999999.99') }

                , from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                , cast(NULL as timestamp) as REC_UPDT_TS

                ,RN as LINE_NUM
                ,{ TAF_Closure.var_set_type1('IHS_SVC_IND') }
            FROM (
                select
                    *,
                    case when LINE_ADJSTMT_IND is NOT NULL and
                    trim(upper(LINE_ADJSTMT_IND)) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(upper(LINE_ADJSTMT_IND)) else NULL end as LINE_ADJSTMT_IND_CLEAN
                from
                    LT_LINE
                ) H
            """

        runner.append("LT", z)

    def build(self, runner: LT_Runner):
        """
        Build the LT claim-line level segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_ltl
                SELECT
                    { LT_Metadata.finalFormatter(LT_Metadata.line_columns) }
                FROM LTL
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
