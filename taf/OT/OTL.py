from taf.OT.OT_Runner import OT_Runner
from taf.OT.OT_Metadata import OT_Metadata
from taf.TAF_Closure import TAF_Closure
from taf.TAF_Metadata import TAF_Metadata


class OTL:
    """
    Each OT TAF is comprised of two files – a claim-header level file and a claim-line level file.
    The claims included in these files are active, final-action, non-voided, and non-denied claims.
    Only header claims with a date in the TAF month/year, along with their associated claim line
    records, are included. Both files can be linked together using a unique key that is constructed
    based on various claim header and claim line data elements. The two OT TAF are produced for each
    calendar month in which the data are reported.
    """

    def create(self, runner: OT_Runner):
        """
        Create the OT claim-line level segment.
        """

        z = f"""
            create or replace temporary view OTL as

            select

                 {runner.DA_RUN_ID} as DA_RUN_ID,
                 {runner.get_link_key_line()} as OT_LINK_KEY,
                '{runner.VERSION}' as OT_VRSN,
                '{runner.TAF_FILE_DATE}' as OT_FIL_DT

                , TMSIS_RUN_ID_LINE as TMSIS_RUN_ID
                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM_LINE', new='MSIS_IDENT_NUM') }
                , NEW_SUBMTG_STATE_CD_LINE as SUBMTG_STATE_CD

                , { TAF_Closure.var_set_type3('ORGNL_CLM_NUM_LINE', cond1='~', new='ORGNL_CLM_NUM') }
                , { TAF_Closure.var_set_type3('ADJSTMT_CLM_NUM_LINE', cond1='~', new='ADJSTMT_CLM_NUM') }
                , { TAF_Closure.var_set_type1('ORGNL_LINE_NUM') }
                , { TAF_Closure.var_set_type1('ADJSTMT_LINE_NUM') }
                , case when ADJDCTN_DT_LINE < to_date('1600-01-01') then to_date('1599-12-31') else nullif(ADJDCTN_DT_LINE, to_date('1960-01-01')) end as ADJDCTN_DT
                , LINE_ADJSTMT_IND_CLEAN as LINE_ADJSTMT_IND
                , { TAF_Closure.var_set_rsn('ADJSTMT_LINE_RSN_CD') }
                , { TAF_Closure.var_set_type1('CLL_STUS_CD') }
                , case when SRVC_BGNNG_DT_LINE < to_date('1600-01-01') then to_date('1599-12-31') else nullif(SRVC_BGNNG_DT_LINE, to_date('1960-01-01')) end as SRVC_BGNNG_DT
                , case when SRVC_ENDG_DT_LINE < to_date('1600-01-01') then to_date('1599-12-31') else nullif(SRVC_ENDG_DT_LINE, to_date('1960-01-01')) end as SRVC_ENDG_DT
                , { TAF_Closure.var_set_type1('REV_CD', lpad=4) }
                , { TAF_Closure.var_set_fillpr('PRCDR_CD', cond1='0', cond2='8', cond3='9', cond4='#') }
                , { TAF_Closure.fix_old_dates('PRCDR_CD_DT') }
                , { TAF_Closure.var_set_proc('PRCDR_CD_IND', upper=True) }
                , { TAF_Closure.var_set_type1('PRCDR_1_MDFR_CD', upper=True, lpad=2) }
                , case when lpad(IMNZTN_TYPE_CD, 2, '0') = '88' then NULL
                    else { TAF_Closure.var_set_type5('IMNZTN_type_cd', lpad=2, lowerbound=0, upperbound=29, multiple_condition='YES') }
                , { TAF_Closure.var_set_type6('BILL_AMT', cond1='888888888.88', cond2='9999999999.99', cond3='999999.99', cond4='999999') }
                , { TAF_Closure.var_set_type6('ALOWD_AMT', cond1='99999999.00', cond2='888888888.88', cond3='9999999999.99') }
                , { TAF_Closure.var_set_type6('BENE_COPMT_PD_AMT', new='COPAY_AMT', cond1='88888888888.00', cond2='888888888.88') }
                , { TAF_Closure.var_set_type6('TPL_AMT', cond1='88888888.88') }
                , { TAF_Closure.var_set_type6('MDCD_PD_AMT', cond1='888888888.88') }
                , { TAF_Closure.var_set_type6('MDCD_FFS_EQUIV_AMT', cond1='88888888888.80', cond2='888888888.88', cond3='999999.99') }
                , { TAF_Closure.var_set_type6('MDCR_PD_AMT', cond1='888888888.88', cond2='8888888.88', cond3='88888888888.00', cond4='99999999999.00', cond5='88888888888.88', cond6='9999999999.99') }
                , { TAF_Closure.var_set_type6('OTHR_INSRNC_AMT',   cond1='8888888888.00', cond2='888888888.88', cond3='88888888888.88') }
                , { TAF_Closure.var_set_type6('OTHR_TOC_RX_CLM_ACTL_QTY', new='ACTL_SRVC_QTY', cond1='999999.000', cond2='888888.000', cond3='999999.99') }
                , { TAF_Closure.var_set_type6('OTHR_TOC_RX_CLM_ALOWD_QTY', new='ALOWD_SRVC_QTY', cond1='999999.000', cond2='888888.000', cond3='888888.880', cond4='99999.999', cond5='99999') }
                , { TAF_Closure.var_set_tos('TOS_CD') }
                , { TAF_Closure.var_set_type5('BNFT_TYPE_CD', lpad=3, lowerbound='001', upperbound='108') }
                , { TAF_Closure.var_set_type2('HCBS_SRVC_CD', 0, cond1=1, cond2=2, cond3=3, cond4=4, cond5=5, cond6=6, cond7=7, upper=True) }
                , { TAF_Closure.var_set_type1('HCBS_TXNMY', upper=True, lpad=5) }
                , { TAF_Closure.var_set_type1('SRVCNG_PRVDR_NUM') }
                , { TAF_Closure.var_set_type1('PRSCRBNG_PRVDR_NPI_NUM', new='SRVCNG_PRVDR_NPI_NUM') }
                , { TAF_Closure.var_set_taxo('SRVCNG_PRVDR_TXNMY_CD', cond1='8888888888', cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY') }
                , { TAF_Closure.var_set_prtype('SRVCNG_PRVDR_TYPE_CD') }
                , { TAF_Closure.var_set_spclty('SRVCNG_PRVDR_SPCLTY_CD') }
                , { TAF_Closure.var_set_type4('TOOTH_DSGNTN_SYS_CD', 'YES', cond1='JO', cond2='JP') }
                , { TAF_Closure.var_set_type1('TOOTH_NUM') }
                , case when lpad(upper(TOOTH_ORAL_CVTY_AREA_DSGNTD_CD), 2, '0') in ('20', '30', '40') then lpad(upper(TOOTH_ORAL_CVTY_AREA_DSGNTD_CD), 2, '0')
                    else { TAF_Closure.var_set_type5('TOOTH_ORAL_CVTY_AREA_DSGNTD_CD', lpad=2, lowerbound=0, upperbound=10, multiple_condition='YES', upper=True) }
                , { TAF_Closure.var_set_type4('TOOTH_SRFC_CD', 'YES', cond1='B', cond2='D', cond3='F', cond4='I', cond5='L', cond6='M', cond7='O') }
                , { TAF_Closure.var_set_type2('CMS_64_FED_REIMBRSMT_CTGRY_CD', 2, cond1='01', cond2='02', cond3='03', cond4='04') }
                , case when XIX_SRVC_CTGRY_CD in { tuple(TAF_Metadata.XIX_SRVC_CTGRY_CD_values) } then XIX_SRVC_CTGRY_CD
                else NULL end as XIX_SRVC_CTGRY_CD
                , case when XXI_SRVC_CTGRY_CD in { tuple(TAF_Metadata.XXI_SRVC_CTGRY_CD_values) } then XXI_SRVC_CTGRY_CD
                    else NULL end as XXI_SRVC_CTGRY_CD
                , { TAF_Closure.var_set_type1('STATE_NOTN_TXT') }
                , { TAF_Closure.var_set_fills('NDC_CD', cond1='0', cond2='8', cond3='9', cond4='#', spaces=True) }
                , { TAF_Closure.var_set_type1('PRCDR_2_MDFR_CD', upper=True, lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_3_MDFR_CD', upper=True, lpad=2) }
                , { TAF_Closure.var_set_type1('PRCDR_4_MDFR_CD', upper=True, lpad=2) }
                , { TAF_Closure.var_set_type3('HCPCS_RATE', cond1='0.0000000000', cond2='0.000000000000', spaces=False) }
                , { TAF_Closure.var_set_type2('SELF_DRCTN_TYPE_CD', 3, cond1='000', cond2='001', cond3='002', cond4='003') }
                , { TAF_Closure.var_set_type1('PRE_AUTHRZTN_NUM') }
                , { TAF_Closure.var_set_type4('UOM_CD', 'YES', cond1='F2', cond2='ML', cond3='GR', cond4='UN', cond5='ME') }
                , { TAF_Closure.var_set_type6('NDC_QTY', cond1='999999', cond2='999999.998', cond3='888888.000', cond4='888888.880', cond5='88888.888', cond6='888888.888') }
                , from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                , cast(NULL as timestamp) as REC_UPDT_TS
                , RN as LINE_NUM
                , PRCDR_CCS_CTGRY_CD
                , SRVCNG_PRVDR_NPPES_TXNMY_CD
            from (
                select
                    *,
                    case when LINE_ADJSTMT_IND is NOT NULL and
                    trim(LINE_ADJSTMT_IND)  in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(LINE_ADJSTMT_IND) else NULL end as LINE_ADJSTMT_IND_CLEAN
                from
                    OTHR_TOC_LINE
                ) H
            """

        runner.append("OTHR_TOC", z)

    def build(self, runner: OT_Runner):
        """
        Build the OT claim-line level segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_otl
                SELECT
                    { OT_Metadata.finalFormatter(OT_Metadata.line_columns) }
                FROM OTL
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
