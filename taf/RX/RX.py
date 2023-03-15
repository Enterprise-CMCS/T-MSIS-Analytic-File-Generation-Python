from taf.RX.RX_Metadata import RX_Metadata
from taf.RX.RX_Runner import RX_Runner
from taf.TAF import TAF


class RX(TAF):
    """
    Pharmacy Claims (RX) TAF: The RX TAF contains information about claims for drugs or other 
    services provided by a pharmacy. The claims in TAF include fee-for-service claims, managed 
    care encounter claims, service tracking claims, and supplemental payments for Medicaid, Medicaid-expansion CHIP, 
    and Separate CHIP. Inclusion in the RX TAF is based on the month/year of the prescription fill date. 
    The RX TAF are comprised of two files – a claim header-level file and a claim line-level file. 
    The claims included in these files are active, non-voided, non-denied (at the header level), 
    non-duplicate final action claims. Only claim header records meeting these inclusion criteria, 
    along with their associated claim line records, are incorporated. Both files can be linked together 
    using unique keys that are constructed based on various claim header and claim line data elements. 
    The two RX TAF are generated for each calendar month for which data are reported.
    """
     
    def __init__(self, runner: RX_Runner):
        super().__init__(runner)
        self.st_fil_type = "RX"

    def AWS_Extract_Line(self, TMSIS_SCHEMA, DA_SCHEMA, fl2, fl, tab_no, _2x_segment):
        """
        Pull RX line item records (FIRST NON DENIED LINE) and join to final action header records dataset
        """
         
        # Create a temporary line file
        # FIXME: change base table or view when TMSIS changes are made
        z = f"""
            create or replace temporary view {fl2}_LINE_IN as
            select
                SUBMTG_STATE_CD,
                { RX_Metadata.selectDataElements(tab_no, 'a') }

            from
                {DA_SCHEMA}.{_2x_segment}_TEMP_TAF A
            where
                a.TMSIS_ACTV_IND = 1
                and concat(a.submtg_state_cd,a.tmsis_run_id) in ({self.runner.get_combined_list()})
        """
        self.runner.append(self.st_fil_type, z)

        z = f"""
            create or replace temporary view {fl2}_LINE as
            select
                row_number() over (
                    partition by
                        a.SUBMTG_STATE_CD,
                        a.ORGNL_CLM_NUM_LINE,
                        a.ADJSTMT_CLM_NUM_LINE,
                        a.ADJDCTN_DT_LINE,
                        a.LINE_ADJSTMT_IND
                    order by
                        a.SUBMTG_STATE_CD,
                        a.ORGNL_CLM_NUM_LINE,
                        a.ADJSTMT_CLM_NUM_LINE,
                        a.ADJDCTN_DT_LINE,
                        a.LINE_ADJSTMT_IND,
                        a.TMSIS_FIL_NAME,
                        a.REC_NUM
                ) as RN
                ,a.*
                ,a.submtg_state_cd as new_submtg_state_cd_line
                ,CASE
                    WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
                    ELSE SUBSTRING(A.DRUG_UTLZTN_CD,1,2)
                    END AS RSN_SRVC_CD
                ,CASE
                    WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
                    ELSE SUBSTRING(A.DRUG_UTLZTN_CD,3,2)
                    END AS PROF_SRVC_CD
                ,CASE
                    WHEN A.DRUG_UTLZTN_CD IS NULL THEN NULL
                    ELSE SUBSTRING(A.DRUG_UTLZTN_CD,5,2)
                    END AS RSLT_SRVC_CD

            from
                {fl2}_LINE_IN as A

            inner join FA_HDR_{fl} HEADER

            on
                HEADER.TMSIS_RUN_ID = a.TMSIS_RUN_ID_LINE and
                HEADER.SUBMTG_STATE_CD = a.SUBMTG_STATE_CD and
                HEADER.ORGNL_CLM_NUM = a.ORGNL_CLM_NUM_LINE and
                HEADER.ADJSTMT_CLM_NUM = a.ADJSTMT_CLM_NUM_LINE and
                HEADER.ADJDCTN_DT = a.ADJDCTN_DT_LINE and
                HEADER.ADJSTMT_IND = a.LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)

        # Pull out maximum row_number for each partition
        z = f"""
            create or replace temporary view RN_{fl2} as
            select
                NEW_SUBMTG_STATE_CD_LINE
                , ORGNL_CLM_NUM_LINE
                , ADJSTMT_CLM_NUM_LINE
                , ADJDCTN_DT_LINE
                , LINE_ADJSTMT_IND
                , max(RN) as NUM_CLL

            from
                {fl2}_LINE

            group by
                NEW_SUBMTG_STATE_CD_LINE,
                ORGNL_CLM_NUM_LINE,
                ADJSTMT_CLM_NUM_LINE,
                ADJDCTN_DT_LINE,
                LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)

        # Attach num_cll variable to header records as per instruction
        z = f"""
            create or replace temporary view {fl}_HEADER as
            select
                HEADER.*
                , coalesce(RN.NUM_CLL,0) as NUM_CLL

            from
                FA_HDR_{fl} HEADER left join RN_{fl2} RN

                on
                    HEADER.NEW_SUBMTG_STATE_CD = RN.NEW_SUBMTG_STATE_CD_LINE and
                    HEADER.ORGNL_CLM_NUM = RN.ORGNL_CLM_NUM_LINE and
                    HEADER.ADJSTMT_CLM_NUM = RN.ADJSTMT_CLM_NUM_LINE and
                    HEADER.ADJDCTN_DT = RN.ADJDCTN_DT_LINE and
                    HEADER.ADJSTMT_IND = RN.LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)


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
