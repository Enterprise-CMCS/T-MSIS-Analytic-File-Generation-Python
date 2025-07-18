from taf.TAF_Claims import TAF_Claims
from taf.TAF_Grouper import TAF_Grouper
from taf.TAF_Runner import TAF_Runner


class LT_Runner(TAF_Runner):
    """
    The TAF-specific module contains executable statements as well as function definitions to
    generate and execute SQL to produce individual segment as well as final output.
    These statements are intended to initialize the module.
    """

    def __init__(self,
                 da_schema: str,
                 reporting_period: str,
                 state_code: str,
                 run_id: str,
                 job_id: int,
                 file_version: str,
                 run_stats_only: int = 0):

        super().__init__(da_schema,
                         reporting_period,
                         state_code,
                         run_id,
                         job_id,
                         file_version,
                         run_stats_only)

        self.monyrout = self.reporting_period.strftime('%Y%m').upper()
        self.run_stats_only = self.__forceBool__(run_stats_only)

    def init(self):
        """
        Import, create, and build out each segment for a given file type.
        At this point, a dictionary has been created for each file segment containing
        SQL queries that will be sequential executed by the run definition to produce output.
        """

        from taf.LT.LT import LT
        from taf.LT.LTH import LTH
        from taf.LT.LTL import LTL
        from taf.LT.LT_DX import LT_DX
        
        TMSIS_SCHEMA = "tmsis"

        #number of DX codes to be backfilled to the Header file.
        NUMDX = 5

        # -------------------------------------------------
        #   Produces:
        # -------------------------------------------------
        #   1 - TAXO_SWITCHES
        #   2 - NPPES_NPI_STEP2
        #   3 - NPPES_NPI
        #   4 - CCS_PROC
        #   5 - CCS_DX
        # -------------------------------------------------
        grouper = TAF_Grouper(self)
        grouper.fetch_nppes("LT")
        grouper.fetch_ccs("LT")

        # -------------------------------------------------
        #   Produces:
        # -------------------------------------------------
        #   1 - HEADER_LT
        #   2 - HEADER2_LT
        #   3 - NO_DISCHARGE_DATES
        #   4 - CLM_FMLY_LT
        #   5 - COMBINED_HEADER
        #   6 - ALL_HEADER_LT
        #   7 - FA_HDR_LT
        # -------------------------------------------------
        claims = TAF_Claims(self)
        claims.AWS_Claims_Family_Table_Link(
            TMSIS_SCHEMA, "CLT00002", "TMSIS_CLH_REC_LT", "LT", "SRVC_ENDG_DT"
        )

        # -------------------------------------------------
        #   Produces:
        # -------------------------------------------------
        #   1 - LT_DX
        #   2 - DX_WIDE
        # -------------------------------------------------
        lt = LT(self)
        lt.select_dx(TMSIS_SCHEMA, "CLT00004", "tmsis_clm_dx_lt", "LT", "FA_HDR_LT", NUMDX)

        # -------------------------------------------------
        #   Produces:
        # -------------------------------------------------
        #   1 - LT_LINE_IN
        #   2 - LT_LINE
        #   3 - RN_LT
        #   4 - LT_HEADER
        # -------------------------------------------------
        lt.AWS_Extract_Line(TMSIS_SCHEMA, self.DA_SCHEMA, "LT", "LT", "CLT00003", "TMSIS_CLL_REC_LT",NUMDX)

        # -------------------------------------------------
        #   Produces:
        # -------------------------------------------------
        #   1 - LT_HEADER_STEP1
        #   2 - LT_TAXONOMY
        #   3 - LT_HEADER_GROUPER
        # -------------------------------------------------
        grouper = TAF_Grouper(self)
        grouper.AWS_Assign_Grouper_Data_Conv(
            "LT", "LT_HEADER", "LT_LINE", "SRVC_ENDG_DT", False, True, True, True, True
        )

        # -------------------------------------------------
        #   Produces:
        # -------------------------------------------------
        #   - LTH
        #   - TAF_LTH
        # -------------------------------------------------
        LTH().create(self)
        LTL().create(self)
        LT_DX().create(self)

        grouper.fasc_code("LT")

        LTH().build(self)
        LTL().build(self)
        LT_DX().build(self)


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
