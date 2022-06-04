# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
from taf.APL.APL import APL
from taf.APL.APL_Runner import APL_Runner
from taf.TAF_Closure import TAF_Closure


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class OA(APL):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, apl: APL_Runner):
        super().__init__(apl)
        self.fileseg = "OPRTG_AUTHRTY"

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        subcols = [
            "%monthly_array(WVR_ID_01,WVR_ID_01,1)",
            "%monthly_array(WVR_ID_02,WVR_ID_02,1)",
            "%monthly_array(WVR_ID_03,WVR_ID_03,1)",
            "%monthly_array(WVR_ID_04,WVR_ID_04,1)"
        ]

        subcols2 = [
            "%monthly_array(WVR_ID_05,WVR_ID_05,1)",
            "%monthly_array(WVR_ID_06,WVR_ID_06,1)",
            "%monthly_array(WVR_ID_07,WVR_ID_07,1)",
            "%monthly_array(WVR_ID_08,WVR_ID_08,1)",
            "%monthly_array(WVR_ID_09,WVR_ID_09,1)"
        ]

        subcols3 = [
            "%monthly_array(WVR_ID_10,WVR_ID_10,1)",
            "%monthly_array(WVR_ID_11,WVR_ID_11,1)",
            "%monthly_array(WVR_ID_12,WVR_ID_12,1)",
            "%monthly_array(WVR_ID_13,WVR_ID_13,1)",
            "%monthly_array(WVR_ID_14,WVR_ID_14,1)",
            "%monthly_array(WVR_ID_15,WVR_ID_15,1)"
        ]

        subcols4 = [
            "%monthly_array(OPRTG_AUTHRTY_01,OPRTG_AUTHRTY_01,1)",
            "%monthly_array(OPRTG_AUTHRTY_02,OPRTG_AUTHRTY_02,1)",
            "%monthly_array(OPRTG_AUTHRTY_03,OPRTG_AUTHRTY_03,1)",
            "%monthly_array(OPRTG_AUTHRTY_04,OPRTG_AUTHRTY_04,1)",
            "%monthly_array(OPRTG_AUTHRTY_05,OPRTG_AUTHRTY_05,1)",
            "%monthly_array(OPRTG_AUTHRTY_06,OPRTG_AUTHRTY_06,1)",
            "%monthly_array(OPRTG_AUTHRTY_07,OPRTG_AUTHRTY_07,1)"
        ]

        subcols5 = [
            "%monthly_array(OPRTG_AUTHRTY_08,OPRTG_AUTHRTY_08,1)",
            "%monthly_array(OPRTG_AUTHRTY_09,OPRTG_AUTHRTY_09,1)",
            "%monthly_array(OPRTG_AUTHRTY_10,OPRTG_AUTHRTY_10,1)",
            "%monthly_array(OPRTG_AUTHRTY_11,OPRTG_AUTHRTY_11,1)",
            "%monthly_array(OPRTG_AUTHRTY_12,OPRTG_AUTHRTY_12,1)",
            "%monthly_array(OPRTG_AUTHRTY_13,OPRTG_AUTHRTY_13,1)",
            "%monthly_array(OPRTG_AUTHRTY_14,OPRTG_AUTHRTY_14,1)",
            "%monthly_array(OPRTG_AUTHRTY_15,OPRTG_AUTHRTY_15,1)"
        ]

        subcols_ = map(TAF_Closure.parse, subcols)
        subcols2_ = map(TAF_Closure.parse, subcols2)
        subcols3_ = map(TAF_Closure.parse, subcols3)
        subcols4_ = map(TAF_Closure.parse, subcols4)
        subcols5_ = map(TAF_Closure.parse, subcols5)

        self.create_temp_table(
            fileseg="MCP",
            tblname="oa_pl",
            inyear=self.year,
            subcols=', '.join(subcols_),
            subcols2=', '.join(subcols2_),
            subcols3=', '.join(subcols3_),
            subcols4=', '.join(subcols4_),
            subcols5=', '.join(subcols5_)
        )

        # OpAuth0
        z = f"""CREATE OR REPLACE TEMPORARY VIEW OpAuth0 AS""" + " "

        for m in range(1, 13):
            mm = "{:02d}".format(m)
            for a in range(1, 16):
                aa = "{:02d}".format(a)

                z += f"""
                     SELECT SUBMTG_STATE_CD
                        ,MC_PLAN_ID
                        ,SPLMTL_SUBMSN_TYPE
                        ,WVR_ID_{aa}_{mm} AS WVR_ID
                        ,OPRTG_AUTHRTY_{aa}_{mm} AS OPRTG_AUTHRTY
                        ,CASE
                           WHEN {mm} = 1 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_01
                        ,CASE
                           WHEN {mm} = 2 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_02
                        ,CASE
                           WHEN {mm} = 3 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_03
                        ,CASE
                           WHEN {mm} = 4 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_04
                        ,CASE
                           WHEN {mm} = 5 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_05
                        ,CASE
                           WHEN {mm} = 6 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_06
                        ,CASE
                           WHEN {mm} = 7 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_07
                        ,CASE
                           WHEN {mm} = 8 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_08
                        ,CASE
                           WHEN {mm} = 9 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_09
                        ,CASE
                           WHEN {mm} = 10 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_10
                        ,CASE
                           WHEN {mm} = 11 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_11
                        ,CASE
                           WHEN {mm} = 12 THEN 1 ELSE cast(NULL AS INTEGER)
                         END AS OPRTG_AUTHRTY_FLAG_12
                     FROM oa_pl_{self.year}
                     WHERE WVR_ID_{aa}_{mm} IS NOT NULL
                        OR OPRTG_AUTHRTY_{aa}_{mm} IS NOT NULL
                """

                if not ((m == 12) and (a == 15)):
                    z += " " + "UNION" + " "

        self.apl.append(type(self).__name__, z)

        # diststyle key distkey(MC_PLAN_ID)
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW OpAuth1 AS
            select SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, WVR_ID, OPRTG_AUTHRTY
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_01,0)) as OPRTG_AUTHRTY_FLAG_01
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_02,0)) as OPRTG_AUTHRTY_FLAG_02
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_03,0)) as OPRTG_AUTHRTY_FLAG_03
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_04,0)) as OPRTG_AUTHRTY_FLAG_04
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_05,0)) as OPRTG_AUTHRTY_FLAG_05
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_06,0)) as OPRTG_AUTHRTY_FLAG_06
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_07,0)) as OPRTG_AUTHRTY_FLAG_07
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_08,0)) as OPRTG_AUTHRTY_FLAG_08
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_09,0)) as OPRTG_AUTHRTY_FLAG_09
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_10,0)) as OPRTG_AUTHRTY_FLAG_10
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_11,0)) as OPRTG_AUTHRTY_FLAG_11
                        ,max(coalesce(OPRTG_AUTHRTY_FLAG_12,0)) as OPRTG_AUTHRTY_FLAG_12
            from OpAuth0
            group by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, WVR_ID, OPRTG_AUTHRTY
            order by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, WVR_ID, OPRTG_AUTHRTY"""
        self.apl.append(type(self).__name__, z)

        # create temp table with just OPRTG_AUTHRTY_SPLMTL to join to base
        self.create_splmlt(segname="OPRTG_AUTHRTY", segfile="OpAuth1")

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def build(self):
        # insert into permanent table
        z = f"""
            CREATE TABLE {self.apl.DA_SCHEMA}.TAF_ANN_PL_OA AS
            SELECT
                 {self.table_id_cols()}
                ,WVR_ID
                ,OPRTG_AUTHRTY
                ,OPRTG_AUTHRTY_FLAG_01
                ,OPRTG_AUTHRTY_FLAG_02
                ,OPRTG_AUTHRTY_FLAG_03
                ,OPRTG_AUTHRTY_FLAG_04
                ,OPRTG_AUTHRTY_FLAG_05
                ,OPRTG_AUTHRTY_FLAG_06
                ,OPRTG_AUTHRTY_FLAG_07
                ,OPRTG_AUTHRTY_FLAG_08
                ,OPRTG_AUTHRTY_FLAG_09
                ,OPRTG_AUTHRTY_FLAG_10
                ,OPRTG_AUTHRTY_FLAG_11
                ,OPRTG_AUTHRTY_FLAG_12
            from OpAuth1
            """
        self.apl.append(type(self).__name__, z)


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
