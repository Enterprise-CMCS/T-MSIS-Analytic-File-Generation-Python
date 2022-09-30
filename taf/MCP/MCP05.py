# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
from taf.MCP import MCP_Runner
from taf.MCP.MCP import MCP
from taf.TAF_Closure import TAF_Closure


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class MCP05(MCP):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, mcp: MCP_Runner):
        super().__init__(mcp)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def process_05_operating_authority(self, runtbl, outtbl):

        # screen out all but the latest run id
        runlist = ["tms_run_id", "submitting_state", "state_plan_id_num"]

        self.screen_runid(
            "tmsis.Managed_care_operating_authority",
            runtbl,
            runlist,
            "MC05_Operating_Authority_Latest1",
        )

        # row count
        self.count_rows("MC05_Operating_Authority_Latest1", "cnt_latest", "MC05_Latest")

        cols05 = [
            "tms_run_id",
            "tms_reporting_period",
            "record_number",
            "submitting_state",
            "submitting_state as submtg_state_cd",
            "%upper_case(state_plan_id_num) as state_plan_id_num",
            """case
                when length(trim(TRAILING FROM operating_authority))<2 and length(trim(TRAILING FROM operating_authority))>0 and operating_authority in ('1','2','3','4','5','6','7','8','9') 
                    then lpad(trim(TRAILING FROM operating_authority),2,'0')
                when trim(TRAILING FROM operating_authority) in ('01','02','03','04','05','06','07','08','09') or 
                    trim(TRAILING FROM operating_authority) in ('10','11','12','13','14','15','16','17','18','19','20','21','22','23')
                    then trim(TRAILING FROM operating_authority)
                else null
            end as operating_authority""",
            "%upper_case(waiver_id) as waiver_id",
            "managed_care_op_authority_eff_date",
            "managed_care_op_authority_end_date",
        ]

        whr05 = "(trim(TRAILING FROM operating_authority) in ('1','2','3','4','5','6','7','8','9') or trim(TRAILING FROM operating_authority) in ('01','02','03','04','05','06','07','08','09') or trim(TRAILING FROM operating_authority) in ('10','11','12','13','14','15','16','17','18','19','20','21','22','23')) or (upper(waiver_id) is not null)"

        self.copy_activerows(
            "MC05_Operating_Authority_Latest1",
            cols05,
            whr05,
            "MC05_Operating_Authority_Copy",
        )
        # row count
        self.count_rows("MC05_Operating_Authority_Copy", "cnt_active", "MC05_Active")

        # screen for eligibility during the month
        keylist = [
            "tms_run_id",
            "submitting_state",
            "state_plan_id_num",
            "waiver_id",
            "operating_authority",
        ]

        self.screen_dates(
            "MC05_Operating_Authority_Copy",
            keylist,
            "managed_care_op_authority_eff_date",
            "managed_care_op_authority_end_date",
            "MC05_Operating_Authority_Latest2",
        )

        # row count
        self.count_rows("MC05_Operating_Authority_Latest2", "cnt_date", "MC05_Date")

        # remove duplicate records
        grplist = [
            "tms_run_id",
            "submitting_state",
            "state_plan_id_num",
            "waiver_id",
            "operating_authority",
        ]

        self.remove_duprecs(
            "MC05_Operating_Authority_Latest2",
            grplist,
            "managed_care_op_authority_eff_date",
            "managed_care_op_authority_end_date",
            "waiver_id",
            outtbl,
        )

        # row count
        self.count_rows(outtbl, "cnt_final", "MC05_Final")

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        self.process_05_operating_authority("MC02_Main_RAW", "MC05_Operating_Authority")

        self.recode_lookup(
            "MC05_Operating_Authority",
            self.srtlist,
            "mc_formats_sm",
            "AUTHV",
            "operating_authority",
            "OPRTG_AUTHRTY_IND",
            "MC05_Operating_Authority_IND",
            "C",
            2,
        )

        # diststyle key distkey(state_plan_id_num)
        z = f"""
                create or replace temporary view MC05_Operating_Authority2 as
                select { ','.join(self.srtlist) },
                    managed_care_op_authority_eff_date as MC_OP_AUTH_EFCTV_DT,
                    managed_care_op_authority_end_date as MC_OP_AUTH_END_DT,
                    case when OPRTG_AUTHRTY_IND='01' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1115_DEMO_IND,
                    case when OPRTG_AUTHRTY_IND='02' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915B_IND,
                    case when OPRTG_AUTHRTY_IND='03' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1932A_IND,
                    case when OPRTG_AUTHRTY_IND='04' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915A_IND,
                    case when OPRTG_AUTHRTY_IND='05' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915BC_CONC_IND,
                    case when OPRTG_AUTHRTY_IND='06' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915AC_CONC_IND,
                    case when OPRTG_AUTHRTY_IND='07' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1932A_1915C_IND,
                    case when OPRTG_AUTHRTY_IND='08' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_PACE_IND,
                    case when OPRTG_AUTHRTY_IND='09' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1905T_IND,
                    case when OPRTG_AUTHRTY_IND='10' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1937_IND,
                    case when OPRTG_AUTHRTY_IND='11' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1902A70_IND,
                    case when OPRTG_AUTHRTY_IND='12' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915BI_CONC_IND,
                    case when OPRTG_AUTHRTY_IND='13' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915AI_CONC_IND,
                    case when OPRTG_AUTHRTY_IND='14' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1932A_1915I_IND,
                    case when OPRTG_AUTHRTY_IND='15' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1945_HH_IND,
                    case when OPRTG_AUTHRTY_IND='16' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915AJ_CONC_IND,
                    case when OPRTG_AUTHRTY_IND='17' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1932A_1915J_IND,
                    case when OPRTG_AUTHRTY_IND='18' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915BJ_CONC_IND,
                    case when OPRTG_AUTHRTY_IND='19' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1115_1915J_IND,
                    case when OPRTG_AUTHRTY_IND='20' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915AK_CONC_IND,
                    case when OPRTG_AUTHRTY_IND='21' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1932A_1915K_IND,
                    case when OPRTG_AUTHRTY_IND='22' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1915BK_CONC_IND,
                    case when OPRTG_AUTHRTY_IND='23' then 1 when OPRTG_AUTHRTY_IND is null then null else 0 end :: smallint as OPRTG_AUTHRTY_1115_1915K_IND,                    
                    OPRTG_AUTHRTY_IND as OPRTG_AUTHRTY,
                    waiver_id as WVR_ID,
                    row_number() over (
                    partition by { ','.join(self.srtlist) }
                    order by record_number, OPRTG_AUTHRTY_IND, waiver_id asc
                    ) as _ndx
                from MC05_Operating_Authority_IND
                where OPRTG_AUTHRTY_IND is not null or waiver_id is not null
                order by { ','.join(self.srtlist) }
            """

        self.mcp.append(type(self).__name__, z)

        # diststyle key distkey(state_plan_id_num)
        z = f"""
                create or replace temporary view MC05_Operating_Authority_Mapped as
                select { ','.join(self.srtlist) },
                        max(OPRTG_AUTHRTY_1115_DEMO_IND) as OPRTG_AUTHRTY_1115_DEMO_IND,
                        max(OPRTG_AUTHRTY_1915B_IND) as OPRTG_AUTHRTY_1915B_IND,
                        max(OPRTG_AUTHRTY_1932A_IND) as OPRTG_AUTHRTY_1932A_IND,
                        max(OPRTG_AUTHRTY_1915A_IND) as OPRTG_AUTHRTY_1915A_IND,
                        max(OPRTG_AUTHRTY_1915BC_CONC_IND) as OPRTG_AUTHRTY_1915BC_CONC_IND,
                        max(OPRTG_AUTHRTY_1915AC_CONC_IND) as OPRTG_AUTHRTY_1915AC_CONC_IND,
                        max(OPRTG_AUTHRTY_1932A_1915C_IND) as OPRTG_AUTHRTY_1932A_1915C_IND,
                        max(OPRTG_AUTHRTY_PACE_IND) as OPRTG_AUTHRTY_PACE_IND,
                        max(OPRTG_AUTHRTY_1905T_IND) as OPRTG_AUTHRTY_1905T_IND,
                        max(OPRTG_AUTHRTY_1937_IND) as OPRTG_AUTHRTY_1937_IND,
                        max(OPRTG_AUTHRTY_1902A70_IND) as OPRTG_AUTHRTY_1902A70_IND,
                        max(OPRTG_AUTHRTY_1915BI_CONC_IND) as OPRTG_AUTHRTY_1915BI_CONC_IND,
                        max(OPRTG_AUTHRTY_1915AI_CONC_IND) as OPRTG_AUTHRTY_1915AI_CONC_IND,
                        max(OPRTG_AUTHRTY_1932A_1915I_IND) as OPRTG_AUTHRTY_1932A_1915I_IND,
                        max(OPRTG_AUTHRTY_1945_HH_IND) as OPRTG_AUTHRTY_1945_HH_IND,
                        max(OPRTG_AUTHRTY_1915AJ_CONC_IND) as OPRTG_AUTHRTY_1915AJ_CONC_IND,
                        max(OPRTG_AUTHRTY_1932A_1915J_IND) as OPRTG_AUTHRTY_1932A_1915J_IND,
                        max(OPRTG_AUTHRTY_1915BJ_CONC_IND) as OPRTG_AUTHRTY_1915BJ_CONC_IND,
                        max(OPRTG_AUTHRTY_1115_1915J_IND) as OPRTG_AUTHRTY_1115_1915J_IND,
                        max(OPRTG_AUTHRTY_1915AK_CONC_IND) as OPRTG_AUTHRTY_1915AK_CONC_IND,
                        max(OPRTG_AUTHRTY_1932A_1915K_IND) as OPRTG_AUTHRTY_1932A_1915K_IND,
                        max(OPRTG_AUTHRTY_1915BK_CONC_IND) as OPRTG_AUTHRTY_1915BK_CONC_IND,
                        max(OPRTG_AUTHRTY_1115_1915K_IND) as OPRTG_AUTHRTY_1115_1915K_IND                        
                        { MCP.map_arrayvars(varnm='WVR_ID', N=17, fldtyp='C') }
                        { MCP.map_arrayvars(varnm='OPRTG_AUTHRTY', N=17, fldtyp='C') }
                from MC05_Operating_Authority2
                group by { ','.join(self.srtlist) }
            """

        self.mcp.append(type(self).__name__, z)


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
# <http://creativecommons.org/publicdomain/zero/1.0/>elg00005
