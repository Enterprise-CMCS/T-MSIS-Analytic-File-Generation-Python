from taf.MCP import MCP_Runner
from taf.MCP.MCP import MCP
from taf.TAF_Closure import TAF_Closure

class MCP04(MCP):
    """
    Description:  Selection Functions for the T-MSIS MC segments
    """

    def __init__(self, mcp: MCP_Runner):
        super().__init__(mcp)

    def process_04_service_area(self, runtbl, outtbl):
        """
        000-04 service_area segment
        """

        # screen out all but the latest run id
        runlist = ["tms_run_id", "submitting_state", "state_plan_id_num"]

        self.screen_runid(
            "tmsis.Tmsis_Mc_Sarea",
            runtbl,
            runlist,
            "MC04_Service_Area_Latest1",
        )

        # row count
        self.count_rows("MC04_Service_Area_Latest1", "cnt_latest", "MC04_Latest")

        cols04 = [
            "tmsis_run_id as tms_run_id",
            "tmsis_rptg_prd as tms_reporting_period",
            "rec_num as record_number",
            "submtg_state_cd as submitting_state",
            "submtg_state_cd",
            f"""{ TAF_Closure.upper_case('state_plan_id_num')} as state_plan_id_num""",
            f"""{ TAF_Closure.upper_case('mc_sarea_name')} as managed_care_service_area_name""",
            f"""{ TAF_Closure.fix_old_dates('mc_sarea_efctv_dt','managed_care_service_area_eff_date')}""",
            f"""{ TAF_Closure.set_end_dt('mc_sarea_end_dt')} as managed_care_service_area_end_date"""
        ]

        whr04 = "upper(mc_sarea_name) is not null"

        self.copy_activerows(
            "MC04_Service_Area_Latest1", cols04, whr04, "MC04_Service_Area_Copy"
        )
        # row count
        self.count_rows("MC04_Service_Area_Copy", "cnt_active", "MC04_Active")

        # screen for eligibility during the month
        keylist = [
            "tms_run_id",
            "submitting_state",
            "state_plan_id_num",
            "managed_care_service_area_name",
        ]

        self.screen_dates(
            "MC04_Service_Area_Copy",
            keylist,
            "managed_care_service_area_eff_date",
            "managed_care_service_area_end_date",
            "MC04_Service_Area_Latest2",
        )

        # row count
        self.count_rows("MC04_Service_Area_Latest2", "cnt_date", "MC04_Date")

        # remove duplicate records
        grplist = [
            "tms_run_id",
            "submitting_state",
            "state_plan_id_num",
            "managed_care_service_area_name",
        ]

        self.remove_duprecs(
            "MC04_Service_Area_Latest2",
            grplist,
            "managed_care_service_area_eff_date",
            "managed_care_service_area_end_date",
            "managed_care_service_area_name",
            outtbl,
        )

        # row count
        self.count_rows(outtbl, "cnt_final", "MC04_Final")

    def create(self):
        """
        Create the MCP04 service area segment.
        """

        self.process_04_service_area("MC02_Main_RAW", "MC04_Service_Area")

        # diststyle key distkey(state_plan_id_num)
        z = f"""
                create or replace temporary view MC04_Service_Area_CNST as
                select
                    {self.mcp.DA_RUN_ID} as DA_RUN_ID,
                    cast (('{self.mcp.VERSION}' || '-' || { self.mcp.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(state_plan_id_num, '*')) as varchar(32)) as MCP_LINK_KEY,
                    '{self.mcp.TAF_FILE_DATE}' as MCP_FIL_DT,
                    '{self.mcp.VERSION}' as MCP_VRSN,
                    tms_run_id as TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    state_plan_id_num as mc_plan_id,
                    managed_care_service_area_eff_date as MC_SAREA_EFCTV_DT,
                    managed_care_service_area_end_date as MC_SAREA_END_DT,
                    managed_care_service_area_name as MC_SAREA_NAME
                from MC04_Service_Area
                order by TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    mc_plan_id
            """

        self.mcp.append(type(self).__name__, z)

    def build(self, runner: MCP_Runner):
        """
        Build the MCP04 service area segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        base_col_list = [
                            "DA_RUN_ID",
                            "MCP_LINK_KEY",
                            "MCP_FIL_DT",
                            "MCP_VRSN",
                            "TMSIS_RUN_ID",
                            "SUBMTG_STATE_CD",
                            "MC_PLAN_ID",
                            "MC_SAREA_EFCTV_DT",
                            "MC_SAREA_END_DT",
                            "MC_SAREA_NAME",
                        ]

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_mcs
                ({', '.join(base_col_list)}, REC_ADD_TS, REC_UPDT_TS)
                SELECT
                    { ', '.join(base_col_list) }
                   ,from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                   ,cast(NULL as timestamp) as REC_UPDT_TS
                FROM
                    MC04_Service_Area_CNST
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
