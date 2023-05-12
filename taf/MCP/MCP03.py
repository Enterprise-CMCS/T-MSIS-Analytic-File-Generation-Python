from taf.MCP import MCP_Runner
from taf.MCP.MCP import MCP
from taf.TAF_Closure import TAF_Closure

class MCP03(MCP):
    """
    Description:  Selection Functions for the T-MSIS MC segments.
    """

    def __init__(self, mcp: MCP_Runner):
        super().__init__(mcp)

    def process_03_location(self, runtbl, outtbl):
        """
        000-03 location segment
        """

        # screen out all but the latest run id
        runlist = ["tms_run_id", "submitting_state", "state_plan_id_num"]

        self.screen_runid(
            "tmsis.Managed_care_location_and_contact_info",
            runtbl,
            runlist,
            "MC03_Location_Latest1",
            "M",
        )

        # row count
        self.count_rows("MC03_Location_Latest1", "cnt_latest", "MC03_Latest")

        cols03 = [
            "tms_run_id",
            "tms_reporting_period",
            "record_number",
            "submitting_state",
            "submitting_state as submtg_state_cd",
            "%upper_case(state_plan_id_num) as state_plan_id_num",
            "%upper_case(managed_care_location_id) as managed_care_location_id",
            "%fix_old_dates(managed_care_location_and_contact_info_eff_date)",
            "%set_end_dt(managed_care_location_and_contact_info_end_date) as managed_care_location_and_contact_info_end_date",
            "%upper_case(managed_care_addr_ln1) as managed_care_addr_ln1",
            "managed_care_addr_ln2",
            "managed_care_addr_ln3",
            "managed_care_addr_type",
            "managed_care_city",
            "managed_care_county",
            "%upper_case(managed_care_state) as managed_care_state",
            "managed_care_zip_code",
        ]

        whr03 = "managed_care_addr_type=3"

        self.copy_activerows(
            "MC03_Location_Latest1", cols03, whr03, "MC03_Location_Copy"
        )
        # row count
        self.count_rows("MC03_Location_Copy", "cnt_active", "MC03_Active")

        # screen for eligibility during the month
        keylist = [
            "tms_run_id",
            "submitting_state",
            "state_plan_id_num",
            "managed_care_location_id",
        ]

        self.screen_dates(
            "MC03_Location_Copy",
            keylist,
            "managed_care_location_and_contact_info_eff_date",
            "managed_care_location_and_contact_info_end_date",
            "MC03_Location_Latest2",
        )

        # row count
        self.count_rows("MC03_Location_Latest2", "cnt_date", "MC03_Date")

        # remove duplicate records
        grplist = [
            "tms_run_id",
            "submitting_state",
            "state_plan_id_num",
            "managed_care_location_id",
            "managed_care_addr_ln1",
        ]

        self.remove_duprecs(
            "MC03_Location_Latest2",
            grplist,
            "managed_care_location_and_contact_info_eff_date",
            "managed_care_location_and_contact_info_end_date",
            "managed_care_location_id",
            outtbl,
        )

        # row count
        self.count_rows(outtbl, "cnt_final", "MC03_Final")

    def create(self):
        """
        Create the MCP03 location segment.
        """

        self.process_03_location("MC02_Main_RAW", "MC03_Location")

        self.recode_notnull(
            "MC03_Location",
            self.srtlist,
            "mc_formats_sm",
            "STCDN",
            "managed_care_state",
            "managed_care_state2",
            "MC03_Location_STM",
            "C",
            2,
        )

        self.recode_lookup(
            "MC03_Location_STM",
            self.srtlist,
            "mc_formats_sm",
            "STFIPV",
            "managed_care_state2",
            "MC_STATE_CD",
            "MC03_Location_STV",
            "C",
            2,
        )

        # diststyle key distkey(state_plan_id_num)
        z = f"""
                create or replace temporary view MC03_Location_CNST as
                select
                    {self.mcp.DA_RUN_ID} as DA_RUN_ID,
                    cast (('{self.mcp.VERSION}' || '-' || { self.mcp.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(state_plan_id_num, '*')) as varchar(32)) as MCP_LINK_KEY,
                    '{self.mcp.TAF_FILE_DATE}' as MCP_FIL_DT,
                    '{self.mcp.VERSION}' as MCP_VRSN,
                    tms_run_id as TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    state_plan_id_num as mc_plan_id,
                    managed_care_location_id as MC_LCTN_ID,
                    managed_care_location_and_contact_info_eff_date as MC_LCTN_CNTCT_EFCTV_DT,
                    managed_care_location_and_contact_info_end_date as MC_LCTN_CNTCT_END_DT,
                    managed_care_addr_ln1 as MC_LINE_1_ADR,
                    { TAF_Closure.upper_case('managed_care_addr_ln2') } as MC_LINE_2_ADR,
                    { TAF_Closure.upper_case('managed_care_addr_ln3') } as MC_LINE_3_ADR,
                    { TAF_Closure.upper_case('managed_care_city') } as MC_CITY_NAME,
                    MC_STATE_CD,
                    nullif(trim(managed_care_zip_code),'') as MC_ZIP_CD,
                    nullif(trim(managed_care_county),'') as MC_CNTY_CD
                from MC03_Location_STV
                order by TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    mc_plan_id,
                    MC_LCTN_ID
            """

        self.mcp.append(type(self).__name__, z)

    def build(self, runner: MCP_Runner):
        """
        Build the MCP03 location segment.
        """

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_mcl
                SELECT
                    *
                   ,from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                   ,cast(NULL as timestamp) as REC_UPDT_TS
                FROM
                    MC03_Location_CNST
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
