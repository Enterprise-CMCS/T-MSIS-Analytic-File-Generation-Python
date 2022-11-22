from taf.MCP import MCP_Runner
from taf.MCP.MCP import MCP
from taf.TAF_Closure import TAF_Closure


class MCP02(MCP):
    """
    Description:  Selection Functions for the T-MSIS provider segments
    """
     
    def __init__(self, mcp: MCP_Runner):
        super().__init__(mcp)

    def process_02_mcmain(self, runtbl, outtbl):
        """
        000-02 main segment
        """
         
        # screen out all but the latest run id
        runlist = ["tms_run_id", "submitting_state"]

        self.screen_runid(
            "tmsis.Managed_Care_Main", runtbl, runlist, "MC02_Main_Latest1", "M"
        )

        # row count
        self.count_rows("MC02_Main_Latest1", "cnt_latest", "MC02_Latest")

        cols02 = [
            "tms_run_id",
            "tms_reporting_period",
            "submitting_state",
            "submitting_state as submtg_state_cd",
            "record_number",
            "%upper_case(state_plan_id_num) as state_plan_id_num",
            "managed_care_main_rec_eff_date",
            "managed_care_main_rec_end_date",
            "%fix_old_dates(managed_care_contract_eff_date)",
            "case when managed_care_contract_eff_date is not null and managed_care_contract_end_date is null then to_date('9999-12-31') when to_date('1600-01-01') > managed_care_contract_end_date then to_date('1599-12-31') else managed_care_contract_end_date end as MC_CNTRCT_END_DT",
            "managed_care_name",
            "managed_care_program",
            "managed_care_plan_type",
            "reimbursement_arrangement",
            "managed_care_profit_status",
            "core_based_statistical_area_code",
            "percent_business",
            "managed_care_service_area",
        ]

        whr02 = "upper(state_plan_id_num) is not null"

        self.copy_activerows("MC02_Main_Latest1", cols02, whr02, "MC02_Main_Copy")
        # row count
        self.count_rows("MC02_Main_Copy", "cnt_active", "MC02_Active")

        # screen for eligibility during the month
        keylist = ["tms_run_id", "submitting_state", "state_plan_id_num"]

        self.screen_dates(
            "MC02_Main_Copy",
            keylist,
            "managed_care_main_rec_eff_date",
            "managed_care_main_rec_end_date",
            "MC02_Main_Latest2",
        )

        # row count
        self.count_rows("MC02_Main_Latest2", "cnt_date", "MC02_Date")

        # remove duplicate records
        grplist = ["tms_run_id", "submitting_state", "state_plan_id_num"]

        self.remove_duprecs(
            "MC02_Main_Latest2",
            grplist,
            "managed_care_contract_eff_date",
            "MC_CNTRCT_END_DT",
            "managed_care_name",
            outtbl,
        )

        # row count
        self.count_rows(outtbl, "cnt_final", "MC02_Final")

    def create(self):
        """
        Create the MCP02 main segment.  
        """
         
        self.process_02_mcmain("MC01_Header", "MC02_Main_RAW")

        self.recode_notnull(
            "MC02_Main_RAW",
            self.srtlist,
            "mc_formats_sm",
            "STFIPC",
            "submitting_state",
            "SUBMTG_STATE_rcd",
            "MC02_Main_STV",
            "C",
            2,
        )

        self.recode_lookup(
            "MC02_Main_STV",
            self.srtlist,
            "mc_formats_sm",
            "STFIPN",
            "SUBMTG_STATE_rcd",
            "State",
            "MC02_Main_ST",
            "C",
            7,
        )

        self.recode_lookup(
            "MC02_Main_ST",
            self.srtlist,
            "mc_formats_sm",
            "REGION",
            "State",
            "REG_FLAG",
            "MC02_Main_RG",
            "N",
        )

        self.recode_lookup(
            "MC02_Main_RG",
            self.srtlist,
            "mc_formats_sm",
            "SAREASW",
            "managed_care_service_area",
            "SAREA_STATEWIDE_IND",
            "MC02_Main_SW",
            "N",
        )

        self.recode_lookup(
            "MC02_Main_SW",
            self.srtlist,
            "mc_formats_sm",
            "SAREAV",
            "managed_care_service_area",
            "MC_SAREA_CD",
            "MC02_Main_SA",
            "C",
            1,
        )

        self.recode_lookup(
            "MC02_Main_SA",
            self.srtlist,
            "mc_formats_sm",
            "REIMBV",
            "reimbursement_arrangement",
            "REIMBRSMT_ARNGMT_CD",
            "MC02_Main_RAC",
            "C",
            2,
        )

        self.recode_lookup(
            "MC02_Main_RAC",
            self.srtlist,
            "mc_formats_sm",
            "REIMB",
            "REIMBRSMT_ARNGMT_CD",
            "reimbrsmt_arngmt_CAT",
            "MC02_Main_RA",
            "N",
        )

        self.recode_lookup(
            "MC02_Main_RA",
            self.srtlist,
            "mc_formats_sm",
            "PROFV",
            "managed_care_profit_status",
            "MC_PRFT_STUS_CD",
            "MC02_Main_PSC",
            "C",
            2,
        )

        self.recode_lookup(
            "MC02_Main_PSC",
            self.srtlist,
            "mc_formats_sm",
            "CBSA",
            "core_based_statistical_area_code",
            "CBSA_CD",
            "MC02_Main_CBSA",
            "C",
            1,
        )

        self.recode_lookup(
            "MC02_Main_CBSA",
            self.srtlist,
            "mc_formats_sm",
            "MCPLNTV",
            "managed_care_plan_type",
            "MC_PLAN_TYPE_CD",
            "MC02_Main_PTC",
            "C",
            2,
        )

        self.recode_lookup(
            "MC02_Main_PTC",
            self.srtlist,
            "mc_formats_sm",
            "MCPLNTY",
            "MC_PLAN_TYPE_CD",
            "MC_plan_type_CAT",
            "MC02_Main_PT",
            "N",
        )

        self.recode_lookup(
            "MC02_Main_PT",
            self.srtlist,
            "mc_formats_sm",
            "PGMCDV",
            "managed_care_program",
            "MC_PGM_CD",
            "MC02_Main_PRC",
            "C",
            1,
        )

        # diststyle key distkey(state_plan_id_num)
        z = f"""
                create or replace temporary view MC02_Main_CNST as
                select { ','.join(self.srtlist) },
                    tms_run_id as TMSIS_RUN_ID,
                    '{self.mcp.TAF_FILE_DATE}' as MCP_FIL_DT,
                    '{self.mcp.version}' as MCP_VRSN,
                    {self.mcp.DA_RUN_ID} as DA_RUN_ID,
                    SUBMTG_STATE_CD,
                    managed_care_contract_eff_date as MC_CNTRCT_EFCTV_DT,
                    MC_CNTRCT_END_DT,
                    REG_FLAG,
                    { TAF_Closure.upper_case('managed_care_name') } as MC_NAME,
                    MC_PGM_CD,
                    MC_PLAN_TYPE_CD,
                    REIMBRSMT_ARNGMT_CD,
                    MC_PRFT_STUS_CD,
                    CBSA_CD,
                    percent_business as BUSNS_PCT,
                    MC_SAREA_CD,
                    MC_plan_type_CAT,
                    reimbrsmt_arngmt_CAT,
                    SAREA_STATEWIDE_IND,
                    cast (('{self.mcp.version}' || '-' || { self.mcp.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(state_plan_id_num, '*')) as varchar(32)) as MCP_LINK_KEY
                from MC02_Main_PRC
                order by { ','.join(self.srtlist) }
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
