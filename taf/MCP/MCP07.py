from taf.MCP import MCP_Runner
from taf.MCP.MCP import MCP
from taf.TAF_Closure import TAF_Closure


class MCP07(MCP):
    """
    Description:  Selection macros for the T-MSIS MC segments
    """

    def __init__(self, mcp: MCP_Runner):
        super().__init__(mcp)

    def process_07_accreditation(self, runtbl, outtbl):
        """
        000-07 Accreditation segment
        """

        # screen out all but the latest run id
        runlist = ["tms_run_id", "submitting_state", "state_plan_id_num"]

        self.screen_runid(
            "tmsis.Managed_care_accreditation_organization",
            runtbl,
            runlist,
            "MC07_Accreditation_Latest1",
            "M",
        )

        # row count
        self.count_rows("MC07_Accreditation_Latest1", "cnt_latest", "MC07_Latest")

        cols07 = [
            "tms_run_id",
            "tms_reporting_period",
            "record_number",
            "submitting_state",
            "submitting_state as submtg_state_cd",
            "%upper_case(state_plan_id_num) as state_plan_id_num",
            "%zero_pad(accreditation_organization, 2)",
            "%fix_old_dates(date_accreditation_achieved)",
            "%set_end_dt(date_accreditation_end) as date_accreditation_end",
        ]

        whr07 = "accreditation_organization is not null"

        self.copy_activerows(
            "MC07_Accreditation_Latest1", cols07, whr07, "MC07_Accreditation_Copy"
        )
        # row count
        self.count_rows("MC07_Accreditation_Copy", "cnt_active", "MC07_Active")

        # screen for eligibility during the month
        keylist = [
            "tms_run_id",
            "submitting_state",
            "state_plan_id_num",
            "accreditation_organization",
        ]

        self.screen_dates(
            "MC07_Accreditation_Copy",
            keylist,
            "date_accreditation_achieved",
            "date_accreditation_end",
            "MC07_Accreditation_Latest2",
        )

        # row count
        self.count_rows("MC07_Accreditation_Latest2", "cnt_date", "MC07_Date")

        # remove duplicate records
        grplist = [
            "tms_run_id",
            "submitting_state",
            "state_plan_id_num",
            "accreditation_organization",
        ]

        self.remove_duprecs(
            "MC07_Accreditation_Latest2",
            grplist,
            "date_accreditation_achieved",
            "date_accreditation_end",
            "accreditation_organization",
            outtbl,
        )

        # row count
        self.count_rows(outtbl, "cnt_final", "MC07_Final")

    def create(self):
        """
        Create the MCP07 accreditation segment.
        """

        self.process_07_accreditation("MC02_Main_RAW", "MC07_Accreditation")

        self.recode_lookup(
            "MC07_Accreditation",
            self.srtlist,
            "mc_formats_sm",
            "ACRVAL",
            "accreditation_organization",
            "ACRDTN_ORG",
            "MC07_Accreditation_val",
            "C",
            2,
        )

        # diststyle key distkey(state_plan_id_num)
        z = f"""
                create or replace temporary view MC07_Accreditation2 as
                select *,
                        date_accreditation_achieved as ACRDTN_ORG_ACHVMT_DT,
                        date_accreditation_end as ACRDTN_ORG_END_DT,
                        row_number() over (
                        partition by { ','.join(self.srtlist) }
                        order by record_number, ACRDTN_ORG asc
                        ) as _ndx
                from MC07_Accreditation_val
                where ACRDTN_ORG is not null
                order by { ','.join(self.srtlist) }
            """

        self.mcp.append(type(self).__name__, z)

        self.recode_lookup(
            "MC07_Accreditation2",
            self.srtlist,
            "mc_formats_sm",
            "ACRNCQA",
            "ACRDTN_ORG",
            "ACRDTN_NCQA",
            "MC07_Accreditation_NCQA",
            "N",
        )

        self.recode_lookup(
            "MC07_Accreditation_NCQA",
            self.srtlist,
            "mc_formats_sm",
            "ACRURAC",
            "ACRDTN_ORG",
            "ACRDTN_URAC",
            "MC07_Accreditation_URAC",
            "N",
        )

        self.recode_lookup(
            "MC07_Accreditation_URAC",
            self.srtlist,
            "mc_formats_sm",
            "ACRAAHC",
            "ACRDTN_ORG",
            "ACRDTN_AAHC",
            "MC07_Accreditation_AAHC",
            "N",
        )

        self.recode_lookup(
            "MC07_Accreditation_AAHC",
            self.srtlist,
            "mc_formats_sm",
            "ACRNONE",
            "ACRDTN_ORG",
            "ACRDTN_NONE",
            "MC07_Accreditation_NONE",
            "N",
        )

        self.recode_lookup(
            "MC07_Accreditation_NONE",
            self.srtlist,
            "mc_formats_sm",
            "ACROTHR",
            "ACRDTN_ORG",
            "ACRDTN_OTHR",
            "MC07_Accreditation_OTHR",
            "N",
        )

        self.recode_lookup(
            "MC07_Accreditation_OTHR",
            self.srtlist,
            "mc_formats_sm",
            "ACRJCAHO",
            "ACRDTN_ORG",
            "ACRDTN_JCAHO",
            "MC07_Accreditation_JCAHO",
            "N",
        )

        # diststyle key distkey(state_plan_id_num)
        z = f"""
                create or replace temporary view MC07_Accreditation_Mapped as
                select { ','.join(self.srtlist) },
                        max(ACRDTN_NCQA) as ACRDTN_NCQA,
                        max(ACRDTN_URAC) as ACRDTN_URAC,
                        max(ACRDTN_AAHC) as ACRDTN_AAHC,
                        max(ACRDTN_NONE) as ACRDTN_NONE,
                        max(ACRDTN_OTHR) as ACRDTN_OTHR,
			            max(ACRDTN_JCAHO) as ACRDTN_JCAHO
                        { MCP.map_arrayvars(varnm='ACRDTN_ORG', N=4, fldtyp='C') }
                        { MCP.map_arrayvars(varnm='ACRDTN_ORG_ACHVMT_DT', N=4, fldtyp='D') }
                        { MCP.map_arrayvars(varnm='ACRDTN_ORG_END_DT', N=4, fldtyp='D') }
                from MC07_Accreditation_JCAHO
                group by { ','.join(self.srtlist) }
            """

        self.mcp.append(type(self).__name__, z)

        # diststyle key distkey(mc_plan_id)
        z = f"""
                create or replace temporary view MC02_Base as
                select M.DA_RUN_ID,
                    M.mcp_link_key,
                    M.MCP_FIL_DT,
                    M.MCP_VRSN,
                    M.tmsis_run_id,
                    M.submtg_state_cd,
                    M.state_plan_id_num as mc_plan_id,
                    M.mc_cntrct_efctv_dt,
                    M.mc_cntrct_end_dt,
                    M.REG_FLAG,
                    M.mc_name,
                    M.mc_pgm_cd,
                    M.mc_plan_type_cd,
                    M.reimbrsmt_arngmt_cd,
                    M.mc_prft_stus_cd,
                    M.cbsa_cd,
                    M.busns_pct,
                    M.mc_sarea_cd,
                    M.mc_plan_type_cat,
                    M.reimbrsmt_arngmt_cat,
                    M.sarea_statewide_ind,
                    O.OPRTG_AUTHRTY_1115_demo_ind,
                    O.OPRTG_AUTHRTY_1915b_ind,
                    O.OPRTG_AUTHRTY_1932a_ind,
                    O.OPRTG_AUTHRTY_1915a_ind,
                    O.OPRTG_AUTHRTY_1915bc_conc_ind,
                    O.OPRTG_AUTHRTY_1915ac_conc_ind,
                    O.OPRTG_AUTHRTY_1932A_1915C_IND,
                    O.OPRTG_AUTHRTY_pace_ind,
                    O.OPRTG_AUTHRTY_1905t_ind,
                    O.OPRTG_AUTHRTY_1937_ind,
                    O.OPRTG_AUTHRTY_1902a70_ind,
                    O.OPRTG_AUTHRTY_1915bi_conc_ind,
                    O.OPRTG_AUTHRTY_1915ai_conc_ind,
                    O.OPRTG_AUTHRTY_1932A_1915I_IND,
                    O.OPRTG_AUTHRTY_1945_HH_ind,
                    O.wvr_id_01,
                    O.wvr_id_02,
                    O.wvr_id_03,
                    O.wvr_id_04,
                    O.wvr_id_05,
                    O.wvr_id_06,
                    O.wvr_id_07,
                    O.wvr_id_08,
                    O.wvr_id_09,
                    O.wvr_id_10,
                    O.wvr_id_11,
                    O.wvr_id_12,
                    O.wvr_id_13,
                    O.wvr_id_14,
                    O.wvr_id_15,
                    O.OPRTG_AUTHRTY_01,
                    O.OPRTG_AUTHRTY_02,
                    O.OPRTG_AUTHRTY_03,
                    O.OPRTG_AUTHRTY_04,
                    O.OPRTG_AUTHRTY_05,
                    O.OPRTG_AUTHRTY_06,
                    O.OPRTG_AUTHRTY_07,
                    O.OPRTG_AUTHRTY_08,
                    O.OPRTG_AUTHRTY_09,
                    O.OPRTG_AUTHRTY_10,
                    O.OPRTG_AUTHRTY_11,
                    O.OPRTG_AUTHRTY_12,
                    O.OPRTG_AUTHRTY_13,
                    O.OPRTG_AUTHRTY_14,
                    O.OPRTG_AUTHRTY_15,
                    E.pop_mdcd_mand_COV_ADLT_IND,
                    E.pop_mdcd_mand_cov_abd_ind,
                    E.pop_mdcd_optn_COV_ADLT_IND,
                    E.pop_mdcd_optn_cov_abd_ind,
                    E.POP_MDCD_MDCLY_NDY_adlt_ind,
                    E.POP_MDCD_MDCLY_NDY_abd_ind,
                    E.pop_chip_cov_CHLDRN_ind,
                    E.pop_chip_optn_CHLDRN_ind,
                    E.pop_chip_optn_prgnt_WMN_ind,
                    E.pop_1115_EXPNSN_ind,
                    E.pop_UNK_ind,
                    A.acrdtn_ncqa,
                    A.acrdtn_urac,
                    A.acrdtn_aahc,
                    A.acrdtn_none,
                    A.acrdtn_othr,
                    A.acrdtn_org_01,
                    A.acrdtn_org_02,
                    A.acrdtn_org_03,
                    A.acrdtn_org_achvmt_dt_01,
                    A.acrdtn_org_achvmt_dt_02,
                    A.acrdtn_org_achvmt_dt_03,
                    A.acrdtn_org_end_dt_01,
                    A.acrdtn_org_end_dt_02,
                    A.acrdtn_org_end_dt_03,
                    O.OPRTG_AUTHRTY_1915AJ_conc_ind,
                    O.OPRTG_AUTHRTY_1932A_1915J_ind,
                    O.OPRTG_AUTHRTY_1915BJ_conc_ind,
                    O.OPRTG_AUTHRTY_1115_1915J_ind,
                    O.OPRTG_AUTHRTY_1915AK_conc_ind,
                    O.OPRTG_AUTHRTY_1932A_1915K_ind,
                    O.OPRTG_AUTHRTY_1915BK_conc_ind,
                    O.OPRTG_AUTHRTY_1115_1915K_ind,
                    A.acrdtn_jcaho
                from MC02_Main_CNST M
                left join MC05_Operating_Authority_Mapped O
                    on { MCP.write_equalkeys(self, keyvars=self.srtlist, t1='M', t2='O') }
                left join MC06_Population_Mapped E
                    on { MCP.write_equalkeys(self, keyvars=self.srtlist, t1='M', t2='E') }
                left join MC07_Accreditation_Mapped A
                    on { MCP.write_equalkeys(self, keyvars=self.srtlist, t1='M', t2='A') }
                order by M.TMSIS_RUN_ID,
                        M.SUBMTG_STATE_CD,
                        mc_plan_id
            """

        self.mcp.append(type(self).__name__, z)

    def build(self, runner: MCP_Runner):
        """
        Build the MCP07 accreditation segment.
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
                            "MC_CNTRCT_EFCTV_DT",
                            "MC_CNTRCT_END_DT",
                            "REG_FLAG",
                            "MC_NAME",
                            "MC_PGM_CD",
                            "MC_PLAN_TYPE_CD",
                            "REIMBRSMT_ARNGMT_CD",
                            "MC_PRFT_STUS_CD",
                            "CBSA_CD",
                            "BUSNS_PCT",
                            "MC_SAREA_CD",
                            "MC_PLAN_TYPE_CAT",
                            "REIMBRSMT_ARNGMT_CAT",
                            "SAREA_STATEWIDE_IND",
                            "OPRTG_AUTHRTY_1115_DEMO_IND",
                            "OPRTG_AUTHRTY_1915B_IND",
                            "OPRTG_AUTHRTY_1932A_IND",
                            "OPRTG_AUTHRTY_1915A_IND",
                            "OPRTG_AUTHRTY_1915BC_CONC_IND",
                            "OPRTG_AUTHRTY_1915AC_CONC_IND",
                            "OPRTG_AUTHRTY_1932A_1915C_IND",
                            "OPRTG_AUTHRTY_PACE_IND",
                            "OPRTG_AUTHRTY_1905T_IND",
                            "OPRTG_AUTHRTY_1937_IND",
                            "OPRTG_AUTHRTY_1902A70_IND",
                            "OPRTG_AUTHRTY_1915BI_CONC_IND",
                            "OPRTG_AUTHRTY_1915AI_CONC_IND",
                            "OPRTG_AUTHRTY_1932A_1915I_IND",
                            "OPRTG_AUTHRTY_1945_HH_IND",
                            "WVR_ID_01",
                            "WVR_ID_02",
                            "WVR_ID_03",
                            "WVR_ID_04",
                            "WVR_ID_05",
                            "WVR_ID_06",
                            "WVR_ID_07",
                            "WVR_ID_08",
                            "WVR_ID_09",
                            "WVR_ID_10",
                            "WVR_ID_11",
                            "WVR_ID_12",
                            "WVR_ID_13",
                            "WVR_ID_14",
                            "WVR_ID_15",
                            "OPRTG_AUTHRTY_01",
                            "OPRTG_AUTHRTY_02",
                            "OPRTG_AUTHRTY_03",
                            "OPRTG_AUTHRTY_04",
                            "OPRTG_AUTHRTY_05",
                            "OPRTG_AUTHRTY_06",
                            "OPRTG_AUTHRTY_07",
                            "OPRTG_AUTHRTY_08",
                            "OPRTG_AUTHRTY_09",
                            "OPRTG_AUTHRTY_10",
                            "OPRTG_AUTHRTY_11",
                            "OPRTG_AUTHRTY_12",
                            "OPRTG_AUTHRTY_13",
                            "OPRTG_AUTHRTY_14",
                            "OPRTG_AUTHRTY_15",
                            "POP_MDCD_MAND_COV_ADLT_IND",
                            "POP_MDCD_MAND_COV_ABD_IND",
                            "POP_MDCD_OPTN_COV_ADLT_IND",
                            "POP_MDCD_OPTN_COV_ABD_IND",
                            "POP_MDCD_MDCLY_NDY_ADLT_IND",
                            "POP_MDCD_MDCLY_NDY_ABD_IND",
                            "POP_CHIP_COV_CHLDRN_IND",
                            "POP_CHIP_OPTN_CHLDRN_IND",
                            "POP_CHIP_OPTN_PRGNT_WMN_IND",
                            "POP_1115_EXPNSN_IND",
                            "POP_UNK_IND",
                            "ACRDTN_NCQA",
                            "ACRDTN_URAC",
                            "ACRDTN_AAHC",
                            "ACRDTN_NONE",
                            "ACRDTN_OTHR",
                            "ACRDTN_ORG_01",
                            "ACRDTN_ORG_02",
                            "ACRDTN_ORG_03",
                            "ACRDTN_ORG_ACHVMT_DT_01",
                            "ACRDTN_ORG_ACHVMT_DT_02",
                            "ACRDTN_ORG_ACHVMT_DT_03",
                            "ACRDTN_ORG_END_DT_01",
                            "ACRDTN_ORG_END_DT_02",
                            "ACRDTN_ORG_END_DT_03",
                            "OPRTG_AUTHRTY_1915AJ_CONC_IND",
                            "OPRTG_AUTHRTY_1932A_1915J_IND",
                            "OPRTG_AUTHRTY_1915BJ_CONC_IND",
                            "OPRTG_AUTHRTY_1115_1915J_IND",
                            "OPRTG_AUTHRTY_1915AK_CONC_IND",
                            "OPRTG_AUTHRTY_1932A_1915K_IND",
                            "OPRTG_AUTHRTY_1915BK_CONC_IND",
                            "OPRTG_AUTHRTY_1115_1915K_IND",
                            "ACRDTN_JCAHO"
                        ]

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_mcp
                SELECT
                    { ', '.join(base_col_list) }
                   ,from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                   ,cast(NULL as timestamp) as REC_UPDT_TS
                FROM
                    MC02_Base
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
