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
class BASE(APL):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, apl: APL_Runner):
        super().__init__(apl)
        self.fileseg = "BASE"
        self.basecols = [
            "MC_NAME",
            "MC_PLAN_TYPE_CD",
            "MC_PLAN_TYPE_CAT",
            "MC_CNTRCT_EFCTV_DT",
            "MC_CNTRCT_END_DT",
            "ADDTNL_CNTRCT_PRD_FLAG",
            "MC_PGM_CD",
            "REIMBRSMT_ARNGMT_CD",
            "REIMBRSMT_ARNGMT_CAT",
            "MC_SAREA_CD",
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
            "ACRDTN_ORG_01",
            "ACRDTN_ORG_02",
            "ACRDTN_ORG_03",
            "ACRDTN_ORG_04",
            "ACRDTN_ORG_05",
            "ACRDTN_ORG_ACHVMT_DT_01",
            "ACRDTN_ORG_ACHVMT_DT_02",
            "ACRDTN_ORG_ACHVMT_DT_03",
            "ACRDTN_ORG_ACHVMT_DT_04",
            "ACRDTN_ORG_ACHVMT_DT_05",
            "ACRDTN_ORG_END_DT_01",
            "ACRDTN_ORG_END_DT_02",
            "ACRDTN_ORG_END_DT_03",
            "ACRDTN_ORG_END_DT_04",
            "ACRDTN_ORG_END_DT_05",
            "REG_FLAG",
            "CBSA_CD",
            "MC_PRFT_STUS_CD",
            "BUSNS_PCT",
            "PLAN_ID_FLAG_01",
            "PLAN_ID_FLAG_02",
            "PLAN_ID_FLAG_03",
            "PLAN_ID_FLAG_04",
            "PLAN_ID_FLAG_05",
            "PLAN_ID_FLAG_06",
            "PLAN_ID_FLAG_07",
            "PLAN_ID_FLAG_08",
            "PLAN_ID_FLAG_09",
            "PLAN_ID_FLAG_10",
            "PLAN_ID_FLAG_11",
            "PLAN_ID_FLAG_12",
            "LCTN_SPLMTL",
            "SAREA_SPLMTL",
            "ENRLMT_SPLMTL",
            "OPRTG_AUTHRTY_SPLMTL",
        ]

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        # create the partial base segment
        # pulling in only columns are not accreditation
        subcols = [
            "%last_best(MC_NAME)",
            "%last_best(MC_PLAN_TYPE_CD)",
            "%last_best(MC_PLAN_TYPE_CAT)",
            "%last_best(MC_CNTRCT_END_DT)",
            f"""{ APL.nonmiss_month(self,'MC_CNTRCT_END_DT') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='MC_CNTRCT_EFCTV_DT', nslots='1') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='MC_CNTRCT_END_DT', nslots='1') }""",
            "%last_best(MC_PGM_CD)",
            "%last_best(REIMBRSMT_ARNGMT_CD)",
            "%last_best(REIMBRSMT_ARNGMT_CAT)",
            "%last_best(MC_SAREA_CD)",
            "%ever_year(SAREA_STATEWIDE_IND)",
            "%ever_year(OPRTG_AUTHRTY_1115_DEMO_IND)",
            "%ever_year(OPRTG_AUTHRTY_1915B_IND)",
            "%ever_year(OPRTG_AUTHRTY_1932A_IND)",
            "%ever_year(OPRTG_AUTHRTY_1915A_IND)",
            "%ever_year(OPRTG_AUTHRTY_1915BC_CONC_IND)",
            "%ever_year(OPRTG_AUTHRTY_1915AC_CONC_IND)",
            "%ever_year(OPRTG_AUTHRTY_1932A_1915C_IND)",
            "%ever_year(OPRTG_AUTHRTY_PACE_IND)",
            "%ever_year(OPRTG_AUTHRTY_1905T_IND)",
            "%ever_year(OPRTG_AUTHRTY_1937_IND)",
            "%ever_year(OPRTG_AUTHRTY_1902A70_IND)",
            "%ever_year(OPRTG_AUTHRTY_1915BI_CONC_IND)",
            "%ever_year(OPRTG_AUTHRTY_1915AI_CONC_IND)",
            "%ever_year(OPRTG_AUTHRTY_1932A_1915I_IND)",
            "%ever_year(OPRTG_AUTHRTY_1945_HH_IND)",
            "%ever_year(POP_MDCD_MAND_COV_ADLT_IND)",
            "%ever_year(POP_MDCD_MAND_COV_ABD_IND)",
            "%ever_year(POP_MDCD_OPTN_COV_ADLT_IND)",
            "%ever_year(POP_MDCD_OPTN_COV_ABD_IND)",
            "%ever_year(POP_MDCD_MDCLY_NDY_ADLT_IND)",
            "%ever_year(POP_MDCD_MDCLY_NDY_ABD_IND)",
            "%ever_year(POP_CHIP_COV_CHLDRN_IND)",
            "%ever_year(POP_CHIP_OPTN_CHLDRN_IND)",
            "%ever_year(POP_CHIP_OPTN_PRGNT_WMN_IND)",
            "%ever_year(POP_1115_EXPNSN_IND)",
            "%ever_year(POP_UNK_IND)",
            "%last_best(REG_FLAG)",
            "%last_best(CBSA_CD)",
            "%last_best(MC_PRFT_STUS_CD)",
            "%last_best(BUSNS_PCT)",
        ]

        subcols2 = [
            f"""{ TAF_Closure.monthly_array(self, incol='ACRDTN_ORG_01', nslots='1') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='ACRDTN_ORG_02', nslots='1') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='ACRDTN_ORG_03', nslots='1') }""",
        ]

        subcols3 = [
            f"""{ TAF_Closure.monthly_array(self, incol='ACRDTN_ORG_ACHVMT_DT_01', nslots='1') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='ACRDTN_ORG_ACHVMT_DT_02', nslots='1') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='ACRDTN_ORG_ACHVMT_DT_03', nslots='1') }""",
        ]

        subcols4 = [
            f"""{ TAF_Closure.monthly_array(self, incol='ACRDTN_ORG_END_DT_01', nslots='1') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='ACRDTN_ORG_END_DT_02', nslots='1') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='ACRDTN_ORG_END_DT_03', nslots='1') }""",
            "%any_month(MC_PLAN_ID MC_NAME,PLAN_ID_FLAG,IS NOT NULL)",
        ]

        outercols = [
            f"""{ APL.assign_nonmiss_month(self,'MC_EFF_DT','MC_CNTRCT_END_DT_MN','MC_CNTRCT_EFCTV_DT') }"""
        ]

        subcols_ = map(TAF_Closure.parse, subcols)
        subcols2_ = map(TAF_Closure.parse, subcols2)
        subcols3_ = map(TAF_Closure.parse, subcols3)
        subcols4_ = map(TAF_Closure.parse, subcols4)
        outercols_ = map(TAF_Closure.parse, outercols)

        self.create_temp_table(
            fileseg="MCP",
            tblname="base_pl",
            inyear=self.year,
            subcols=", ".join(subcols_),
            subcols2=", ".join(subcols2_),
            subcols3=", ".join(subcols3_),
            subcols4=", ".join(subcols4_),
            outercols=", ".join(outercols_),
        )

        # create Accreditation0 so that separate records with a unique row for each set
        # of values that occurs in any month can be sorted and loaded into new annual
        # array

        # z = f"""
        #     create or replace table Accreditation0 (
        #         SUBMTG_STATE_CD         VARCHAR(2)
        #        ,MC_PLAN_ID              VARCHAR(12)
        #        ,SPLMTL_SUBMSN_TYPE      VARCHAR(6)
        #        ,ACRDTN_ORG              VARCHAR(2)
        #        ,ACRDTN_ORG_ACHVMT_DT    DATE
        #        ,ACRDTN_ORG_END_DT       DATE
        #     )
        #    """
        # self.apl.append(type(self).__name__, z)

        # Accreditation0
        z = f"""CREATE OR REPLACE TEMPORARY VIEW Accreditation0 AS""" + " "

        for m in range(1, 13):
            mm = "{:02d}".format(m)
            for a in range(1, 4):
                aa = "{:02d}".format(a)

                z += f"""
                    SELECT SUBMTG_STATE_CD
                           ,MC_PLAN_ID
                           ,splmtl_submsn_type
                           ,ACRDTN_ORG_{aa}_{mm} as ACRDTN_ORG
                           ,ACRDTN_ORG_ACHVMT_DT_{aa}_{mm} as ACRDTN_ORG_ACHVMT_DT
                           ,ACRDTN_ORG_END_DT_{aa}_{mm} as ACRDTN_ORG_END_DT
                    FROM base_pl_{self.year}
                    WHERE ACRDTN_ORG_{aa}_{mm} IS NOT NULL
                    """

                if not ((m == 12) and (a == 3)):
                    z += " " + "UNION" + " "

        self.apl.append(type(self).__name__, z)

        # identify unique records for new annual accreditation arrays
        # groups and sorts unique values for annual arrays

        # diststyle key distkey(MC_PLAN_ID) as
        z = f"""
            create or replace temporary view Accreditation1 as
            select SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type,
                    ACRDTN_ORG,
                    ACRDTN_ORG_ACHVMT_DT,
                    ACRDTN_ORG_END_DT
            from Accreditation0
            group by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, ACRDTN_ORG, ACRDTN_ORG_ACHVMT_DT, ACRDTN_ORG_END_DT
            order by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, ACRDTN_ORG, ACRDTN_ORG_ACHVMT_DT, ACRDTN_ORG_END_DT
            """
        self.apl.append(type(self).__name__, z)

        # _ndx identifies record order for new annual accreditation arrays keeps unique
        # values together with the same array index

        # diststyle key distkey(MC_PLAN_ID) as
        z = f"""
            create or replace temporary view Accreditation2 as
            select SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type,
                ACRDTN_ORG,
                ACRDTN_ORG_ACHVMT_DT,
                ACRDTN_ORG_END_DT,
                row_number() over (
                partition by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type
                order by ACRDTN_ORG, ACRDTN_ORG_ACHVMT_DT, ACRDTN_ORG_END_DT
                ) as _ndx
            from Accreditation1
            order by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, ACRDTN_ORG, ACRDTN_ORG_ACHVMT_DT, ACRDTN_ORG_END_DT
            """
        self.apl.append(type(self).__name__, z)

        # create new annual arrays to join to base

        # diststyle key distkey(MC_PLAN_ID) as
        z = f"""
            create or replace temporary view Accreditation_Array as
            select SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type
                    { APL.map_arrayvars(varnm='ACRDTN_ORG', N=6) }
                    { APL.map_arrayvars(varnm='ACRDTN_ORG_ACHVMT_DT', N=6) }
                    { APL.map_arrayvars(varnm='ACRDTN_ORG_END_DT', N=6) }
            from Accreditation2
            group by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type
            """
        self.apl.append(type(self).__name__, z)

        # find effective dates for contract that are continuous in earlier periods
        # with the record that has the last and or best MC_CNTRCT_END_DT

        # contract start date
        # z = f"""
        #     create or replace table taf_python.cntrct_vert_month (
        #             SUBMTG_STATE_CD varchar(2),
        #             MC_PLAN_ID varchar(12),
        #             splmtl_submsn_type varchar(6),
        #             MC_EFF_DT date,
        #             MC_CNTRCT_END_DT date,
        #             mc_mnth_eff_dt date,
        #             mc_mnth_end_dt date
        #     )
        #     """
        # self.apl.append(type(self).__name__, z)

        for m in range(1, 13):
            mm = "{:02d}".format(m)

            z = f"""
                create or replace temporary view cntrct_vert_month_{mm} AS
                select SUBMTG_STATE_CD
                     , MC_PLAN_ID
                     , splmtl_submsn_type
                     , MC_EFF_DT
                     , MC_CNTRCT_END_DT
                     , MC_CNTRCT_EFCTV_DT_{mm} AS mc_mnth_eff_dt
                     , MC_CNTRCT_END_DT_{mm} AS mc_mnth_end_dt
                from base_pl_{self.year}
                where (MC_CNTRCT_EFCTV_DT_{mm} is not null)
                    and (MC_CNTRCT_END_DT_{mm} is not null)
                    and (MC_CNTRCT_EFCTV_DT_{mm} <= MC_CNTRCT_END_DT)
                """
            self.apl.append(type(self).__name__, z)

        z = f"""CREATE OR REPLACE TEMPORARY VIEW cntrct_vert_month AS""" + " "

        for m in range(1, 13):
            mm = "{:02d}".format(m)

            z += f"""SELECT * FROM cntrct_vert_month_{mm}"""

            if not (m == 12):
                z += " " + "UNION" + " "

        self.apl.append(type(self).__name__, z)

        for m in range(1, 13):
            mm = "{:02d}".format(m)

            z = f"""
                create or replace temporary view srtd as
                select
                    SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type,
                    MC_EFF_DT, MC_CNTRCT_END_DT,
                    mc_mnth_eff_dt, mc_mnth_end_dt,
                    case (
                            least	(mc_mnth_eff_dt,
                """

            for m in range(1, 12):
                mm = "{:02d}".format(m)
                z += f"""lag(mc_mnth_eff_dt,{mm}) over (partition by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type order by mc_mnth_end_dt desc, mc_mnth_eff_dt desc)
                     """
                if m < 11:
                    z += f""" , """

            z += f"""
                                    ) <= (lag(mc_mnth_end_dt) over (partition by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type order by mc_mnth_end_dt, mc_mnth_eff_dt))  + 1
                            )
                    when true then 'C'
                    when false then 'A'
                    else 'B'
                    end
                        as cont_rank,
                    least	(mc_mnth_eff_dt,
                """

            for m in range(1, 12):
                mm = "{:02d}".format(m)
                z += f"""lag(mc_mnth_eff_dt,{mm}) over (partition by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type order by mc_mnth_end_dt desc, mc_mnth_eff_dt desc)
                     """
                if m < 11:
                    z += f""" , """

            z += f"""
                                )
                        as new_beginning
                from cntrct_vert_month
                group by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, MC_EFF_DT, MC_CNTRCT_END_DT, mc_mnth_eff_dt, mc_mnth_end_dt
                order by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, mc_mnth_end_dt desc
                """
            self.apl.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view selected_cntrct_dt as
                    select SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, MC_EFF_DT, MC_CNTRCT_END_DT, cont_rank, new_beginning
                    from (select SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, MC_EFF_DT, MC_CNTRCT_END_DT, cont_rank, new_beginning,
                            row_number() over (partition by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type order by MC_EFF_DT, MC_CNTRCT_END_DT, cont_rank, mc_mnth_end_dt desc, mc_mnth_eff_dt desc, new_beginning) as cntrct_dt_rank
                            from srtd order by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, cont_rank
                    )
                    where cntrct_dt_rank = 1
            """
        self.apl.append(type(self).__name__, z)

        # join base_pl selected_cntrct_dt and new accreditation arrays

        # distkey(MC_PLAN_ID)
        # sortkey(SUBMTG_STATE_CD,MC_PLAN_ID,splmtl_submsn_type) as
        z = f"""
            create or replace temporary view base_{self.year} as
                    select a.*
                        ,case
                            when (a.MC_EFF_DT is null or a.MC_CNTRCT_END_DT is null) or (b.new_beginning is null and (a.MC_EFF_DT > a.MC_CNTRCT_END_DT)) then a.MC_EFF_DT
                            else to_date(b.new_beginning) end as MC_CNTRCT_EFCTV_DT
                        ,case
                            when b.cont_rank='A' or ((a.MC_EFF_DT is null or a.MC_CNTRCT_END_DT is null) and b.cont_rank='B') then 1 else 0 end as ADDTNL_CNTRCT_PRD_FLAG
                        ,ACRDTN_ORG_01
                        ,ACRDTN_ORG_02
                        ,ACRDTN_ORG_03
                        ,ACRDTN_ORG_04
                        ,ACRDTN_ORG_05
                        ,ACRDTN_ORG_ACHVMT_DT_01
                        ,ACRDTN_ORG_ACHVMT_DT_02
                        ,ACRDTN_ORG_ACHVMT_DT_03
                        ,ACRDTN_ORG_ACHVMT_DT_04
                        ,ACRDTN_ORG_ACHVMT_DT_05
                        ,ACRDTN_ORG_END_DT_01
                        ,ACRDTN_ORG_END_DT_02
                        ,ACRDTN_ORG_END_DT_03
                        ,ACRDTN_ORG_END_DT_04
                        ,ACRDTN_ORG_END_DT_05
                    from base_pl_{self.year} a
                        left join
                        Accreditation_Array c on a.SUBMTG_STATE_CD = c.SUBMTG_STATE_CD and a.MC_PLAN_ID = c.MC_PLAN_ID and a.splmtl_submsn_type=c.splmtl_submsn_type
                        left join
                        (select SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type, cont_rank, new_beginning
                            from selected_cntrct_dt) b
                            on a.SUBMTG_STATE_CD = b.SUBMTG_STATE_CD and a.MC_PLAN_ID = b.MC_PLAN_ID and a.splmtl_submsn_type=b.splmtl_submsn_type
            """
        self.apl.append(type(self).__name__, z)

        # join to the monthly supplemental flags

        # distkey(MC_PLAN_ID)
        # sortkey(SUBMTG_STATE_CD,MC_PLAN_ID,splmtl_submsn_type) as
        z = f"""
            create or replace temporary view base_{self.year}_final as
                    select a.*
                        ,case when b.LCTN_SPLMTL_CT>0 then 1 else 0 end as LCTN_SPLMTL
                        ,case when c.SAREA_SPLMTL_CT>0 then 1 else 0 end as SAREA_SPLMTL
                        ,case when d.ENRLMT_SPLMTL_CT>0 then 1 else 0 end as ENRLMT_SPLMTL
                        ,case when e.OPRTG_AUTHRTY_SPLMTL_CT>0 then 1 else 0 end as OPRTG_AUTHRTY_SPLMTL

                    from base_{self.year} a
                        left join
                        LCTN_SPLMTL_{self.year} b on a.SUBMTG_STATE_CD = b.SUBMTG_STATE_CD and a.splmtl_submsn_type=b.splmtl_submsn_type and a.MC_PLAN_ID = b.MC_PLAN_ID
                        left join
                        SAREA_SPLMTL_{self.year} c on a.SUBMTG_STATE_CD = c.SUBMTG_STATE_CD and a.splmtl_submsn_type=c.splmtl_submsn_type and a.MC_PLAN_ID = c.MC_PLAN_ID
                        left join
                        ENRLMT_SPLMTL_{self.year} d on a.SUBMTG_STATE_CD = d.SUBMTG_STATE_CD and a.splmtl_submsn_type=d.splmtl_submsn_type and a.MC_PLAN_ID = d.MC_PLAN_ID
                        left join
                        OPRTG_AUTHRTY_SPLMTL_{self.year} e on a.SUBMTG_STATE_CD = e.SUBMTG_STATE_CD and a.splmtl_submsn_type=e.splmtl_submsn_type and a.MC_PLAN_ID = e.MC_PLAN_ID

                        order by SUBMTG_STATE_CD, MC_PLAN_ID, splmtl_submsn_type
            """
        self.apl.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def build(self):
        # insert into permanent table
        z = f"""
            INSERT INTO {self.apl.DA_SCHEMA}.TAF_ANN_PL_BASE
            SELECT
                 {self.table_id_cols()}
                ,{",".join(self.basecols)}
                ,to_timestamp('{self.apl.DA_RUN_ID}', 'yyyyMMddHHmmss') as REC_ADD_TS
                ,current_timestamp() as REC_UPDT_TS
            FROM base_{self.year}_final
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
