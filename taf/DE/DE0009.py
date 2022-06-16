from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner
from taf.TAF_Closure import TAF_Closure


class DE0009(DE):
    table_name: str = "disability_need"
    tbl_suffix: str = "dsblty"

    def __init__(self, runner: DE_Runner):
        # TODO: Review this
        DE.__init__(self, runner)

    def create(self):
        super().create()
        self.create_temp()
        self.create_dsblty_suppl_table()

    def create_temp(self):

        # Must use month of latest non-missing LCKIN_PRVDR_NUM1 to pull all three NUMs
        # and TYPES (so must array to then pull in outer query)

        s = f"""{TAF_Closure.last_best('HCBS_AGED_NON_HHCC_FLAG')}
                {TAF_Closure.last_best('HCBS_PHYS_DSBL_NON_HHCC_FLAG')}
                {TAF_Closure.last_best('HCBS_INTEL_DSBL_NON_HHCC_FLAG')}
                {TAF_Closure.last_best('HCBS_AUTSM_NON_HHCC_FLAG')}
                {TAF_Closure.last_best('HCBS_DD_NON_HHCC_FLAG')}
                {TAF_Closure.last_best('HCBS_MI_SED_NON_HHCC_FLAG')}
                {TAF_Closure.last_best('HCBS_BRN_INJ_NON_HHCC_FLAG')}
                {TAF_Closure.last_best('HCBS_HIV_AIDS_NON_HHCC_FLAG')}
                {TAF_Closure.last_best('HCBS_TECH_DEP_MF_NON_HHCC_FLAG', outcol='HCBS_TCH_DP_MF_NON_HHCC_FLAG')}
                {TAF_Closure.last_best('HCBS_DSBL_OTHR_NON_HHCC_FLAG')}
                {TAF_Closure.monthly_array('CARE_LVL_STUS_CD')}
                {TAF_Closure.monthly_array('DFCLTY_CONC_DSBL_FLAG,outcol=DFCLTY_CNCNTRTNG_DSBL_FLAG')}
                {TAF_Closure.monthly_array('DFCLTY_WLKG_DSBL_FLAG')}
                {TAF_Closure.monthly_array('DFCLTY_DRSNG_BATHG_DSBL_FLAG,outcol=DFCLTY_DRSNG_BTH_DSBL_FLAG')}
                {TAF_Closure.monthly_array('DFCLTY_ERRANDS_ALN_DSBL_FLAG,outcol=DFCLTY_ERNDS_ALN_DSBL_FLAG')}
                {TAF_Closure.ever_year('LCKIN_FLAG', outcol='LCKIN_FLAG')}
                {TAF_Closure.last_best('LCKIN_PRVDR_NUM1')}
                {DE.nonmiss_month('LCKIN_PRVDR_NUM1')}
                {TAF_Closure.monthly_array('LCKIN_PRVDR_TYPE_CD1')}
                {TAF_Closure.monthly_array('LCKIN_PRVDR_NUM2')}
                {TAF_Closure.monthly_array('LCKIN_PRVDR_TYPE_CD2')}
                {TAF_Closure.monthly_array('LCKIN_PRVDR_NUM3')}
                {TAF_Closure.monthly_array('LCKIN_PRVDR_TYPE_CD3')}
                {TAF_Closure.last_best('LTSS_PRVDR_NUM1')}
                {DE.nonmiss_month('LTSS_PRVDR_NUM1')}
                {TAF_Closure.monthly_array('LTSS_LVL_CARE_CD1')}
                {TAF_Closure.last_best('LTSS_LVL_CARE_CD1', outcol='LTSS_LVL_CARE_CD1_LTST')}
                {TAF_Closure.monthly_array('LTSS_PRVDR_NUM2')}
                {TAF_Closure.monthly_array('LTSS_LVL_CARE_CD2')}
                {TAF_Closure.last_best('LTSS_LVL_CARE_CD2', outcol='LTSS_LVL_CARE_CD2_LTST')}
                {TAF_Closure.monthly_array('LTSS_PRVDR_NUM3')}
                {TAF_Closure.monthly_array('LTSS_LVL_CARE_CD3')}
                {TAF_Closure.last_best('LTSS_LVL_CARE_CD3', outcol='LTSS_LVL_CARE_CD3_LTST')}
                {TAF_Closure.monthly_array('SSDI_IND')}
                {TAF_Closure.monthly_array('SSI_IND')}'
                {TAF_Closure.monthly_array('SSI_STATE_SPLMT_STUS_CD')}
                {TAF_Closure.monthly_array('SSI_STUS_CD')}
                {TAF_Closure.monthly_array('BIRTH_CNCPTN_IND')}
                {TAF_Closure.monthly_array('TANF_CASH_CD')}
                {TAF_Closure.monthly_array('TPL_INSRNC_CVRG_IND')}
                {TAF_Closure.monthly_array('TPL_OTHR_CVRG_IND')}
                {TAF_Closure.ever_year('CARE_LVL_STUS_CD', condition='is not null')}
                {TAF_Closure.ever_year('LTSS_LVL_CARE_CD1', condition='is not null')}
                {TAF_Closure.ever_year('LTSS_LVL_CARE_CD2', condition='is not null')}
                {TAF_Closure.ever_year('LTSS_LVL_CARE_CD3', condition='is not null')}
                {TAF_Closure.ever_year('DFCLTY_CONC_DSBL_FLAG', outcol='DFCLTY_CNCNTRTNG_DSBL_FLAG_EVR')}
                {TAF_Closure.ever_year('DFCLTY_WLKG_DSBL_FLAG')}
                {TAF_Closure.ever_year('DFCLTY_DRSNG_BATHG_DSBL_FLAG', outcol='DFCLTY_DRSNG_BTH_DSBL_FLAG_EVR')}
                {TAF_Closure.ever_year('DFCLTY_ERRANDS_ALN_DSBL_FLAG', outcol='DFCLTY_ERNDS_ALN_DSBL_FLAG_EVR')}
                {TAF_Closure.ever_year('SSDI_IND')}
                {TAF_Closure.ever_year('SSI_IND')}
                {TAF_Closure.ever_year('BIRTH_CNCPTN_IND')}
                {TAF_Closure.ever_year('TPL_INSRNC_CVRG_IND')}
                {TAF_Closure.ever_year('TPL_OTHR_CVRG_IND')}
                {TAF_Closure.ever_year('SSI_STATE_SPLMT_STUS_CD', usenulls=1, nullcond='000', condition='is not null')}
                {TAF_Closure.ever_year('SSI_STUS_CD', usenulls=1, nullcond='000',condition='is not null')}
                {TAF_Closure.ever_year('TANF_CASH_CD', condition="='2'")}
            """

        # Create HCBS_COND_SPLMTL (which will go onto the base segment AND determines
        # the records that go into the permanent MFP table)

        # Create LCKIN_SPLMTL as LCKIN_FLAG (which will go onto the base segment and be used to determine
        # whether a record will go onto the Disability and Need segment)

        # Create OTHER_NEEDS_SPLMTL (which will go onto the base segment and be used to determine
        # whether a record will go onto the Disability and Need segment)

        os = f"""{DE.assign_nonmiss_month('LCKIN_PRVDR_TYPE_CD1', 'LCKIN_PRVDR_NUM1_MN', 'LCKIN_PRVDR_TYPE_CD1')}
                {DE.assign_nonmiss_month('LCKIN_PRVDR_NUM2', 'LCKIN_PRVDR_NUM1_MN', 'LCKIN_PRVDR_NUM2')}
                {DE.assign_nonmiss_month('LCKIN_PRVDR_TYPE_CD2', 'LCKIN_PRVDR_NUM1_MN', 'LCKIN_PRVDR_TYPE_CD2')}
                {DE.assign_nonmiss_month('LCKIN_PRVDR_NUM3', 'LCKIN_PRVDR_NUM1_MN', 'LCKIN_PRVDR_NUM3')}
                {DE.assign_nonmiss_month('LCKIN_PRVDR_TYPE_CD3', 'LCKIN_PRVDR_NUM1_MN', 'LCKIN_PRVDR_TYPE_CD3')}
                {DE.assign_nonmiss_month('LTSS_PRVDR_NUM2', 'LTSS_PRVDR_NUM1_MN', 'LTSS_PRVDR_NUM2')}
                {DE.assign_nonmiss_month('LTSS_PRVDR_NUM3', 'LTSS_PRVDR_NUM1_MN', 'LTSS_PRVDR_NUM3')}

                {DE.any_col('HCBS_AGED_NON_HHCC_FLAG HCBS_PHYS_DSBL_NON_HHCC_FLAG HCBS_INTEL_DSBL_NON_HHCC_FLAG HCBS_AUTSM_NON_HHCC_FLAG HCBS_DD_NON_HHCC_FLAG HCBS_MI_SED_NON_HHCC_FLAG HCBS_BRN_INJ_NON_HHCC_FLAG HCBS_HIV_AIDS_NON_HHCC_FLAG HCBS_TCH_DP_MF_NON_HHCC_FLAG HCBS_DSBL_OTHR_NON_HHCC_FLAG', 'HCBS_COND_SPLMTL')}

                ,LCKIN_FLAG as LCKIN_SPLMTL

                {DE.any_col('DFCLTY_CNCNTRTNG_DSBL_FLAG_EVR DFCLTY_WLKG_DSBL_FLAG_EVR DFCLTY_DRSNG_BTH_DSBL_FLAG_EVR DFCLTY_ERNDS_ALN_DSBL_FLAG_EVR SSDI_IND_EVR SSI_IND_EVR BIRTH_CNCPTN_IND_EVR TPL_INSRNC_CVRG_IND_EVR TPL_OTHR_CVRG_IND_EVR SSI_STATE_SPLMT_STUS_CD_EVR SSI_STUS_CD_EVR TANF_CASH_CD_EVR',
                        outcol='OTHER_NEEDS_SPLMTL')}
            """
        DE.create_temp_table(tblname=self.table_name, subcols=s, outercols=os)

        z = f"""create or replace temporary view disability_need_{self.de.YEAR}2 as

                select *
                    ,case when CARE_LVL_STUS_CD_EVR=1 or
                    LTSS_LVL_CARE_CD1_EVR=1 or
                    LTSS_LVL_CARE_CD2_EVR=1 or
                    LTSS_LVL_CARE_CD3_EVR=1 or
                    LTSS_PRVDR_NUM1 is not null or
                    LTSS_PRVDR_NUM2 is not null or
                    LTSS_PRVDR_NUM3 is not null
                    then 1 else 0
                    end as LTSS_SPLMTL

                from disability_need_{self.de.YEAR}"""

        self.de.append(type(self).__name__, z)

        return

    def create_dsblty_suppl_table(self):
        z = f"""create or replace temporary view DIS_NEED_SPLMTLS_{self.de.YEAR} as
        select submtg_state_cd
                ,msis_ident_num
                ,HCBS_COND_SPLMTL
                ,LCKIN_SPLMTL
                ,LTSS_SPLMTL
                ,OTHER_NEEDS_SPLMTL

        from disability_need_{self.de.YEAR}2"""

        self.de.append(type(self).__name__, z)

        z = f"""insert into {self.de.DA_SCHEMA}.TAF_ANN_DE_{self.tbl_suffix}
                select

                    {DE.table_id_cols_sfx}
                    ,HCBS_AGED_NON_HHCC_FLAG
                    ,HCBS_PHYS_DSBL_NON_HHCC_FLAG
                    ,HCBS_INTEL_DSBL_NON_HHCC_FLAG
                    ,HCBS_AUTSM_NON_HHCC_FLAG
                    ,HCBS_DD_NON_HHCC_FLAG
                    ,HCBS_MI_SED_NON_HHCC_FLAG
                    ,HCBS_BRN_INJ_NON_HHCC_FLAG
                    ,HCBS_HIV_AIDS_NON_HHCC_FLAG
                    ,HCBS_TCH_DP_MF_NON_HHCC_FLAG
                    ,HCBS_DSBL_OTHR_NON_HHCC_FLAG
                    ,CARE_LVL_STUS_CD_01
                    ,CARE_LVL_STUS_CD_02
                    ,CARE_LVL_STUS_CD_03
                    ,CARE_LVL_STUS_CD_04
                    ,CARE_LVL_STUS_CD_05
                    ,CARE_LVL_STUS_CD_06
                    ,CARE_LVL_STUS_CD_07
                    ,CARE_LVL_STUS_CD_08
                    ,CARE_LVL_STUS_CD_09
                    ,CARE_LVL_STUS_CD_10
                    ,CARE_LVL_STUS_CD_11
                    ,CARE_LVL_STUS_CD_12
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_01
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_02
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_03
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_04
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_05
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_06
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_07
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_08
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_09
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_10
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_11
                    ,DFCLTY_CNCNTRTNG_DSBL_FLAG_12
                    ,DFCLTY_WLKG_DSBL_FLAG_01
                    ,DFCLTY_WLKG_DSBL_FLAG_02
                    ,DFCLTY_WLKG_DSBL_FLAG_03
                    ,DFCLTY_WLKG_DSBL_FLAG_04
                    ,DFCLTY_WLKG_DSBL_FLAG_05
                    ,DFCLTY_WLKG_DSBL_FLAG_06
                    ,DFCLTY_WLKG_DSBL_FLAG_07
                    ,DFCLTY_WLKG_DSBL_FLAG_08
                    ,DFCLTY_WLKG_DSBL_FLAG_09
                    ,DFCLTY_WLKG_DSBL_FLAG_10
                    ,DFCLTY_WLKG_DSBL_FLAG_11
                    ,DFCLTY_WLKG_DSBL_FLAG_12
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_01
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_02
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_03
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_04
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_05
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_06
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_07
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_08
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_09
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_10
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_11
                    ,DFCLTY_DRSNG_BTH_DSBL_FLAG_12
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_01
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_02
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_03
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_04
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_05
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_06
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_07
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_08
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_09
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_10
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_11
                    ,DFCLTY_ERNDS_ALN_DSBL_FLAG_12
                    ,LCKIN_FLAG
                    ,LCKIN_PRVDR_NUM1
                    ,LCKIN_PRVDR_TYPE_CD1
                    ,LCKIN_PRVDR_NUM2
                    ,LCKIN_PRVDR_TYPE_CD2
                    ,LCKIN_PRVDR_NUM3
                    ,LCKIN_PRVDR_TYPE_CD3
                    ,LTSS_PRVDR_NUM1
                    ,LTSS_LVL_CARE_CD1_01
                    ,LTSS_LVL_CARE_CD1_02
                    ,LTSS_LVL_CARE_CD1_03
                    ,LTSS_LVL_CARE_CD1_04
                    ,LTSS_LVL_CARE_CD1_05
                    ,LTSS_LVL_CARE_CD1_06
                    ,LTSS_LVL_CARE_CD1_07
                    ,LTSS_LVL_CARE_CD1_08
                    ,LTSS_LVL_CARE_CD1_09
                    ,LTSS_LVL_CARE_CD1_10
                    ,LTSS_LVL_CARE_CD1_11
                    ,LTSS_LVL_CARE_CD1_12
                    ,LTSS_LVL_CARE_CD1_LTST
                    ,LTSS_PRVDR_NUM2
                    ,LTSS_LVL_CARE_CD2_01
                    ,LTSS_LVL_CARE_CD2_02
                    ,LTSS_LVL_CARE_CD2_03
                    ,LTSS_LVL_CARE_CD2_04
                    ,LTSS_LVL_CARE_CD2_05
                    ,LTSS_LVL_CARE_CD2_06
                    ,LTSS_LVL_CARE_CD2_07
                    ,LTSS_LVL_CARE_CD2_08
                    ,LTSS_LVL_CARE_CD2_09
                    ,LTSS_LVL_CARE_CD2_10
                    ,LTSS_LVL_CARE_CD2_11
                    ,LTSS_LVL_CARE_CD2_12
                    ,LTSS_LVL_CARE_CD2_LTST
                    ,LTSS_PRVDR_NUM3
                    ,LTSS_LVL_CARE_CD3_01
                    ,LTSS_LVL_CARE_CD3_02
                    ,LTSS_LVL_CARE_CD3_03
                    ,LTSS_LVL_CARE_CD3_04
                    ,LTSS_LVL_CARE_CD3_05
                    ,LTSS_LVL_CARE_CD3_06
                    ,LTSS_LVL_CARE_CD3_07
                    ,LTSS_LVL_CARE_CD3_08
                    ,LTSS_LVL_CARE_CD3_09
                    ,LTSS_LVL_CARE_CD3_10
                    ,LTSS_LVL_CARE_CD3_11
                    ,LTSS_LVL_CARE_CD3_12
                    ,LTSS_LVL_CARE_CD3_LTST
                    ,SSDI_IND_01
                    ,SSDI_IND_02
                    ,SSDI_IND_03
                    ,SSDI_IND_04
                    ,SSDI_IND_05
                    ,SSDI_IND_06
                    ,SSDI_IND_07
                    ,SSDI_IND_08
                    ,SSDI_IND_09
                    ,SSDI_IND_10
                    ,SSDI_IND_11
                    ,SSDI_IND_12
                    ,SSI_IND_01
                    ,SSI_IND_02
                    ,SSI_IND_03
                    ,SSI_IND_04
                    ,SSI_IND_05
                    ,SSI_IND_06
                    ,SSI_IND_07
                    ,SSI_IND_08
                    ,SSI_IND_09
                    ,SSI_IND_10
                    ,SSI_IND_11
                    ,SSI_IND_12
                    ,SSI_STATE_SPLMT_STUS_CD_01
                    ,SSI_STATE_SPLMT_STUS_CD_02
                    ,SSI_STATE_SPLMT_STUS_CD_03
                    ,SSI_STATE_SPLMT_STUS_CD_04
                    ,SSI_STATE_SPLMT_STUS_CD_05
                    ,SSI_STATE_SPLMT_STUS_CD_06
                    ,SSI_STATE_SPLMT_STUS_CD_07
                    ,SSI_STATE_SPLMT_STUS_CD_08
                    ,SSI_STATE_SPLMT_STUS_CD_09
                    ,SSI_STATE_SPLMT_STUS_CD_10
                    ,SSI_STATE_SPLMT_STUS_CD_11
                    ,SSI_STATE_SPLMT_STUS_CD_12
                    ,SSI_STUS_CD_01
                    ,SSI_STUS_CD_02
                    ,SSI_STUS_CD_03
                    ,SSI_STUS_CD_04
                    ,SSI_STUS_CD_05
                    ,SSI_STUS_CD_06
                    ,SSI_STUS_CD_07
                    ,SSI_STUS_CD_08
                    ,SSI_STUS_CD_09
                    ,SSI_STUS_CD_10
                    ,SSI_STUS_CD_11
                    ,SSI_STUS_CD_12
                    ,BIRTH_CNCPTN_IND_01
                    ,BIRTH_CNCPTN_IND_02
                    ,BIRTH_CNCPTN_IND_03
                    ,BIRTH_CNCPTN_IND_04
                    ,BIRTH_CNCPTN_IND_05
                    ,BIRTH_CNCPTN_IND_06
                    ,BIRTH_CNCPTN_IND_07
                    ,BIRTH_CNCPTN_IND_08
                    ,BIRTH_CNCPTN_IND_09
                    ,BIRTH_CNCPTN_IND_10
                    ,BIRTH_CNCPTN_IND_11
                    ,BIRTH_CNCPTN_IND_12
                    ,TANF_CASH_CD_01
                    ,TANF_CASH_CD_02
                    ,TANF_CASH_CD_03
                    ,TANF_CASH_CD_04
                    ,TANF_CASH_CD_05
                    ,TANF_CASH_CD_06
                    ,TANF_CASH_CD_07
                    ,TANF_CASH_CD_08
                    ,TANF_CASH_CD_09
                    ,TANF_CASH_CD_10
                    ,TANF_CASH_CD_11
                    ,TANF_CASH_CD_12
                    ,TPL_INSRNC_CVRG_IND_01
                    ,TPL_INSRNC_CVRG_IND_02
                    ,TPL_INSRNC_CVRG_IND_03
                    ,TPL_INSRNC_CVRG_IND_04
                    ,TPL_INSRNC_CVRG_IND_05
                    ,TPL_INSRNC_CVRG_IND_06
                    ,TPL_INSRNC_CVRG_IND_07
                    ,TPL_INSRNC_CVRG_IND_08
                    ,TPL_INSRNC_CVRG_IND_09
                    ,TPL_INSRNC_CVRG_IND_10
                    ,TPL_INSRNC_CVRG_IND_11
                    ,TPL_INSRNC_CVRG_IND_12
                    ,TPL_OTHR_CVRG_IND_01
                    ,TPL_OTHR_CVRG_IND_02
                    ,TPL_OTHR_CVRG_IND_03
                    ,TPL_OTHR_CVRG_IND_04
                    ,TPL_OTHR_CVRG_IND_05
                    ,TPL_OTHR_CVRG_IND_06
                    ,TPL_OTHR_CVRG_IND_07
                    ,TPL_OTHR_CVRG_IND_08
                    ,TPL_OTHR_CVRG_IND_09
                    ,TPL_OTHR_CVRG_IND_10
                    ,TPL_OTHR_CVRG_IND_11
                    ,TPL_OTHR_CVRG_IND_12

                    from disability_need_{self.de.YEAR}2
                    where HCBS_COND_SPLMTL=1 or
                        LCKIN_SPLMTL=1 or
                        LTSS_SPLMTL=1 or
                        OTHER_NEEDS_SPLMTL=1
            """

        self.de.append(type(self).__name__, z)
        return

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
