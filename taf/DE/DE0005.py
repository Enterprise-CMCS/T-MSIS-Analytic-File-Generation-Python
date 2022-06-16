from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner
from taf.TAF_Closure import TAF_Closure


class DE0005(DE):
    tblname: str = "managed_care"
    tbl_suffix: str = "mc"

    def __init__(self, runner: DE_Runner):
        # TODO: Review this
        DE.__init__(self, runner)

    def create(self):
        super().create()
        self.create_temp()
        self.create_mc_suppl_table()

    def create_temp(self):
        s = f"""{DE.run_mc_slots(self, 1, 3)}
                {TAF_Closure.monthly_array(self, incol='MC_PLAN_ID', nslots=self.de.NMCSLOTS)}
                {TAF_Closure.monthly_array(self, incol='MC_PLAN_TYPE_CD', nslots=self.de.NMCSLOTS)}"""
        s2 = f"""{DE.run_mc_slots(self, 4, 6)}"""
        s3 = f"""{DE.run_mc_slots(self, 7, 9)}"""
        s4 = f"""{DE.run_mc_slots(self, 10, 12)}"""
        s5 = f"""{DE.mc_nonnull_zero(self, 'MNGD_CARE_SPLMTL', 1, 3)}"""
        s6 = f"""{DE.mc_nonnull_zero(self, 'MNGD_CARE_SPLMTL', 4, 6)}"""
        s7 = f"""{DE.mc_nonnull_zero(self, 'MNGD_CARE_SPLMTL', 7, 9)}"""
        s7 = f"""{DE.mc_nonnull_zero(self, 'MNGD_CARE_SPLMTL', 10, 12)}"""
        os = f""" {DE.sum_months(self, 'CMPRHNSV_MC_PLAN')}
                  {DE.sum_months(self, 'TRDTNL_PCCM_MC_PLAN')}
                  {DE.sum_months(self, 'ENHNCD_PCCM_MC_PLAN')}
                  {DE.sum_months(self, 'HIO_MC_PLAN')}
                  {DE.sum_months(self, 'PIHP_MC_PLAN')}
                  {DE.sum_months(self, 'PAHP_MC_PLAN')}
                  {DE.sum_months(self, 'LTC_PIHP_MC_PLAN')}
                  {DE.sum_months(self, 'MH_PIHP_MC_PLAN')}
                  {DE.sum_months(self, 'MH_PAHP_MC_PLAN')}
                  {DE.sum_months(self, 'SUD_PIHP_MC_PLAN')}
                  {DE.sum_months(self, 'SUD_PAHP_MC_PLAN')}
                  {DE.sum_months(self, 'MH_SUD_PIHP_MC_PLAN')}
                  {DE.sum_months(self, 'MH_SUD_PAHP_MC_PLAN')}
                  {DE.sum_months(self, 'DNTL_PAHP_MC_PLAN')}
                  {DE.sum_months(self, 'TRANSPRTN_PAHP_MC_PLAN')}
                  {DE.sum_months(self, 'DEASE_MGMT_MC_PLAN')}
                  {DE.sum_months(self, 'PACE_MC_PLAN')}
                  {DE.sum_months(self, 'PHRMCY_PAHP_MC_PLAN')}
                  {DE.sum_months(self, 'ACNTBL_MC_PLAN')}
                  {DE.sum_months(self, 'HM_HOME_MC_PLAN')}
                  {DE.sum_months(self, 'IC_DUALS_MC_PLAN')}
            """
        print(s)
        DE.create_temp_table(self, tblname=self.tblname, inyear=self.de.YEAR, subcols=s, subcols2=s2, subcols3=s3,
                             subcols4=s4, subcols5=s5, subcols6=s6, subcols7=s7, outercols=os)
        return

    def create_mc_suppl_table(self):
        z = f"""create or replace temporary view MNGD_CARE_SPLMTL_{self.de.YEAR} as
        select submtg_state_cd
                ,msis_ident_num
                ,case when MNGD_CARE_SPLMTL_1_3=1 or MNGD_CARE_SPLMTL_4_6=1 or
                            MNGD_CARE_SPLMTL_7_9=1 or MNGD_CARE_SPLMTL_10_12=1
                        then 1 else 0 end
                        as MNGD_CARE_SPLMTL

        from managed_care_{self.de.YEAR}"""

        self.de.append(type(self).__name__, z)

        z = f"""insert into {self.de.DA_SCHEMA}.TAF_ANN_DE_{self.tbl_suffix}
                select

                    {DE.table_id_cols_sfx}
                    ,CMPRHNSV_MC_PLAN_MOS
                    ,TRDTNL_PCCM_MC_PLAN_MOS
                    ,ENHNCD_PCCM_MC_PLAN_MOS
                    ,HIO_MC_PLAN_MOS
                    ,PIHP_MC_PLAN_MOS
                    ,PAHP_MC_PLAN_MOS
                    ,LTC_PIHP_MC_PLAN_MOS
                    ,MH_PIHP_MC_PLAN_MOS
                    ,MH_PAHP_MC_PLAN_MOS
                    ,SUD_PIHP_MC_PLAN_MOS
                    ,SUD_PAHP_MC_PLAN_MOS
                    ,MH_SUD_PIHP_MC_PLAN_MOS
                    ,MH_SUD_PAHP_MC_PLAN_MOS
                    ,DNTL_PAHP_MC_PLAN_MOS
                    ,TRANSPRTN_PAHP_MC_PLAN_MOS
                    ,DEASE_MGMT_MC_PLAN_MOS
                    ,PACE_MC_PLAN_MOS
                    ,PHRMCY_PAHP_MC_PLAN_MOS
                    ,ACNTBL_MC_PLAN_MOS
                    ,HM_HOME_MC_PLAN_MOS
                    ,IC_DUALS_MC_PLAN_MOS
                    ,MC_PLAN_ID1_01
                    ,MC_PLAN_ID1_02
                    ,MC_PLAN_ID1_03
                    ,MC_PLAN_ID1_04
                    ,MC_PLAN_ID1_05
                    ,MC_PLAN_ID1_06
                    ,MC_PLAN_ID1_07
                    ,MC_PLAN_ID1_08
                    ,MC_PLAN_ID1_09
                    ,MC_PLAN_ID1_10
                    ,MC_PLAN_ID1_11
                    ,MC_PLAN_ID1_12
                    ,MC_PLAN_TYPE_CD1_01
                    ,MC_PLAN_TYPE_CD1_02
                    ,MC_PLAN_TYPE_CD1_03
                    ,MC_PLAN_TYPE_CD1_04
                    ,MC_PLAN_TYPE_CD1_05
                    ,MC_PLAN_TYPE_CD1_06
                    ,MC_PLAN_TYPE_CD1_07
                    ,MC_PLAN_TYPE_CD1_08
                    ,MC_PLAN_TYPE_CD1_09
                    ,MC_PLAN_TYPE_CD1_10
                    ,MC_PLAN_TYPE_CD1_11
                    ,MC_PLAN_TYPE_CD1_12
                    ,MC_PLAN_ID2_01
                    ,MC_PLAN_ID2_02
                    ,MC_PLAN_ID2_03
                    ,MC_PLAN_ID2_04
                    ,MC_PLAN_ID2_05
                    ,MC_PLAN_ID2_06
                    ,MC_PLAN_ID2_07
                    ,MC_PLAN_ID2_08
                    ,MC_PLAN_ID2_09
                    ,MC_PLAN_ID2_10
                    ,MC_PLAN_ID2_11
                    ,MC_PLAN_ID2_12
                    ,MC_PLAN_TYPE_CD2_01
                    ,MC_PLAN_TYPE_CD2_02
                    ,MC_PLAN_TYPE_CD2_03
                    ,MC_PLAN_TYPE_CD2_04
                    ,MC_PLAN_TYPE_CD2_05
                    ,MC_PLAN_TYPE_CD2_06
                    ,MC_PLAN_TYPE_CD2_07
                    ,MC_PLAN_TYPE_CD2_08
                    ,MC_PLAN_TYPE_CD2_09
                    ,MC_PLAN_TYPE_CD2_10
                    ,MC_PLAN_TYPE_CD2_11
                    ,MC_PLAN_TYPE_CD2_12
                    ,MC_PLAN_ID3_01
                    ,MC_PLAN_ID3_02
                    ,MC_PLAN_ID3_03
                    ,MC_PLAN_ID3_04
                    ,MC_PLAN_ID3_05
                    ,MC_PLAN_ID3_06
                    ,MC_PLAN_ID3_07
                    ,MC_PLAN_ID3_08
                    ,MC_PLAN_ID3_09
                    ,MC_PLAN_ID3_10
                    ,MC_PLAN_ID3_11
                    ,MC_PLAN_ID3_12
                    ,MC_PLAN_TYPE_CD3_01
                    ,MC_PLAN_TYPE_CD3_02
                    ,MC_PLAN_TYPE_CD3_03
                    ,MC_PLAN_TYPE_CD3_04
                    ,MC_PLAN_TYPE_CD3_05
                    ,MC_PLAN_TYPE_CD3_06
                    ,MC_PLAN_TYPE_CD3_07
                    ,MC_PLAN_TYPE_CD3_08
                    ,MC_PLAN_TYPE_CD3_09
                    ,MC_PLAN_TYPE_CD3_10
                    ,MC_PLAN_TYPE_CD3_11
                    ,MC_PLAN_TYPE_CD3_12
                    ,MC_PLAN_ID4_01
                    ,MC_PLAN_ID4_02
                    ,MC_PLAN_ID4_03
                    ,MC_PLAN_ID4_04
                    ,MC_PLAN_ID4_05
                    ,MC_PLAN_ID4_06
                    ,MC_PLAN_ID4_07
                    ,MC_PLAN_ID4_08
                    ,MC_PLAN_ID4_09
                    ,MC_PLAN_ID4_10
                    ,MC_PLAN_ID4_11
                    ,MC_PLAN_ID4_12
                    ,MC_PLAN_TYPE_CD4_01
                    ,MC_PLAN_TYPE_CD4_02
                    ,MC_PLAN_TYPE_CD4_03
                    ,MC_PLAN_TYPE_CD4_04
                    ,MC_PLAN_TYPE_CD4_05
                    ,MC_PLAN_TYPE_CD4_06
                    ,MC_PLAN_TYPE_CD4_07
                    ,MC_PLAN_TYPE_CD4_08
                    ,MC_PLAN_TYPE_CD4_09
                    ,MC_PLAN_TYPE_CD4_10
                    ,MC_PLAN_TYPE_CD4_11
                    ,MC_PLAN_TYPE_CD4_12
                    ,MC_PLAN_ID5_01
                    ,MC_PLAN_ID5_02
                    ,MC_PLAN_ID5_03
                    ,MC_PLAN_ID5_04
                    ,MC_PLAN_ID5_05
                    ,MC_PLAN_ID5_06
                    ,MC_PLAN_ID5_07
                    ,MC_PLAN_ID5_08
                    ,MC_PLAN_ID5_09
                    ,MC_PLAN_ID5_10
                    ,MC_PLAN_ID5_11
                    ,MC_PLAN_ID5_12
                    ,MC_PLAN_TYPE_CD5_01
                    ,MC_PLAN_TYPE_CD5_02
                    ,MC_PLAN_TYPE_CD5_03
                    ,MC_PLAN_TYPE_CD5_04
                    ,MC_PLAN_TYPE_CD5_05
                    ,MC_PLAN_TYPE_CD5_06
                    ,MC_PLAN_TYPE_CD5_07
                    ,MC_PLAN_TYPE_CD5_08
                    ,MC_PLAN_TYPE_CD5_09
                    ,MC_PLAN_TYPE_CD5_10
                    ,MC_PLAN_TYPE_CD5_11
                    ,MC_PLAN_TYPE_CD5_12
                    ,MC_PLAN_ID6_01
                    ,MC_PLAN_ID6_02
                    ,MC_PLAN_ID6_03
                    ,MC_PLAN_ID6_04
                    ,MC_PLAN_ID6_05
                    ,MC_PLAN_ID6_06
                    ,MC_PLAN_ID6_07
                    ,MC_PLAN_ID6_08
                    ,MC_PLAN_ID6_09
                    ,MC_PLAN_ID6_10
                    ,MC_PLAN_ID6_11
                    ,MC_PLAN_ID6_12
                    ,MC_PLAN_TYPE_CD6_01
                    ,MC_PLAN_TYPE_CD6_02
                    ,MC_PLAN_TYPE_CD6_03
                    ,MC_PLAN_TYPE_CD6_04
                    ,MC_PLAN_TYPE_CD6_05
                    ,MC_PLAN_TYPE_CD6_06
                    ,MC_PLAN_TYPE_CD6_07
                    ,MC_PLAN_TYPE_CD6_08
                    ,MC_PLAN_TYPE_CD6_09
                    ,MC_PLAN_TYPE_CD6_10
                    ,MC_PLAN_TYPE_CD6_11
                    ,MC_PLAN_TYPE_CD6_12
                    ,MC_PLAN_ID7_01
                    ,MC_PLAN_ID7_02
                    ,MC_PLAN_ID7_03
                    ,MC_PLAN_ID7_04
                    ,MC_PLAN_ID7_05
                    ,MC_PLAN_ID7_06
                    ,MC_PLAN_ID7_07
                    ,MC_PLAN_ID7_08
                    ,MC_PLAN_ID7_09
                    ,MC_PLAN_ID7_10
                    ,MC_PLAN_ID7_11
                    ,MC_PLAN_ID7_12
                    ,MC_PLAN_TYPE_CD7_01
                    ,MC_PLAN_TYPE_CD7_02
                    ,MC_PLAN_TYPE_CD7_03
                    ,MC_PLAN_TYPE_CD7_04
                    ,MC_PLAN_TYPE_CD7_05
                    ,MC_PLAN_TYPE_CD7_06
                    ,MC_PLAN_TYPE_CD7_07
                    ,MC_PLAN_TYPE_CD7_08
                    ,MC_PLAN_TYPE_CD7_09
                    ,MC_PLAN_TYPE_CD7_10
                    ,MC_PLAN_TYPE_CD7_11
                    ,MC_PLAN_TYPE_CD7_12
                    ,MC_PLAN_ID8_01
                    ,MC_PLAN_ID8_02
                    ,MC_PLAN_ID8_03
                    ,MC_PLAN_ID8_04
                    ,MC_PLAN_ID8_05
                    ,MC_PLAN_ID8_06
                    ,MC_PLAN_ID8_07
                    ,MC_PLAN_ID8_08
                    ,MC_PLAN_ID8_09
                    ,MC_PLAN_ID8_10
                    ,MC_PLAN_ID8_11
                    ,MC_PLAN_ID8_12
                    ,MC_PLAN_TYPE_CD8_01
                    ,MC_PLAN_TYPE_CD8_02
                    ,MC_PLAN_TYPE_CD8_03
                    ,MC_PLAN_TYPE_CD8_04
                    ,MC_PLAN_TYPE_CD8_05
                    ,MC_PLAN_TYPE_CD8_06
                    ,MC_PLAN_TYPE_CD8_07
                    ,MC_PLAN_TYPE_CD8_08
                    ,MC_PLAN_TYPE_CD8_09
                    ,MC_PLAN_TYPE_CD8_10
                    ,MC_PLAN_TYPE_CD8_11
                    ,MC_PLAN_TYPE_CD8_12
                    ,MC_PLAN_ID9_01
                    ,MC_PLAN_ID9_02
                    ,MC_PLAN_ID9_03
                    ,MC_PLAN_ID9_04
                    ,MC_PLAN_ID9_05
                    ,MC_PLAN_ID9_06
                    ,MC_PLAN_ID9_07
                    ,MC_PLAN_ID9_08
                    ,MC_PLAN_ID9_09
                    ,MC_PLAN_ID9_10
                    ,MC_PLAN_ID9_11
                    ,MC_PLAN_ID9_12
                    ,MC_PLAN_TYPE_CD9_01
                    ,MC_PLAN_TYPE_CD9_02
                    ,MC_PLAN_TYPE_CD9_03
                    ,MC_PLAN_TYPE_CD9_04
                    ,MC_PLAN_TYPE_CD9_05
                    ,MC_PLAN_TYPE_CD9_06
                    ,MC_PLAN_TYPE_CD9_07
                    ,MC_PLAN_TYPE_CD9_08
                    ,MC_PLAN_TYPE_CD9_09
                    ,MC_PLAN_TYPE_CD9_10
                    ,MC_PLAN_TYPE_CD9_11
                    ,MC_PLAN_TYPE_CD9_12
                    ,MC_PLAN_ID10_01
                    ,MC_PLAN_ID10_02
                    ,MC_PLAN_ID10_03
                    ,MC_PLAN_ID10_04
                    ,MC_PLAN_ID10_05
                    ,MC_PLAN_ID10_06
                    ,MC_PLAN_ID10_07
                    ,MC_PLAN_ID10_08
                    ,MC_PLAN_ID10_09
                    ,MC_PLAN_ID10_10
                    ,MC_PLAN_ID10_11
                    ,MC_PLAN_ID10_12
                    ,MC_PLAN_TYPE_CD10_01
                    ,MC_PLAN_TYPE_CD10_02
                    ,MC_PLAN_TYPE_CD10_03
                    ,MC_PLAN_TYPE_CD10_04
                    ,MC_PLAN_TYPE_CD10_05
                    ,MC_PLAN_TYPE_CD10_06
                    ,MC_PLAN_TYPE_CD10_07
                    ,MC_PLAN_TYPE_CD10_08
                    ,MC_PLAN_TYPE_CD10_09
                    ,MC_PLAN_TYPE_CD10_10
                    ,MC_PLAN_TYPE_CD10_11
                    ,MC_PLAN_TYPE_CD10_12
                    ,MC_PLAN_ID11_01
                    ,MC_PLAN_ID11_02
                    ,MC_PLAN_ID11_03
                    ,MC_PLAN_ID11_04
                    ,MC_PLAN_ID11_05
                    ,MC_PLAN_ID11_06
                    ,MC_PLAN_ID11_07
                    ,MC_PLAN_ID11_08
                    ,MC_PLAN_ID11_09
                    ,MC_PLAN_ID11_10
                    ,MC_PLAN_ID11_11
                    ,MC_PLAN_ID11_12
                    ,MC_PLAN_TYPE_CD11_01
                    ,MC_PLAN_TYPE_CD11_02
                    ,MC_PLAN_TYPE_CD11_03
                    ,MC_PLAN_TYPE_CD11_04
                    ,MC_PLAN_TYPE_CD11_05
                    ,MC_PLAN_TYPE_CD11_06
                    ,MC_PLAN_TYPE_CD11_07
                    ,MC_PLAN_TYPE_CD11_08
                    ,MC_PLAN_TYPE_CD11_09
                    ,MC_PLAN_TYPE_CD11_10
                    ,MC_PLAN_TYPE_CD11_11
                    ,MC_PLAN_TYPE_CD11_12
                    ,MC_PLAN_ID12_01
                    ,MC_PLAN_ID12_02
                    ,MC_PLAN_ID12_03
                    ,MC_PLAN_ID12_04
                    ,MC_PLAN_ID12_05
                    ,MC_PLAN_ID12_06
                    ,MC_PLAN_ID12_07
                    ,MC_PLAN_ID12_08
                    ,MC_PLAN_ID12_09
                    ,MC_PLAN_ID12_10
                    ,MC_PLAN_ID12_11
                    ,MC_PLAN_ID12_12
                    ,MC_PLAN_TYPE_CD12_01
                    ,MC_PLAN_TYPE_CD12_02
                    ,MC_PLAN_TYPE_CD12_03
                    ,MC_PLAN_TYPE_CD12_04
                    ,MC_PLAN_TYPE_CD12_05
                    ,MC_PLAN_TYPE_CD12_06
                    ,MC_PLAN_TYPE_CD12_07
                    ,MC_PLAN_TYPE_CD12_08
                    ,MC_PLAN_TYPE_CD12_09
                    ,MC_PLAN_TYPE_CD12_10
                    ,MC_PLAN_TYPE_CD12_11
                    ,MC_PLAN_TYPE_CD12_12
                    ,MC_PLAN_ID13_01
                    ,MC_PLAN_ID13_02
                    ,MC_PLAN_ID13_03
                    ,MC_PLAN_ID13_04
                    ,MC_PLAN_ID13_05
                    ,MC_PLAN_ID13_06
                    ,MC_PLAN_ID13_07
                    ,MC_PLAN_ID13_08
                    ,MC_PLAN_ID13_09
                    ,MC_PLAN_ID13_10
                    ,MC_PLAN_ID13_11
                    ,MC_PLAN_ID13_12
                    ,MC_PLAN_TYPE_CD13_01
                    ,MC_PLAN_TYPE_CD13_02
                    ,MC_PLAN_TYPE_CD13_03
                    ,MC_PLAN_TYPE_CD13_04
                    ,MC_PLAN_TYPE_CD13_05
                    ,MC_PLAN_TYPE_CD13_06
                    ,MC_PLAN_TYPE_CD13_07
                    ,MC_PLAN_TYPE_CD13_08
                    ,MC_PLAN_TYPE_CD13_09
                    ,MC_PLAN_TYPE_CD13_10
                    ,MC_PLAN_TYPE_CD13_11
                    ,MC_PLAN_TYPE_CD13_12
                    ,MC_PLAN_ID14_01
                    ,MC_PLAN_ID14_02
                    ,MC_PLAN_ID14_03
                    ,MC_PLAN_ID14_04
                    ,MC_PLAN_ID14_05
                    ,MC_PLAN_ID14_06
                    ,MC_PLAN_ID14_07
                    ,MC_PLAN_ID14_08
                    ,MC_PLAN_ID14_09
                    ,MC_PLAN_ID14_10
                    ,MC_PLAN_ID14_11
                    ,MC_PLAN_ID14_12
                    ,MC_PLAN_TYPE_CD14_01
                    ,MC_PLAN_TYPE_CD14_02
                    ,MC_PLAN_TYPE_CD14_03
                    ,MC_PLAN_TYPE_CD14_04
                    ,MC_PLAN_TYPE_CD14_05
                    ,MC_PLAN_TYPE_CD14_06
                    ,MC_PLAN_TYPE_CD14_07
                    ,MC_PLAN_TYPE_CD14_08
                    ,MC_PLAN_TYPE_CD14_09
                    ,MC_PLAN_TYPE_CD14_10
                    ,MC_PLAN_TYPE_CD14_11
                    ,MC_PLAN_TYPE_CD14_12
                    ,MC_PLAN_ID15_01
                    ,MC_PLAN_ID15_02
                    ,MC_PLAN_ID15_03
                    ,MC_PLAN_ID15_04
                    ,MC_PLAN_ID15_05
                    ,MC_PLAN_ID15_06
                    ,MC_PLAN_ID15_07
                    ,MC_PLAN_ID15_08
                    ,MC_PLAN_ID15_09
                    ,MC_PLAN_ID15_10
                    ,MC_PLAN_ID15_11
                    ,MC_PLAN_ID15_12
                    ,MC_PLAN_TYPE_CD15_01
                    ,MC_PLAN_TYPE_CD15_02
                    ,MC_PLAN_TYPE_CD15_03
                    ,MC_PLAN_TYPE_CD15_04
                    ,MC_PLAN_TYPE_CD15_05
                    ,MC_PLAN_TYPE_CD15_06
                    ,MC_PLAN_TYPE_CD15_07
                    ,MC_PLAN_TYPE_CD15_08
                    ,MC_PLAN_TYPE_CD15_09
                    ,MC_PLAN_TYPE_CD15_10
                    ,MC_PLAN_TYPE_CD15_11
                    ,MC_PLAN_TYPE_CD15_12
                    ,MC_PLAN_ID16_01
                    ,MC_PLAN_ID16_02
                    ,MC_PLAN_ID16_03
                    ,MC_PLAN_ID16_04
                    ,MC_PLAN_ID16_05
                    ,MC_PLAN_ID16_06
                    ,MC_PLAN_ID16_07
                    ,MC_PLAN_ID16_08
                    ,MC_PLAN_ID16_09
                    ,MC_PLAN_ID16_10
                    ,MC_PLAN_ID16_11
                    ,MC_PLAN_ID16_12
                    ,MC_PLAN_TYPE_CD16_01
                    ,MC_PLAN_TYPE_CD16_02
                    ,MC_PLAN_TYPE_CD16_03
                    ,MC_PLAN_TYPE_CD16_04
                    ,MC_PLAN_TYPE_CD16_05
                    ,MC_PLAN_TYPE_CD16_06
                    ,MC_PLAN_TYPE_CD16_07
                    ,MC_PLAN_TYPE_CD16_08
                    ,MC_PLAN_TYPE_CD16_09
                    ,MC_PLAN_TYPE_CD16_10
                    ,MC_PLAN_TYPE_CD16_11
                    ,MC_PLAN_TYPE_CD16_12

                from managed_care_{self.de.YEAR}
                where MNGD_CARE_SPLMTL_1_3=1 or MNGD_CARE_SPLMTL_4_6=1 or
                    MNGD_CARE_SPLMTL_7_9=1 or MNGD_CARE_SPLMTL_10_12=1"""

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
