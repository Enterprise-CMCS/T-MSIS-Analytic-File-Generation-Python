from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner
from taf.TAF_Closure import TAF_Closure


class DE0008(DE):
    table_name: str = "hhspo"
    tbl_suffix: str = "hh_spo"

    def __init__(self, runner: DE_Runner):
        DE.__init__(self, runner)
        self.de = runner

    #def __init__(self, de: DE_Runner):
        #super().__init__(de)

    def create(self):
        #super().create()
        self.create_temp()
        self.create_mfp_suppl_table()

    def create_temp(self):
        # Create a series of flags to be evaluated to create HH_SPO_SPLMTL

        s = f""",{TAF_Closure.monthly_array(self, 'HH_PGM_PRTCPNT_FLAG')}
                {DE.last_best(self, 'HH_PRVDR_NUM')}
                {DE.last_best(self, 'HH_ENT_NAME')}
                {DE.last_best(self, 'MH_HH_CHRNC_COND_FLAG')}
                {DE.last_best(self, 'SA_HH_CHRNC_COND_FLAG')}
                {DE.last_best(self, 'ASTHMA_HH_CHRNC_COND_FLAG')}
                {DE.last_best(self, 'DBTS_HH_CHRNC_COND_FLAG')}
                {DE.last_best(self, 'HRT_DIS_HH_CHRNC_COND_FLAG')}
                {DE.last_best(self, 'OVRWT_HH_CHRNC_COND_FLAG')}
                {DE.last_best(self, 'HIV_AIDS_HH_CHRNC_COND_FLAG')}
                {DE.last_best(self, 'OTHR_HH_CHRNC_COND_FLAG')}
                ,{TAF_Closure.monthly_array(self, 'CMNTY_1ST_CHS_SPO_FLAG')}
                ,{TAF_Closure.monthly_array(self, '_1915I_SPO_FLAG')}
                ,{TAF_Closure.monthly_array(self, '_1915J_SPO_FLAG')}
                ,{TAF_Closure.monthly_array(self, '_1932A_SPO_FLAG')}
                ,{TAF_Closure.monthly_array(self, '_1915A_SPO_FLAG')}
                ,{TAF_Closure.monthly_array(self, '_1937_ABP_SPO_FLAG')}
                ,{DE.ever_year(self, 'HH_PGM_PRTCPNT_FLAG')}
                ,{DE.ever_year(self, 'CMNTY_1ST_CHS_SPO_FLAG')}
                ,{DE.ever_year(self, '_1915I_SPO_FLAG')}
                ,{DE.ever_year(self, '_1915J_SPO_FLAG')}
                ,{DE.ever_year(self, '_1932A_SPO_FLAG')}
                ,{DE.ever_year(self, '_1915A_SPO_FLAG')}
                ,{DE.ever_year(self, '_1937_ABP_SPO_FLAG')}
            """

        # Create MFP_SPLMTL (which will go onto the base segment AND determines
        # the records that go into the permanent MFP table)
        os = DE.any_col('MH_HH_CHRNC_COND_FLAG SA_HH_CHRNC_COND_FLAG ASTHMA_HH_CHRNC_COND_FLAG \
                                DBTS_HH_CHRNC_COND_FLAG HRT_DIS_HH_CHRNC_COND_FLAG \
                                OVRWT_HH_CHRNC_COND_FLAG HIV_AIDS_HH_CHRNC_COND_FLAG \
                                OTHR_HH_CHRNC_COND_FLAG', 'HH_CHRNC_COND_ANY')

        DE.create_temp_table(self, tblname=self.table_name, inyear=self.de.YEAR, subcols=s, outercols=os)

        z = f"""create or replace temporary view {self.table_name}_{self.de.YEAR}2 as

                select *
                    {DE.any_col('HH_PGM_PRTCPNT_FLAG_EVR CMNTY_1ST_CHS_SPO_FLAG_EVR _1915I_SPO_FLAG_EVR _1915J_SPO_FLAG_EVR _1932A_SPO_FLAG_EVR _1915A_SPO_FLAG_EVR _1937_ABP_SPO_FLAG_EVR  HH_CHRNC_COND_ANY', 'HH_SPO_SPLMTL')}

                from {self.table_name}_{self.de.YEAR}"""

        self.de.append(type(self).__name__, z)

        return

    def create_mfp_suppl_table(self):
        z = f"""create or replace temporary view HH_SPO_SPLMTL_{self.de.YEAR} as
        select submtg_state_cd
                ,msis_ident_num
                ,HH_SPO_SPLMTL

        from {self.table_name}_{self.de.YEAR}2"""

        self.de.append(type(self).__name__, z)

        z = f"""insert into {self.de.DA_SCHEMA_DC}.TAF_ANN_DE_{self.table_name}
                select

                    {DE.table_id_cols_pre(self)}
                    ,HH_PGM_PRTCPNT_FLAG_01
                    ,HH_PGM_PRTCPNT_FLAG_02
                    ,HH_PGM_PRTCPNT_FLAG_03
                    ,HH_PGM_PRTCPNT_FLAG_04
                    ,HH_PGM_PRTCPNT_FLAG_05
                    ,HH_PGM_PRTCPNT_FLAG_06
                    ,HH_PGM_PRTCPNT_FLAG_07
                    ,HH_PGM_PRTCPNT_FLAG_08
                    ,HH_PGM_PRTCPNT_FLAG_09
                    ,HH_PGM_PRTCPNT_FLAG_10
                    ,HH_PGM_PRTCPNT_FLAG_11
                    ,HH_PGM_PRTCPNT_FLAG_12
                    ,HH_PRVDR_NUM
                    ,HH_ENT_NAME
                    ,MH_HH_CHRNC_COND_FLAG
                    ,SA_HH_CHRNC_COND_FLAG
                    ,ASTHMA_HH_CHRNC_COND_FLAG
                    ,DBTS_HH_CHRNC_COND_FLAG
                    ,HRT_DIS_HH_CHRNC_COND_FLAG
                    ,OVRWT_HH_CHRNC_COND_FLAG
                    ,HIV_AIDS_HH_CHRNC_COND_FLAG
                    ,OTHR_HH_CHRNC_COND_FLAG
                    ,CMNTY_1ST_CHS_SPO_FLAG_01
                    ,CMNTY_1ST_CHS_SPO_FLAG_02
                    ,CMNTY_1ST_CHS_SPO_FLAG_03
                    ,CMNTY_1ST_CHS_SPO_FLAG_04
                    ,CMNTY_1ST_CHS_SPO_FLAG_05
                    ,CMNTY_1ST_CHS_SPO_FLAG_06
                    ,CMNTY_1ST_CHS_SPO_FLAG_07
                    ,CMNTY_1ST_CHS_SPO_FLAG_08
                    ,CMNTY_1ST_CHS_SPO_FLAG_09
                    ,CMNTY_1ST_CHS_SPO_FLAG_10
                    ,CMNTY_1ST_CHS_SPO_FLAG_11
                    ,CMNTY_1ST_CHS_SPO_FLAG_12
                    ,_1915I_SPO_FLAG_01
                    ,_1915I_SPO_FLAG_02
                    ,_1915I_SPO_FLAG_03
                    ,_1915I_SPO_FLAG_04
                    ,_1915I_SPO_FLAG_05
                    ,_1915I_SPO_FLAG_06
                    ,_1915I_SPO_FLAG_07
                    ,_1915I_SPO_FLAG_08
                    ,_1915I_SPO_FLAG_09
                    ,_1915I_SPO_FLAG_10
                    ,_1915I_SPO_FLAG_11
                    ,_1915I_SPO_FLAG_12
                    ,_1915J_SPO_FLAG_01
                    ,_1915J_SPO_FLAG_02
                    ,_1915J_SPO_FLAG_03
                    ,_1915J_SPO_FLAG_04
                    ,_1915J_SPO_FLAG_05
                    ,_1915J_SPO_FLAG_06
                    ,_1915J_SPO_FLAG_07
                    ,_1915J_SPO_FLAG_08
                    ,_1915J_SPO_FLAG_09
                    ,_1915J_SPO_FLAG_10
                    ,_1915J_SPO_FLAG_11
                    ,_1915J_SPO_FLAG_12
                    ,_1932A_SPO_FLAG_01
                    ,_1932A_SPO_FLAG_02
                    ,_1932A_SPO_FLAG_03
                    ,_1932A_SPO_FLAG_04
                    ,_1932A_SPO_FLAG_05
                    ,_1932A_SPO_FLAG_06
                    ,_1932A_SPO_FLAG_07
                    ,_1932A_SPO_FLAG_08
                    ,_1932A_SPO_FLAG_09
                    ,_1932A_SPO_FLAG_10
                    ,_1932A_SPO_FLAG_11
                    ,_1932A_SPO_FLAG_12
                    ,_1915A_SPO_FLAG_01
                    ,_1915A_SPO_FLAG_02
                    ,_1915A_SPO_FLAG_03
                    ,_1915A_SPO_FLAG_04
                    ,_1915A_SPO_FLAG_05
                    ,_1915A_SPO_FLAG_06
                    ,_1915A_SPO_FLAG_07
                    ,_1915A_SPO_FLAG_08
                    ,_1915A_SPO_FLAG_09
                    ,_1915A_SPO_FLAG_10
                    ,_1915A_SPO_FLAG_11
                    ,_1915A_SPO_FLAG_12
                    ,_1937_ABP_SPO_FLAG_01
                    ,_1937_ABP_SPO_FLAG_02
                    ,_1937_ABP_SPO_FLAG_03
                    ,_1937_ABP_SPO_FLAG_04
                    ,_1937_ABP_SPO_FLAG_05
                    ,_1937_ABP_SPO_FLAG_06
                    ,_1937_ABP_SPO_FLAG_07
                    ,_1937_ABP_SPO_FLAG_08
                    ,_1937_ABP_SPO_FLAG_09
                    ,_1937_ABP_SPO_FLAG_10
                    ,_1937_ABP_SPO_FLAG_11
                    ,_1937_ABP_SPO_FLAG_12
                    {DE.table_id_cols_sfx(self)}

                    from {self.table_name}_{self.de.YEAR}2
                    where HH_SPO_SPLMTL=1"""

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
