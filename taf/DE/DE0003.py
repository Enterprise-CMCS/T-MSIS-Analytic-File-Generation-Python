

from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner
from taf.TAF_Closure import TAF_Closure


class DE0003(DE):
    tblname: str = "cntct_dtls"

    def __init__(self, runner: DE_Runner):
        # TODO: Review this
        DE.__init__(DE, runner)

    def create(self):
        super().create()
        self.create_temp()
        self.address_phone(runyear=self.de.YEAR)
        self.create_CNTCT_DTLS()

    def address_phone(self, runyear):
        DE.create_temp_table(self,
                             tblname=self.tblname,
                             inyear=runyear,
                             subcols=f"""{TAF_Closure.monthly_array(self, 'ELGBL_LINE_1_ADR_HOME')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_LINE_2_ADR_HOME')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_LINE_3_ADR_HOME')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_CITY_NAME_HOME')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_ZIP_CD_HOME')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_CNTY_CD_HOME')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_STATE_CD_HOME')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_LINE_1_ADR_MAIL')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_LINE_2_ADR_MAIL')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_LINE_3_ADR_MAIL')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_CITY_NAME_MAIL')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_ZIP_CD_MAIL')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_CNTY_CD_MAIL')}
                                         {TAF_Closure.monthly_array(self, 'ELGBL_STATE_CD_MAIL')}
                                         {DE.last_best(self, 'ELGBL_PHNE_NUM_HOME')}
                                         {DE.nonmiss_month(self, 'ELGBL_LINE_1_ADR_HOME')}
                                         {DE.nonmiss_month(self, 'ELGBL_LINE_1_ADR_MAIL')}
                                     """,
                             outercols=f"""{DE.address_flag(self)}
                                           {DE.assign_nonmiss_month(self, 'ELGBL_LINE_1_ADR', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_LINE_1_ADR_HOME', 'monthval2=ELGBL_LINE_1_ADR_MAIL_MN', 'incol2=ELGBL_LINE_1_ADR_MAIL')}
                                           {DE.assign_nonmiss_month(self, 'ELGBL_LINE_2_ADR', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_LINE_2_ADR_HOME', 'monthval2=ELGBL_LINE_1_ADR_MAIL_MN', 'incol2=ELGBL_LINE_2_ADR_MAIL')}
                                           {DE.assign_nonmiss_month(self, 'ELGBL_LINE_3_ADR', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_LINE_3_ADR_HOME', 'monthval2=ELGBL_LINE_1_ADR_MAIL_MN', 'incol2=ELGBL_LINE_3_ADR_MAIL')}
                                           {DE.assign_nonmiss_month(self, 'ELGBL_CITY_NAME', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_CITY_NAME_HOME', 'monthval2=ELGBL_LINE_1_ADR_MAIL_MN', 'incol2=ELGBL_CITY_NAME_MAIL')}
                                           {DE.assign_nonmiss_month(self, 'ELGBL_ZIP_CD', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_ZIP_CD_HOME', 'monthval2=ELGBL_LINE_1_ADR_MAIL_MN', 'incol2=ELGBL_ZIP_CD_MAIL')}
                                           {DE.assign_nonmiss_month(self, 'ELGBL_CNTY_CD', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_CNTY_CD_HOME', 'monthval2=ELGBL_LINE_1_ADR_MAIL_MN', 'incol2=ELGBL_CNTY_CD_MAIL')}
                                           {DE.assign_nonmiss_month(self, 'ELGBL_STATE_CD', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_STATE_CD_HOME', 'monthval2=ELGBL_LINE_1_ADR_MAIL_MN', 'incol2=ELGBL_STATE_CD_MAIL')}
                                        """)

    def create_temp(self):
        DE.create_temp_table(self,
                             tblname='name',
                             inyear=self.de.YEAR,
                             subcols=f"""{DE.last_best(self, 'ELGBL_1ST_NAME')}
                                         {DE.last_best(self, 'ELGBL_LAST_NAME')}
                                         {DE.last_best(self, 'ELGBL_MDL_INITL')}
                            """)

    def create_CNTCT_DTLS(self):
        if self.de.GETPRIOR == 1:
            cnt = 0
            if self.de.GETPRIOR == 1:
                for pyear in range(1, self.de.PYEARS + 1):
                    self.address_phone(pyear)

            # Join current and prior year(s) and first, identify year pulled for latest non-null value of ELGBL_LINE_1_ADR.
            # Use that year to then take value for all cols

            z = f"""create or replace temporary view address_phone_{self.de.YEAR}_out as
                    select c.submtg_state_cd,
                        c.msis_ident_num

            {DE.last_best(self, 'ELGBL_LINE_1_ADR', prior=1)}
            ,case when c.ELGBL_LINE_1_ADR is not null then {self.de.YEAR}"""
            for pyear in range(1, self.de.PYEARS + 1):
                cnt += 1
                z += f"""when p{cnt}.ELGBL_LINE_1_ADR is not null then {pyear}"""
            z += f"""else null
                    end as yearpull

                {DE.address_same_year('ELGBL_ADR_MAIL_FLAG')}
                {DE.address_same_year('ELGBL_LINE_2_ADR')}
                {DE.address_same_year('ELGBL_LINE_3_ADR')}
                {DE.address_same_year('ELGBL_CITY_NAME')}
                {DE.address_same_year('ELGBL_ZIP_CD')}
                {DE.address_same_year('ELGBL_CNTY_CD')}
                {DE.address_same_year('ELGBL_STATE_CD')}

                {DE.last_best(self, 'ELGBL_PHNE_NUM_HOME', prior=1)}

                from address_phone_{self.de.YEAR} c"""
            cnt = 0
            for pyear in range(1, self.de.PYEARS + 1):
                f"""left join
                    address_phone_{self.de.YEAR} p{cnt}

                on c.submtg_state_cd = p{cnt}.submtg_state_cd and
                    c.msis_ident_num = p{cnt}.msis_ident_num
                """
            self.de.append(type(self).__name__, z)

        if self.de.GETPRIOR == 0:
            z = f"""alter view address_phone_{self.de.YEAR} rename to address_phone_{self.de.YEAR}_out"""
            self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view name_address_phone_{self.de.YEAR} as
                select a.submtg_state_cd,
                    a.msis_ident_num,
                    a.ELGBL_1ST_NAME,
                    a.ELGBL_LAST_NAME,
                    a.ELGBL_MDL_INITL_NAME,
                    b.ELGBL_ADR_MAIL_FLAG,
                    b.ELGBL_LINE_1_ADR,
                    b.ELGBL_LINE_2_ADR,
                    b.ELGBL_LINE_3_ADR,
                    b.ELGBL_CITY_NAME,
                    b.ELGBL_ZIP_CD,
                    b.ELGBL_CNTY_CD,
                    b.ELGBL_STATE_CD,
                    b.ELGBL_PHNE_NUM_HOME

                    from name_{self.de.YEAR} a
                        inner join
                        address_phone_{self.de.YEAR}_out b

                    on a.submtg_state_cd = b.submtg_state_cd and
                    a.msis_ident_num = b.msis_ident_num
                """
        self.de.append(type(self).__name__, z)

        z = f"""insert into {self.de.DA_SCHEMA}.TAF_ANN_DE_{self.tblname}
                select
                    {DE.table_id_cols_sfx(self)}
                    ,ELGBL_1ST_NAME
                    ,ELGBL_LAST_NAME
                    ,ELGBL_MDL_INITL_NAME
                    ,ELGBL_ADR_MAIL_FLAG
                    ,ELGBL_LINE_1_ADR
                    ,ELGBL_LINE_2_ADR
                    ,ELGBL_LINE_3_ADR
                    ,ELGBL_CITY_NAME
                    ,ELGBL_ZIP_CD
                    ,ELGBL_CNTY_CD
                    ,ELGBL_STATE_CD
                    ,ELGBL_PHNE_NUM_HOME

                from name_address_phone_{self.de.YEAR}
            """
        self.de.append(type(self).__name__, z)

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
