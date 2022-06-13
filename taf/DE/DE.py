from taf.DE.DE_Runner import DE_Runner
from taf.TAF import TAF


class DE(TAF):

    #  - REPORTING_PERIOD: Date value from which we will take the last 4 characters to determine year
    #                      (read from job control table)
    #  - YEAR: Year of annual file (created from REPORTING_PERIOD)
    #  - RUNDATE: Date of run
    #  - VERSION: Version, in format of P1, P2, F1, F2, etc.. through P9/F9 (read from job control table)
    #  - DA_RUN_ID: sequential run ID, increments by 1 for each run of the monthly/annual TAF (read from
    #               job control table)
    #  - ROWCOUNT: # of records in the final tables, which will be assigned after the creation of
    #              each table and then inserted into the metadata table
    #  - TMSIS_SCHEMA: TMSIS schema (e.g. dev, val, prod) in which the program is being run, assigned
    #                  in the tmsis_config_macro above
    #  - DA_SCHEMA: Data Analytic schema (e.g. dev, val, prod) in which the program is being run, assigned
    #               in the da_config_macro above
    #  - ST_FILTER: List of states to run (if no states are listed, then take all) (read from job control
    #               table)
    #  - PYEARS: Prior years (all years from 2014 to current year minus 1)
    #  - GETPRIOR: Indicator for whether there are ANY records in the prior yeara to do prior year lookback.
    #              If yes, set = 1 and look to prior yeara to get demographic information if current year
    #              is missing for each enrollee/demographic column. This will be determined with the macro
    #              count_prior_year
    #  - NMCSLOTS: # of monthly slots for MC IDs/types (currently 16, set below)
    #  - NWAIVSLOTS: # of waiver slots for waiver IDs/types (currently 10, set below)
    #  - MONTHSB: List of months backwards from December to January (to loop through when needed)

    def __init__(self, de: DE_Runner, dtype, dval, year):

        self.de = de

        self.dtype = dtype
        self.dval = dval
        self.YEAR = self.de.reporting_period.year
        self.st_fil_type: str = 'DE'

        self.NMCSLOTS: int = 16
        self.NWAIVSLOTS: int = 10
        self.MONTHSB = ["12", "11", "10", "09", "08", "07", "06", "05", "04", "03", "02", "01"]
        self.REPORTING_PERIOD = ""
        self.RUNDATE = ""
        self.VERSION: int = 0
        self.DA_RUN_ID: int = 0
        self.ROWCOUNT: int = 0
        self.TMSIS_SCHEMA = ""
        self.DA_SCHEMA = ""
        self.ST_FILTER = ""
        self.GETPRIOR: int = 0
        self.PYEARS: int = 0
        self.NMCSLOTS = ""
        self.NWAIVSLOTS = ""

        self.create_initial_table()

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):
        print('')

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create_temp_table(
            self,
            fileseg,
            tblname,
            inyear,
            subcols,
            outercols="",
            subcols2="",
            subcols3="",
            subcols4="",
            subcols5="",
            subcols6="",
            subcols7="",
            subcols8="",
    ):

        # distkey({self.main_id})
        # sortkey(submtg_state_cd,{self.main_id},splmtl_submsn_type) as

        _outercols = outercols
        _subcols = subcols
        _subcols2 = subcols2
        _subcols3 = subcols3
        _subcols4 = subcols4
        _subcols5 = subcols5
        _subcols6 = subcols6
        _subcols7 = subcols7
        _subcols8 = subcols8

        if not _outercols == "":
            _outercols = "," + " " + _outercols

        if not _subcols == "":
            _subcols = "," + " " + _subcols

        if not _subcols2 == "":
            _subcols2 = "," + " " + _subcols2

        if not _subcols3 == "":
            _subcols3 = "," + " " + _subcols3

        if not _subcols4 == "":
            _subcols4 = "," + " " + _subcols4

        if not _subcols5 == "":
            _subcols5 = "," + " " + _subcols5

        if not _subcols6 == "":
            _subcols6 = "," + " " + _subcols6

        if not _subcols7 == "":
            _subcols7 = "," + " " + _subcols7

        if not _subcols8 == "":
            _subcols8 = "," + " " + _subcols8

        z = f"""
            create or replace temporary view {tblname}_{self.year} as
            select * {_outercols}
                from (
                    select fseg.submtg_state_cd
                        ,fseg.{self.main_id}
                        ,fseg.splmtl_submsn_type
                        {_subcols}
                        {_subcols2}
                        {_subcols3}
                        {_subcols4}
                        {_subcols5}
                        {_subcols6}
                        {_subcols7}
                        {_subcols8}

                    from ( {self.join_monthly(fileseg, self.fil_typ, inyear, self.main_id) } )

                    ) sub

            order by submtg_state_cd,
                    {self.main_id},
                    splmtl_submsn_type

        """
        self.de.append(type(self).__name__, z)

    def mc_type_rank(self, smonth: str, emonth: str):

        priorities = ["01", "04", "05", "06", "15", "07", "14", "17", "08", "09", "10",
                      "11", "12", "13", "18", "16", "02", "03", "60", "70", "80", "99"]

        z = ""

        for mm in range(smonth, emonth):

            if mm < 10:
                mm = str(mm).zfill(2)

            z += ",case "
            for p in priorities:
                z += "when (\n"
                for s in range(1, self.NMCSLOTS - 1):
                    z += f"""m{mm}.MC_PLAN_TYPE_CD{s} = '{p}'"""
                    if s < self.NMCSLOTS - 2:
                        z += " or\n"
            z += f""" ) then ('{p}')"""
            z += f"""else null
            end as MC_PLAN_TYPE_CD_{mm}
            """
        return z

    def misg_enrlm_type():
        z = ""
        for mm in range(1, 13):
            if mm < 10:
                mm = str(mm).zfill(2)

            z += f""",case when m{mm}.ENRL_TYPE_FLAG is null and m{mm}.msis_ident_num is not null
                then 1
                when m{mm}.msis_ident_num is not null
                then 0
                else null
                end as MISG_ENRLMT_TYPE_IND_{mm}."""

        return z

    def nonmiss_month(self, incol, outcol="", var_type="D"):

        if outcol == "":
            outcol = incol + "_MN"

        cases = []
        for m in self.monthsb:

            z = f"""m{m}.{incol} is not null"""
            if var_type != "D":
                z += f"""and m{m}.{incol} not in ('',' ') then '{m}'"""

            cases.append(z)

        return f"""case when {' or '.join(cases)} then 1 else '00' end as {outcol}"""

    def assign_nonmiss_month(self, outcol, monthval1, incol1, monthval2='', incol2=''):

        cases = []
        for m in self.monthsb:
            cases.append(f"when {monthval1}='{m}' then {incol1}_{m}")

            if monthval2 != '':
                for m in self.monthsb:
                    cases.append(f"when {monthval2}='{m}' then {incol2}_{m}")

        return f"case {' '.join(cases)} else null end as {outcol}"

    def address_flag(self):
        z = """,case when ELGBL_LINE_1_ADR_MAIL_MN != '00' and ELGBL_LINE_1_ADR_HOME_MN = '00' then 1
                    when ELGBL_LINE_1_ADR_HOME_MN != '00' then 0
                else null
                end as ELGBL_ADR_MAIL_FLAG
            """
        return z

    def address_same_year(self, incol):
        cnt = 0
        z = f""",case when yearpull = {self.YEAR} then c.{incol}"""
        for pyear in range(1, self.PYEARS + 1):
            cnt += 1
            z += f"""when yearpull = {pyear} then p{cnt}.{incol}"""

        z += f"""else null\n
                end as {incol}"""
        return z

    def unique_claims_ids(self, cltype):
        z = f"""
            select distinct b.submtg_state_cd
                            ,msis_ident_num

            from max_run_id_{cltype}_{self.YEAR} a
                inner join
                {self.DA_SCHEMA}.taf_{cltype}h b

            on a.submtg_state_cd = b.submtg_state_cd and
                a.{cltype}_fil_dt = b.{cltype}_fil_dt and
                a.da_run_id = b.da_run_id

            where msis_ident_num is not null and
                    substring(msis_ident_num,1,1) != ''
            """
        return z

    def table_id_cols(self):
        z = f"""{self.DA_SCHEMA} as DA_RUN_ID
            ,cast (('{self.DA_RUN_ID}') || '-' || '{self.YEAR}' || '-' || '{self.VERSION}') || '-' ||
            SUBMTG_STATE_CD || '-' || MSIS_IDENT_NUM) as varchar(40)) as UP_LINK_KEY
            ,'{self.YEAR}' as UP_FIL_DT
            ,('{self.VERSION}') as ANN_UP_VRSN
            ,SUBMTG_STATE_CD
            ,MSIS_IDENT_NUM
            """
        return z

    def monthly_array_eldts(self, incol, outcol, nslots=16, truncfirst=1):

        if outcol is None:
            outcol = incol

        for s in range(1, nslots + 1):
            if nslots == 1:
                snum = 1
            else:
                snum = s

            for m in range(1, 13):
                mm = str(m)
                if len(mm) == 1:
                    mm.zfill(2)

                if mm in ("01", "03", "05", "07", "08", "10", "12"):
                    lday = "31"
                if mm in ("09", "04", "06", "11"):
                    lday = 30
                if mm == "02" and self.YEAR % 4 == 0 and (self.YEAR % 100 != 0 or self.YEAR % 400 == 0):
                    lday = 29
                elif mm == "02":
                    lday = 28

                # Truncate effective dates to the 1st of the month if prior to the first. Otherwise pull in the raw date.
                if truncfirst == 1:
                    z = f""",case when m{mm}.{incol}.{snum} is not null and
                            date_cmp(m{mm}.{incol}.{snum},to_date('01 {mm} {self.YEAR}'),'dd mm yyyy')) = -1
                        then to_date('01 {mm} {self.YEAR}'),'dd mm yyyy')
                        else m{mm}.{incol}.{snum}
                        end as {outcol}{snum}_{mm}
                        """

                # Truncate end dates to the last day of the month if after the last day of the month. Otherwise pull in the
                # raw date
                if truncfirst == 0:
                    z = f""",case when m{mm}.{incol}.{snum} is not null and
                            date_cmp(m{mm}.{incol}.{snum},to_date('{lday} {mm} {self.YEAR}'),'dd mm yyyy')) = 1
                        then to_date('{lday} {mm} {self.YEAR}'),'dd mm yyyy')
                        else m{mm}.{incol}.{snum}
                        end as {outcol}{snum}_{mm}
                        """
        return z

    def mc_waiv_slots(self, incol, values, outcol, smonth=1, emonth=12):

        if incol == 'MC_PLAN_TYPE_CD':
            nslots = self.NMCSLOTS
        if incol == 'WVR_TYPE_CD':
            nslots = self.NWAIVSLOTS

        for m in range(smonth, emonth + 1):
            if m < 10:
                m = str(m).zfill(2)
            z = """,case when"""
            for s in range(1, nslots + 1):
                if s > 1:
                    z += "or"
                z += f"""m{m}.{incol}{s} in ({values})"""
            z += f"""
                then 1
                else 0
                end as {outcol}_{m}"""

    def run_mc_slots(self, smonth, emonth):
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('01'), 'CMPRHNSV_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('02'), 'TRDTNL_PCCM_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('03'), 'ENHNCD_PCCM_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('04'), 'HIO_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('05'), 'PIHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('06'), 'PAHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('07'), 'LTC_PIHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('08'), 'MH_PIHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('09'), 'MH_PAHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('10'), 'SUD_PIHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('11'), 'SUD_PAHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('12'), 'MH_SUD_PIHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('13'), 'MH_SUD_PAHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('14'), 'DNTL_PAHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('15'), 'TRANSPRTN_PAHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('16'), 'DEASE_MGMT_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('17'), 'PACE_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('18'), 'PHRMCY_PAHP_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('60'), 'ACNTBL_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('70'), 'HM_HOME_MC_PLAN', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('MC_PLAN_TYPE_CD', ('80'), 'IC_DUALS_MC_PLAN', smonth=smonth, emonth=emonth)

    def mc_nonnull_zero(self, smonth, emonth):
        pass

    def waiv_nonnull(self, outcol):
        pass

    def sum_months(incol, raw=0, outcol=""):

        if outcol == "":
            outcol = incol + "_MOS"

        if raw == 1:
            z = f""",coalesce(m01.{incol},0) || coalesce(m02.{incol},0) || coalesce(m03.{incol},0) ||
            coalesce(m04.{incol},0) || coalesce(m05.{incol},0) || coalesce(m06.{incol},0) ||
            coalesce(m07.{incol},0) || coalesce(m08.{incol},0) || coalesce(m09.{incol},0) ||
            coalesce(m10.{incol},0) || coalesce(m11.{incol},0) || coalesce(m12.{incol},0)"""

        if raw == 0:

            z = f""",coalesce({incol}_01,0) || coalesce({incol}_02,0) || coalesce({incol}_03,0) ||
            coalesce({incol}_04,0) || coalesce({incol}_05,0) || coalesce({incol}_06,0) ||
            coalesce({incol}_07,0) || coalesce({incol}_08,0) || coalesce({incol}_09,0) ||
            coalesce({incol}_10,0) || coalesce({incol}_11,0) || coalesce({incol}_12,0)"""

        z += f"""as {outcol}"""

        return z

    def run_waiv_slots(self, smonth, emonth):

        DE.mc_waiv_slots('WVR_TYPE_CD', "('22')", '_1115_PHRMCY_PLUS_WVR', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('WVR_TYPE_CD', "('23')", '_1115_DSTR_REL_WVR', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('WVR_TYPE_CD', "('24')", '_1115_FP_ONLY_WVR', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('WVR_TYPE_CD', "('06','07','08','09','10','11','12','13','14','15','16','17','18','19','33')", '_1915C_WVR', smonth=smonth, emonth=emonth)

        DE.mc_waiv_slots('WVR_TYPE_CD', "('20')", '_1915BC_WVR', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('WVR_TYPE_CD', "('02','03','04','05','32')", '_1915B_WVR', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('WVR_TYPE_CD', "('01')", '_1115_OTHR_WVR', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('WVR_TYPE_CD', "('21')", '_1115_HIFA_WVR', smonth=smonth, emonth=emonth)
        DE.mc_waiv_slots('WVR_TYPE_CD', "('25','26','27','28','29','30','31')", '_OTHR_WVR', smonth=smonth, emonth=emonth)

    def any_col(incols: str, outcol, condition='=1'):
        cols = incols.split(" ")
        cnt = len(cols)
        z = """,case when """
        for c in cnt:
            if c > 1:
                z += "or"
            z += f"""c {condition}"""

        z += f"""then 1 else 0
                end as {outcol}
            """
        return z

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
