from taf.DE.DE_Runner import DE_Runner
from taf.TAF import TAF


class DE(TAF):
    """
    Annual Demographic and Eligibility (DE) TAF: The annual DE TAF contain demographic,
    eligibility, and enrollment information for all Medicaid and CHIP beneficiaries who
    were enrolled for at least one day during each calendar year; there is also a “dummy”
    record for each beneficiary who had claims information during the year but no
    corresponding eligibility information. Each annual DE TAF is comprised of eight
    files:  a Base file; Eligibility Dates file; Name, Address & Phone file; Managed Care file;
    Waiver file; Money Follows the Person file; Health Home & State Plan Option file;
    and Disability and Need file. All eight files can be linked together using unique keys
    that are constructed based on various data elements.  The annual DE TAF are created
    solely from the monthly BSF TAF.
    """

    def __init__(self, runner: DE_Runner):
        self.de = runner
        self.main_id = runner.main_id

    #def __init__(self, de: DE_Runner):
        #self.de = de
        #self.main_id = de.main_id

    def create(self):
        DE.create_pyears(self)
        if self.de.GETPRIOR == 1:
            for pyear in self.de.PYEARS:
                DE.max_run_id(self, file="DE", inyear=pyear)
        DE.max_run_id(self, file="DE", inyear=self.de.YEAR)
        DE.max_run_id(self, file="BSF", inyear=self.de.YEAR)
        DE.max_run_id(self, file="IP", inyear=self.de.YEAR)
        DE.max_run_id(self, file="IP", inyear=self.de.PYEAR)
        DE.max_run_id(self, file="IP", inyear=self.de.FYEAR)
        DE.max_run_id(self, file="LT", inyear=self.de.YEAR)
        DE.max_run_id(self, file="OT", inyear=self.de.YEAR)
        DE.max_run_id(self, file="RX", inyear=self.de.YEAR)
        pass

    def create_temp_table(
            self,
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

        """
        Function create_temp_table to create each main table. For each table, there are columns we must get from the raw data in
        the subquery, and then columns we must get from the outer query that pulls from the subquery.

        Function parms:
            tblname=table name
            inyear=input year, where the default will be set to the current year but it can be changed to the prior year,
                    for when we need to read in demographic information from the prior year
            subcols=creation statements for all columns that must be pulled from the raw data in the subquery
            outercols=creation statements for all columns that must be pulled from the subquery
            subcols2 - subcols8=additional subcols when needing to loop over MC and waiver slots, because cannot
                                loop over all slots within one Function var or will exceed text limit of 65534 chars
        """

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

        if inyear == "":
            inyear = self.de.YEAR

        z = f"""
            create or replace temporary view {tblname}_{inyear} as
                select * {outercols}

                    from (
                        select
                             enrl.submtg_state_cd
                            ,enrl.msis_ident_num
                            {subcols}
                            {subcols2}
                            {subcols3}
                            {subcols4}
                            {subcols5}
                            {subcols6}
                            {subcols7}
                            {subcols8}

                        from ( {self.joinmonthly()} )


                    ) sub

            order by submtg_state_cd,
                    msis_ident_num

        """
        self.de.append(type(self).__name__, z)
        print(f"""Creating Temp Table: {tblname}_{inyear}""")

    def mc_type_rank(self, smonth: int, emonth: int):
        """
        Function mc_type_rank to look across all MC types for each month and assign one type for
        the month based on the priority ranking. For each month, must loop through each value
        in priority order and within each value, must loop through each slot.

        Function parms:
            smonth=the month to begin looping over, where default=1.
            emonth=the month to end looping over, where default=12.
        """

        priorities = ["01", "04", "05", "06", "15", "07", "14", "17", "08", "09", "10",
                      "11", "12", "13", "19", "18", "16", "02", "03", "60", "70", "80", "20", "99"]

        z = ""

        for mm in range(smonth, emonth + 1):
            mm = str(mm).zfill(2)

            z += ",case "
            for p in priorities:
                z += " when ("

                for s in range(1, self.de.NMCSLOTS + 1):
                    z += f"""m{mm}.MC_PLAN_TYPE_CD{s} = '{p}'"""
                    if s < self.de.NMCSLOTS:
                        z += " or "
                z += f""" ) then '{p}'"""
            z += f""" else null
            end as MC_PLAN_TYPE_CD_{mm}
            """
        return z

    def misg_enrlm_type():
        """
        Function misg_enrlmt_type to create indicators for ENRL_TYPE_FLAG = NULL.
        Set to 1 if ENRL_TYPE_FLAG = NULL AND the person is in the month.
        Set to 0 if ENRL_TYPE_FLAG != NULL AND person in the month.
        Set to NULL if the person is not in the month.
        """

        z = ""
        for mm in range(1, 13):
            if mm < 10:
                mm = str(mm).zfill(2)

            z += f"""
                ,case when m{mm}.ENRL_TYPE_FLAG is null and m{mm}.msis_ident_num is not null
                then 1
                when m{mm}.msis_ident_num is not null
                then 0
                else null
                end as MISG_ENRLMT_TYPE_IND_{mm}
                """

        return z

    def nonmiss_month(self, incol, outcol="", var_type="D"):
        """
        Function nonmiss_month to loop through given variable from month 12 to 1 and identify the month with
        the first non-missing value. This will then be used to pull additional columns that should be paired
        with that month. The month = 00 if NO non-missing month.

        Function parms:
            incol=input monthly column
            outcol=output column with month number, where the default is the incol name with the _MN (month number) suffix
        """


        if outcol == "":
            outcol = incol + "_MN"

        cases = []
        for m in range(12, 0, -1):
            if m < 10:
                m = str(m).zfill(2)

            z = f""" m{m}.{incol} is not null """
            if var_type != "D":
                z += f""" and m{m}.{incol} not in ('',' ') then '{m}' """

            cases.append(z)

        return f""",case when {' or '.join(cases)} then 1 else '00' end as {outcol}"""

    def nonmiss_month2(self, incol, outcol="", var_type="D"):

        if outcol == "":
            outcol = incol + "_MN"

        cases = []
        for m in range(12, 0, -1):
            if m < 10:
                m = str(m).zfill(2)

            z = f""" m{m}.{incol} is not null """
            if var_type != "D":
                z += f""" and m{m}.{incol} not in ('',' ') then '{m}' """

            cases.append(z)

        return f""",case when {' when '.join(cases)} else '00' end as {outcol}"""

    def assign_nonmiss_month(self, outcol, monthval1, incol1, monthval2='', incol2=''):
        """
        Function assign_nonmiss_month looks at the values for the monthly variables assigned in nonmiss_month,
        and pulls multiple variables for that month based on the assigned month from nonmiss_month. Note
        this can be based on 1 or 2 monthly assignments from nonmiss_month, where the first is evaluated and
        if a month is never assigned to that variable, the second will be evaluated. This happens for HOME and
        MAIL address. Note that nonmiss_month must be run in the subquery before assign_nonmiss_month is run in
        the outer query.

        Function parms:
            outcol=column to assign based on the month captured in nonmiss_month
            monthval1=monthly value to evaluate captured in nonmiss_month
            incol1=input column to assign if monthval1 is met
            monthval2=optional monthly value to evaluate captured in nonmiss_month, IF monthval1=00
            incol2=optional input column to assign if monthval2 is met
        """

        cases = []
        for m in range(12, 0, -1):
            if m < 10:
                m = str(m).zfill(2)
            cases.append(f" when {monthval1}='{m}' then {incol1}_{m}")

        if monthval2 != '':
            for m in range(12, 0, -1):
                if m < 10:
                    m = str(m).zfill(2)
                cases.append(f" when {monthval2}='{m}' then {incol2}_{m}")

        return f",case {' '.join(cases)} else null end as {outcol}"

    def address_flag(self):
        """
        Function address_flag looks at the values for HOME_month and MAIL_month and assigns a 1
        if MAIL_month ne 00, otherwise 0 if HOME_month ne 00, otherwise null
        """

        z = """,case when ELGBL_LINE_1_ADR_MAIL_MN != '00' and ELGBL_LINE_1_ADR_HOME_MN = '00' then '1'
                    when ELGBL_LINE_1_ADR_HOME_MN != '00' then '0'
                else null
                end as ELGBL_ADR_MAIL_FLAG
            """
        return z

    def address_same_year(self, incol):
        """
        Function address_same_year to use yearpull to pull in the address information
        from the same year in which ELGBL_LINE_1_ADR was pulled

        Function parms:
            incol = input col to pull
        """

        cnt = 0
        z = f""",case when one.yearpull = {self.de.YEAR} then c.{incol}"""
        for pyear in self.de.PYEARS:
            cnt += 1
            z += f""" when one.yearpull = {pyear} then p{cnt}.{incol}"""

        z += f""" else null
                end as {incol}"""
        return z

    def unique_claims_ids(self, cltype):
        """
        Function unique_claims_ids to join the max da_run_ids for the given claims file back to the monthly TAF and
        create a table of list of unique state/msis IDs with any claim.
        These lists will be unioned outside the Function.
        """

        z = f"""
            select distinct b.submtg_state_cd
                            ,msis_ident_num

            from max_run_id_{cltype}_{self.de.YEAR} a
                inner join
                {self.de.DA_SCHEMA}.taf_{cltype}h b

            on a.submtg_state_cd = b.submtg_state_cd and
                a.{cltype}_fil_dt = b.{cltype}_fil_dt and
                a.da_run_id = b.da_run_id

            where msis_ident_num is not null and
                    substring(msis_ident_num,1,1) != '&'
            """
        return z

    def table_id_cols_pre(self, suffix="", extra_cols=[]):
        """
        Function to generate ID cols like DE_LINK_KEY, DE_FIL_DT, ANN_DE_VRSN, and MSIS_IDENT_NUM
        """

        z = f"""cast ({self.de.DA_RUN_ID} || '-' || '{self.de.YEAR}' || '-' || '{self.de.VERSION}' || '-' ||
            SUBMTG_STATE_CD{suffix} || '-' || MSIS_IDENT_NUM{suffix} as varchar(40)) as DE_LINK_KEY
            ,'{self.de.YEAR}' as DE_FIL_DT
            ,'{self.de.VERSION}' as ANN_DE_VRSN
            ,MSIS_IDENT_NUM"""
        if len(extra_cols) > 0:
            z += ","
        z += f"""{",".join(extra_cols)}"""
        return z

    def table_id_cols_sfx(self, suffix="", extra_cols=[], as_select=False):
        """
        Function to generate SQL adding current timestamp, DA_RUN_ID, and Submitting State code to the query.
        """

        z = ""
        if as_select is False:
            z += f"""
                ,current_timestamp() as REC_ADD_TS
                ,cast(NULL as timestamp) as REC_UPDT_TS
                ,{self.de.DA_RUN_ID} as DA_RUN_ID
                ,SUBMTG_STATE_CD
                """
        else:
            z += """,REC_ADD_TS
                    ,REC_UPDT_TS
                    ,DA_RUN_ID
                    ,SUBMTG_STATE_CD
                 """
        return z

    def monthly_array_eldts(self, incol, outcol, nslots=16, truncfirst=1):
        """
        Function monthly_array_eldts to take the raw monthly columns and array into columns with _MO suffixes.
        Effective dates will be truncated to the first of the month if prior to the first, and end dates
        will be truncated to the end of the month if after.

        Function parms:
            incol=input monthly column
            outcol=name of column to be output, where default is the name of the incol with _MO for each month appended as a suffix
            nslots=# of slots, default = 16 (# of slots of effective/end dates on the BSF)
            truncfirst=indicator for whether date should be truncated to the first of the month (i.e. date being read in is an effective
                        date), where default = 1. Set to 0 for end dates (truncated to last day of the month)
        """

        lday = "31"
        z = ""
        if outcol == "":
            outcol = incol

        for s in range(1, nslots + 1):
            if nslots == 1:
                snum = 1
            else:
                snum = s

            for m in range(1, 13):
                m = str(m).zfill(2)

                if m in ("01", "03", "05", "07", "08", "10", "12"):
                    lday = "31"
                if m in ("09", "04", "06", "11"):
                    lday = "30"
                if m == "02" and self.de.YEAR % 4 == 0 and (self.de.YEAR % 100 != 0 or self.de.YEAR % 400 == 0):
                    lday = "29"
                elif m == "02":
                    lday = "28"

                # Truncate effective dates to the 1st of the month if prior to the first. Otherwise pull in the raw date.
                if truncfirst == 1:
                    z += f"""
                        ,case when m{m}.{incol}{snum} is not null and
                            datediff(m{m}.{incol}{snum},to_date('01 {m} {self.de.YEAR}','dd MM yyyy')) <= -1
                        then to_date('01 {m} {self.de.YEAR}','dd MM yyyy')
                        else m{m}.{incol}{snum}
                        end as {outcol}{snum}_{m}
                        """

                # Truncate end dates to the last day of the month if after the last day of the month. Otherwise pull in the
                # raw date
                if truncfirst == 0:
                    z += f"""
                        ,case when m{m}.{incol}{snum} is not null and
                            datediff(m{m}.{incol}{snum},to_date('{lday} {m} {self.de.YEAR}','dd MM yyyy')) >= 1
                        then to_date('{lday} {m} {self.de.YEAR}','dd MM yyyy')
                        else m{m}.{incol}{snum}
                        end as {outcol}{snum}_{m}
                        """
        return z

    def mc_waiv_slots(self, incol, values, outcol, smonth=1, emonth=12):
        """
        Function mc_waiv_slots to look across all MC or waiver slots for the month and create an indicator for months with specific
        values of type.

        Function parms:
            incol=input type column to evaluate
            values=list of values (waiver or MC types) to look for
            outcol=output column with indicator for specific type
            smonth=the month to begin looping over, where default=1
            emonth=the month to end looping over, where default=12
        """


        z = ""
        if incol == 'MC_PLAN_TYPE_CD':
            nslots = self.de.NMCSLOTS

        if incol == 'WVR_TYPE_CD':
            nslots = self.de.NWAIVSLOTS

        for m in range(smonth, emonth + 1):
            m = str(m).zfill(2)
            z += """
                    ,case when
                 """
            for s in range(1, nslots + 1):
                if s > 1:
                    z += " or "
                z += f""" m{m}.{incol}{s} in ({values}) """
            z += f"""
                then 1
                else 0
                end as {outcol}_{m} """
        return z

    def run_mc_slots(self, _smonth, _emonth):
        """
        Function run_mc_slots to run the above mc_waiv_slots Function for all the MC types.

        Function parms:
            smonth=the month to begin looping over, where default=1
            emonth=the month to end looping over, where default=12
        """

        z = f"""{DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'01'", outcol='CMPRHNSV_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'02'", outcol='TRDTNL_PCCM_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'03'", outcol='ENHNCD_PCCM_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'04'", outcol='HIO_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'05'", outcol='PIHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'06'", outcol='PAHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'07'", outcol='LTC_PIHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'08'", outcol='MH_PIHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'09'", outcol='MH_PAHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'10'", outcol='SUD_PIHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'11'", outcol='SUD_PAHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'12'", outcol='MH_SUD_PIHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'13'", outcol='MH_SUD_PAHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'14'", outcol='DNTL_PAHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'15'", outcol='TRANSPRTN_PAHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'16'", outcol='DEASE_MGMT_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'17'", outcol='PACE_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'18'", outcol='PHRMCY_PAHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'19'", outcol='LTSS_PIHP_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'20'", outcol='OTHR_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'60'", outcol='ACNTBL_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'70'", outcol='HM_HOME_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'80'", outcol='IC_DUALS_MC_PLAN', smonth=_smonth, emonth=_emonth)}"""
        return z

    def sum_months(self, incol, raw=0, outcol=""):
        """
        Function sum_months to take a SUM over all the input months.

        Function parms:
        incol=input monthly column which will be summed (with _MO suffix for each month)
        raw=indicator for whether the monthly variables are raw (must come from the 12 monthly files) or were created
            in an earlier subquery and will therefore have the _MO suffixes, where default = 0
        outcol=output column with summation, where the default is the incol name with the _MONTHS suffix
        """

        if outcol == "":
            outcol = incol + "_MOS"

        if raw == 1:
            z = f""",coalesce(m01.{incol},0) + coalesce(m02.{incol},0) + coalesce(m03.{incol},0) +
            coalesce(m04.{incol},0) + coalesce(m05.{incol},0) + coalesce(m06.{incol},0) +
            coalesce(m07.{incol},0) + coalesce(m08.{incol},0) + coalesce(m09.{incol},0) +
            coalesce(m10.{incol},0) + coalesce(m11.{incol},0) + coalesce(m12.{incol},0)"""

        if raw == 0:

            z = f""",coalesce({incol}_01,0) + coalesce({incol}_02,0) + coalesce({incol}_03,0) +
            coalesce({incol}_04,0) + coalesce({incol}_05,0) + coalesce({incol}_06,0) +
            coalesce({incol}_07,0) + coalesce({incol}_08,0) + coalesce({incol}_09,0) +
            coalesce({incol}_10,0) + coalesce({incol}_11,0) + coalesce({incol}_12,0)"""

        z += f""" as {outcol}"""

        return z

    def run_waiv_slots(self, _smonth, _emonth):
        """
        Function run_waiv_slots to run the above mc_waiv_slots Function for all the Waiver types.

        Function parms:
            smonth=the month to begin looping over, where default=1
            emonth=the month to end looping over, where default=12
        """


        z = f"""
            {DE.mc_waiv_slots(self, incol='WVR_TYPE_CD', values="'22'", outcol='_1115_PHRMCY_PLUS_WVR', smonth=_smonth, emonth=_emonth)}
            {DE.mc_waiv_slots(self, incol='WVR_TYPE_CD', values="'23'", outcol='_1115_DSTR_REL_WVR', smonth=_smonth, emonth=_emonth)}
            {DE.mc_waiv_slots(self, incol='WVR_TYPE_CD', values="'24'", outcol='_1115_FP_ONLY_WVR', smonth=_smonth, emonth=_emonth)}
            {DE.mc_waiv_slots(self, incol='WVR_TYPE_CD', values="'06','07','08','09','10','11','12','13','14','15','16','17','18','19','33'", outcol='_1915C_WVR', smonth=_smonth, emonth=_emonth)}
            {DE.mc_waiv_slots(self, incol='WVR_TYPE_CD', values="'20'", outcol='_1915BC_WVR', smonth=_smonth, emonth=_emonth)}
            {DE.mc_waiv_slots(self, incol='WVR_TYPE_CD', values="'02','03','04','05','32'", outcol='_1915B_WVR', smonth=_smonth, emonth=_emonth)}
            {DE.mc_waiv_slots(self, incol='WVR_TYPE_CD', values="'01'", outcol='_1115_OTHR_WVR', smonth=_smonth, emonth=_emonth)}
            {DE.mc_waiv_slots(self, incol='WVR_TYPE_CD', values="'21'", outcol='_1115_HIFA_WVR', smonth=_smonth, emonth=_emonth)}
            {DE.mc_waiv_slots(self, incol='WVR_TYPE_CD', values="'25','26','27','28','29','30','31'", outcol='_OTHR_WVR', smonth=_smonth, emonth=_emonth)}
            """
        return z

    # Non-null/00 plan type
    # OR non-0, 8, 9 only or non-null ID
    def mc_nonnull_zero(self, outcol, smonth, emonth):
        """
        Function mc_nonnull_zero to look across all MC IDs AND types slots and create an indicator
        if there is any non-null/00 value for type OR any non-null/0-, 8- or 9-only value for ID (for the _SPLMTL flags)

        Function parms:
            outcol=name of outcol (supp flag, will have suffix of smonth_emonth and then all must be combined to get
                    yearly value)
            smonth=start month to loop over
            endmonth=end month to loop over
        """

        z = ",case when "
        for m in range(smonth, emonth + 1):
            mm = str(m).zfill(2)

            for s in range(1, self.de.NMCSLOTS + 1):

                z += f""" nullif(nullif(trim(m{mm}.MC_PLAN_TYPE_CD{s}),''),'00') is not null or
                         (nullif(trim(m{mm}.MC_PLAN_ID{s}),'') is not null and

                         trim(m{mm}.MC_PLAN_ID{s}) not in ('0','00','000','0000','00000','000000','0000000',
                                                '00000000','000000000','0000000000','00000000000','000000000000',
                                                '8','88','888','8888','88888','888888','8888888',
                                                '88888888','888888888','8888888888','88888888888','888888888888',
                                                '9','99','999','9999','99999','999999','9999999',
                                                '99999999','999999999','9999999999','99999999999','999999999999'))
                     """
                if m < emonth or s < self.de.NMCSLOTS:
                    z += " or "
        z += f""" then 1
                else 0
                end as {outcol}_{smonth}_{emonth}"""
        return z

    def any_col(incols: str, outcol, condition='=1'):
        """
        Function any_col to look across a list of columns (non-monthly) to determine if ANY meet a given
        condition. The default condition is = 1.

        Function parms:
            incols=input columns
            outcol=name of column to be output
            condition=monthly condition to be evaulated, where default is = 1
        """

        cols = incols.split(" ")
        listcols = [newcols for newcols in cols if newcols != '']
        cnt = 0
        z = """,case when """
        for col in listcols:
            if cnt > 0:
                z += " or "
            z += f"""{col}{condition}"""
            cnt += 1

        z += f""" then 1 else 0
                end as {outcol}
            """
        return z

    # Create the backbone of unique state/msis_id to then left join each month back to
    # and loop through each month to left join back to the backbone

    def joinmonthly(self):
        """
        Function join_monthly to join the max da_run_ids for the given state/month back to the monthly TAF and
        then join each month by submtg_state_cd and msis_ident_num. Note this table will be pulled into for the subquery in the
        creation of each base and supplemental segment.
        """

        z = f"""(select a.submtg_state_cd,
                b.msis_ident_num,
                count(a.submtg_state_cd) as nmonths

        from max_run_id_bsf_{self.de.YEAR} a
                inner join
                {self.de.DA_SCHEMA}.taf_mon_bsf b

            on a.submtg_state_cd = b.submtg_state_cd and
                a.bsf_fil_dt = b.bsf_fil_dt and
                a.da_run_id = b.da_run_id

            group by a.submtg_state_cd,
                    b.msis_ident_num) as enrl """

        for m in range(1, 13):
            if m < 10:
                m = str(m).zfill(2)
            z += f"""
                left join
                    (select b.* from

                    max_run_id_bsf_{self.de.YEAR} a
                    inner join
                    {self.de.DA_SCHEMA}.taf_mon_bsf b

                    on a.submtg_state_cd = b.submtg_state_cd and
                        a.bsf_fil_dt = b.bsf_fil_dt and
                        a.da_run_id = b.da_run_id

                    where substring(a.bsf_fil_dt,5,2)='{m}') as m{m}

                    on enrl.submtg_state_cd=m{m}.submtg_state_cd and
                       enrl.msis_ident_num=m{m}.msis_ident_num
                """
        return z

    def last_best(self, incol, outcol="", prior=0):
        """
        Function last_best to take the last best value (go backwards in time from month 12 to month 1, taking the first non-missing/null value).

        Function parms:
            incol=input monthly column
            outcol=name of column to be output, where default is the same name of the incol
            prior=indicator to compare current year against prior years (for demographics) to take prior if current year is missing, where default=0
        """

        z = ''
        if outcol == "":
            outcol = incol

        if prior == 0:
            z += f"""
                ,coalesce(m12.{incol}, m11.{incol}, m10.{incol}, m09.{incol},
                            m08.{incol}, m07.{incol}, m06.{incol}, m05.{incol},
                            m04.{incol}, m03.{incol}, m02.{incol}, m01.{incol})
                """

        if prior == 1:
            z += f"""
                    ,coalesce(c.{incol} """
            for p in range(1, len(self.de.PYEARS)+1):
                z += f"""
                    ,p{p}.{incol}
                """
            z += ")"

        z += f""" as {outcol} """
        return z

    def waiv_nonnull(self, outcol):
        """
        Function waiv_nonnull to look across all waiver IDs AND types slots and create an indicator
        if there is any non-null value (for the _SPLMTL flags)

        Function parms:
            outcol=name of outcol (supp flag)
        """

        z = """,case when """
        for m in range(1, 13):
            if m < 10:
                mm = str(m).zfill(2)
            else:
                mm = m
            for s in range(1, self.de.NWAIVSLOTS + 1):
                z += f"""
                    m{mm}.WVR_ID{s} is not null or
                    m{mm}.WVR_TYPE_CD{s} is not null """

                if m < 12 or s < self.de.NWAIVSLOTS:
                    z += " or "

        z += f"""then 1
        else 0
        end as {outcol}
        """
        return z

    def ever_year(self, incol, condition='=1', raw=1, outcol='', usenulls=0, nullcond=''):
        """
        Function ever_year to look across all monthly columns and create an indicator for whether ANY of the monthly
        columns meet the given condition. The default condition is = 1.

        Function parms:
            incol=input monthly column
            condition=monthly condition to be evaulated, where default is = 1
            raw=indicator for whether the monthly variables are raw (must come from the 12 monthly files) or were created
                in an earlier subquery and will therefore have the _MO suffixes, where default = 1
            outcol=name of column to be output, where default is the name of the incol with _EVER appended as a suffix
            usenulls=indicator to determine whether to use the nullif function to compare both nulls AND another value,
                    where default is = 0
            nullcond=additional value to look for when usenulls=1
        """

        if outcol == '':
            outcol = incol + "_EVR"

        if usenulls == 0:

            if raw == 1:

                z = f"""case when
                    m01.{incol} {condition} or
                    m02.{incol} {condition} or
                    m03.{incol} {condition} or
                    m04.{incol} {condition} or
                    m05.{incol} {condition} or
                    m06.{incol} {condition} or
                    m07.{incol} {condition} or
                    m08.{incol} {condition} or
                    m09.{incol} {condition} or
                    m10.{incol} {condition} or
                    m11.{incol} {condition} or
                    m12.{incol} {condition}"""

            if raw == 0:

                z = f"""case when
                    {incol}_01 {condition} or
                    {incol}_02 {condition} or
                    {incol}_03 {condition} or
                    {incol}_04 {condition} or
                    {incol}_05 {condition} or
                    {incol}_06 {condition} or
                    {incol}_07 {condition} or
                    {incol}_08 {condition} or
                    {incol}_09 {condition} or
                    {incol}_10 {condition} or
                    {incol}_11 {condition} or
                    {incol}_12"""

        if usenulls == 1:

            if raw == 1:

                z = f"""case when
                    nullif(m01.{incol}, {nullcond}) {condition} or
                    nullif(m02.{incol}, {nullcond}) {condition} or
                    nullif(m03.{incol}, {nullcond}) {condition} or
                    nullif(m04.{incol}, {nullcond}) {condition} or
                    nullif(m05.{incol}, {nullcond}) {condition} or
                    nullif(m06.{incol}, {nullcond}) {condition} or
                    nullif(m07.{incol}, {nullcond}) {condition} or
                    nullif(m08.{incol}, {nullcond}) {condition} or
                    nullif(m09.{incol}, {nullcond}) {condition} or
                    nullif(m10.{incol}, {nullcond}) {condition} or
                    nullif(m11.{incol}, {nullcond}) {condition} or
                    nullif(m12.{incol}, {nullcond}) {condition}"""
            if raw == 0:

                z = f"""case when
                    nullif({incol}_01, {nullcond}) {condition} or
                    nullif({incol}_02, {nullcond}) {condition} or
                    nullif({incol}_03, {nullcond}) {condition} or
                    nullif({incol}_04, {nullcond}) {condition} or
                    nullif({incol}_05, {nullcond}) {condition} or
                    nullif({incol}_06, {nullcond}) {condition} or
                    nullif({incol}_07, {nullcond}) {condition} or
                    nullif({incol}_08, {nullcond}) {condition} or
                    nullif({incol}_09, {nullcond}) {condition} or
                    nullif({incol}_10, {nullcond}) {condition} or
                    nullif({incol}_11, {nullcond}) {condition} or
                    nullif({incol}_12, {nullcond}) {condition}"""

        return f"{z} then 1 else 0 end as {outcol}"

    def ST_FILTER(self):
        """
        Use the trim function to remove extraneous space characters from start and end of state names.
        """

        return "and trim(submitting_state) not in ('94','96')"

    def max_run_id(self, file="", tbl="", inyear=""):
        """
        Function max_run_id to get the highest da_run_id for the given state for each input monthly TAF (DE or claims). This
        table will then be merged back to the monthly TAF to pull all records for that state, month, and da_run_id.
        It is also inserted into the metadata table to keep a record of the state/month DA_RUN_IDs that make up
        each annual run.
        To get the max run ID, must go to the job control table and get the latest national run, and then also
        get the latest state-specific run. Determine the later by state and month and then pull those IDs.

        Function parms:
            inyear=input year, where the default will be set to the current year but it can be changed to all prior years,
                    for when we need to read in demographic information from all prior years
        """

        if not inyear:
            inyear = self.de.YEAR

        # For NON state-specific runs (where job_parms_text does not include submtg_state_cd in)
        # pull highest da_run_id by time

        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear}_nat AS

            SELECT {file}_fil_dt
                ,max(da_run_id) AS da_run_id
            FROM (
                SELECT substring(job_parms_txt, 1, 4) || substring(job_parms_txt, 6, 2) AS {file}_fil_dt
                    ,da_run_id
                FROM {self.de.DA_SCHEMA_DC}.job_cntl_parms
                WHERE upper(substring(fil_type, 2)) = "{file}"
                    AND sucsfl_ind = 1
                    AND substring(job_parms_txt, 1, 4) = "{inyear}"
        """

        z += f"""
                    AND charindex('submtg_state_cd in', regexp_replace(job_parms_txt, '\\\s+', ' ')) = 0
                )

            GROUP BY {file}_fil_dt
        """
        self.de.append(type(self).__name__, z)

        # For state-specific runs (where job_parms_text includes submtg_state_cd in)
        # pull highest da_run_id by time and state;

        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear}_ss AS

            SELECT {file}_fil_dt
                ,submtg_state_cd
                ,max(da_run_id) AS da_run_id
            FROM (
                SELECT substring(job_parms_txt, 1, 4) || substring(job_parms_txt, 6, 2) AS {file}_fil_dt
                    ,regexp_extract(substring(job_parms_txt, 10), '([0-9]{{2}})') AS submtg_state_cd
                    ,da_run_id
                FROM {self.de.DA_SCHEMA_DC}.job_cntl_parms
                WHERE upper(substring(fil_type, 2)) = "{file}"
                    AND sucsfl_ind = 1
                    AND substring(job_parms_txt, 1, 4) = "{inyear}"
        """

        z += f"""
                    AND charindex('submtg_state_cd in', regexp_replace(job_parms_txt, '\\\s+', ' ')) > 0
                )

            GROUP BY {file}_fil_dt
                ,submtg_state_cd
        """
        self.de.append(type(self).__name__, z)

        # Now join the national and state lists by month - take the national run ID if higher than
        # the state-specific, otherwise take the state-specific
        # Must ALSO stack with the national IDs so they are not lost
        # In outer query, get a list of unique IDs to pull

        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW job_cntl_parms_both_{file}_{inyear} AS

            SELECT DISTINCT {file}_fil_dt
                ,da_run_id
            FROM (
                SELECT coalesce(a.{file}_fil_dt, b.{file}_fil_dt) AS {file}_fil_dt
                    ,CASE
                        WHEN a.da_run_id > b.da_run_id
                            OR b.da_run_id IS NULL
                            THEN a.da_run_id
                        ELSE b.da_run_id
                        END AS da_run_id
                FROM max_run_id_{file}_{inyear}_nat a
                FULL JOIN max_run_id_{file}_{inyear}_ss b ON a.{file}_fil_dt = b.{file}_fil_dt

                UNION ALL

                SELECT {file}_fil_dt
                    ,da_run_id
                FROM max_run_id_{file}_{inyear}_nat
                ) c
        """
        self.de.append(type(self).__name__, z)

        # Now join to EFTS data to get table of month/state/run IDs to use for data pull
        # Note must then take the highest da_run_id by state/month (if any state-specific runs
        # were identified as being later than a national run)
        # Note for DE only, strip off month from fil_dt

        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear} AS

            SELECT
        """

        if file.casefold() == "de":
            z += f"""
                 substring(a.{file}_fil_dt, 1, 4) AS {file}_fil_dt
            """
        else:
            z += f"""
                a.{file}_fil_dt
            """

        z += f"""
                ,b.submtg_state_cd
                ,max(b.da_run_id) AS da_run_id
                ,max(b.fil_cret_dt) AS fil_cret_dt
            FROM job_cntl_parms_both_{file}_{inyear} a
            INNER JOIN (
                SELECT da_run_id
                    ,incldd_state_cd AS submtg_state_cd
                    ,fil_cret_dt
                FROM {self.de.DA_SCHEMA_DC}.efts_fil_meta
                WHERE incldd_state_cd != 'Missing'
                ) b ON a.da_run_id = b.da_run_id
        """

        if self.ST_FILTER().count("ALL"):
            z += f"""
                 WHERE {self.ST_FILTER()}
                 """
        z += f"""
            GROUP BY a.{file}_fil_dt
                ,b.submtg_state_cd
        """
        self.de.append(type(self).__name__, z)

        # Insert into metadata table so we keep track of all monthly DA_RUN_IDs (both DE and claims)
        # that go into each annual UP file

        z = f"""
            INSERT INTO {self.de.DA_SCHEMA_DC}.TAF_ANN_INP_SRC
            SELECT
                 {self.de.DA_RUN_ID} AS ANN_DA_RUN_ID
                ,'ade' as ann_fil_type
                ,SUBMTG_STATE_CD
                ,lower('{file}') as src_fil_type
                ,{file}_FIL_DT as src_fil_dt
                ,DA_RUN_ID AS SRC_DA_RUN_ID
                ,fil_cret_dt as src_fil_creat_dt
            FROM max_run_id_{file}_{inyear}
        """
        self.de.append(type(self).__name__, z)

    def create_dates_out_root(self):
        """
        Create dates_out by extracting and combining data from MDCD_dates_out and CHIP_dates_out.
        """

        from taf.DE.DE0002 import DE0002
        z = """create or replace temporary view dates_out as
                (select msis_ident_num,
                        submtg_state_cd,
                        ENRL_TYPE_FLAG,
                        MDCD_ENRLMT_EFF_DT as ENRLMT_EFCTV_CY_DT,
                        MDCD_ENRLMT_END_DT as ENRLMT_END_CY_DT
                from MDCD_dates_out)

                union all

                (select msis_ident_num,
                        submtg_state_cd,
                        ENRL_TYPE_FLAG,
                        CHIP_ENRLMT_EFF_DT as ENRLMT_EFCTV_CY_DT,
                        CHIP_ENRLMT_END_DT as ENRLMT_END_CY_DT
                from CHIP_dates_out)

        """
        self.de.append(type(self).__name__, z)

        extra_cols = ['ENRL_TYPE_FLAG',
                      'ENRLMT_EFCTV_CY_DT',
                      'ENRLMT_END_CY_DT'
                     ]
        z = f"""insert into {self.de.DA_SCHEMA}.taf_ann_de_{DE0002.tbl_abrv}
                select
                    {DE.table_id_cols_pre(self, suffix="", extra_cols=extra_cols)}
                    {DE.table_id_cols_sfx(self)}
                from dates_out
                """

        self.de.append(type(self).__name__, z)

    def drop_table(self, tblname):
        """
        Function drop_tables to drop temp tables.

        Function parms:
            temptables=list of tables to drop
        """

        z = f"""drop table {self.de.DA_SCHEMA_DC}.{tblname}"""
        self.de.append(type(self).__name__, z)

    def create_pyears(self):
        """
        Function create_pyears to create a list of all prior years (from current year minus 1 to 2014).
        Note for 2014 the list will be empty.
        """

        pyears = []

        for py in range(self.de.YEAR-1, self.de.YEAR-3,-1):
            pyears.append(py)

        self.de.PYEARS.extend(pyears)

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
