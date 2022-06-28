from taf.DE.DE_Runner import DE_Runner
from taf.TAF import TAF


class DE(TAF):

    def __init__(self, runner: DE_Runner):
        self.de = runner
        self.main_id = runner.main_id

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):
        self.create_pyears()
        self.max_run_id(file="DE", tbl="taf_ann_de_base", inyear=self.de.YEAR)
        self.max_run_id(file="DE", inyear=self.de.YEAR)
        self.max_run_id(file="BSF", inyear=self.de.YEAR)
        self.max_run_id(file="IP", inyear=self.de.YEAR)
        self.max_run_id(file="IP", inyear=self.de.PYEAR)
        self.max_run_id(file="IP", inyear=self.de.FYEAR)
        self.max_run_id(file="LT", inyear=self.de.YEAR)
        self.max_run_id(file="OT", inyear=self.de.YEAR)
        self.max_run_id(file="RX", inyear=self.de.YEAR)
        pass

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
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
        priorities = ["01", "04", "05", "06", "15", "07", "14", "17", "08", "09", "10",
                      "11", "12", "13", "18", "16", "02", "03", "60", "70", "80", "99"]

        z = ""

        for mm in range(smonth, emonth):

            if mm < 10:
                mm = str(mm).zfill(2)

            z += ",case "
            for p in priorities:
                z += " when ("

                for s in range(1, self.de.NMCSLOTS - 1):
                    z += f"""m{mm}.MC_PLAN_TYPE_CD{s} = '{p}'"""
                    if s < self.de.NMCSLOTS - 2:
                        z += " or "
                z += f""" ) then ('{p}')"""
            z += f""" else null
            end as MC_PLAN_TYPE_CD_{mm}
            """
        return z

    def misg_enrlm_type():
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

    def assign_nonmiss_month(self, outcol, monthval1, incol1, monthval2='', incol2=''):

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
        z = """,case when ELGBL_LINE_1_ADR_MAIL_MN != '00' and ELGBL_LINE_1_ADR_HOME_MN = '00' then 1
                    when ELGBL_LINE_1_ADR_HOME_MN != '00' then 0
                else null
                end as ELGBL_ADR_MAIL_FLAG
            """
        return z

    def address_same_year(self, incol):
        cnt = 0
        z = f""",case when yearpull = {self.de.YEAR} then c.{incol}"""
        for pyear in self.de.PYEARS:
            cnt += 1
            z += f""" when yearpull = {pyear} then p{cnt}.{incol}"""

        z += f""" else null
                end as {incol}"""
        return z

    def unique_claims_ids(self, cltype):
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
                    substring(msis_ident_num,1,1) != ''
            """
        return z

    def table_id_cols_sfx(self, suffix="", extra_cols=[]):
        z = f"""cast ('{self.de.DA_RUN_ID}' || '-' || '{self.de.YEAR}' || '-' || '{self.de.VERSION}' || '-' ||
            SUBMTG_STATE_CD{suffix} || '-' || MSIS_IDENT_NUM{suffix} as varchar(40)) as DE_LINK_KEY
            ,'{self.de.YEAR}' as DE_FIL_DT
            ,'{self.de.VERSION}' as ANN_DE_VRSN
            ,MSIS_IDENT_NUM"""
        if len(extra_cols) > 0:
            z += ","
        z += f"""
            {",".join(extra_cols)}
            ,to_timestamp('{self.de.DA_RUN_ID}', 'yyyyMMddHHmmss') as REC_ADD_TS
            ,current_timestamp() as REC_UPDT_TS
            ,{self.de.DA_RUN_ID} as DA_RUN_ID
            ,SUBMTG_STATE_CD
            """
        return z

    def monthly_array_eldts(self, incol, outcol, nslots=16, truncfirst=1):
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
                            datediff(m{m}.{incol}{snum},to_date('01 {m} {self.de.YEAR}','dd m yyyy')) >= 1
                        then to_date('01 {m} {self.de.YEAR}','dd m yyyy')
                        else m{m}.{incol}{snum}
                        end as {outcol}{snum}_{m}
                        """

                # Truncate end dates to the last day of the month if after the last day of the month. Otherwise pull in the
                # raw date
                if truncfirst == 0:
                    z += f"""
                        ,case when m{m}.{incol}{snum} is not null and
                            datediff(m{m}.{incol}{snum},to_date('{lday} {m} {self.de.YEAR}','dd m yyyy')) <= -1
                        then to_date('{lday} {m} {self.de.YEAR}','dd m yyyy')
                        else m{m}.{incol}{snum}
                        end as {outcol}{snum}_{m}
                        """
        return z

    def mc_waiv_slots(self, incol, values, outcol, smonth=1, emonth=12):
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
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'60'", outcol='ACNTBL_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'70'", outcol='HM_HOME_MC_PLAN', smonth=_smonth, emonth=_emonth)}
                {DE.mc_waiv_slots(self, 'MC_PLAN_TYPE_CD', values="'80'", outcol='IC_DUALS_MC_PLAN', smonth=_smonth, emonth=_emonth)}"""
        return z

    def sum_months(self, incol, raw=0, outcol=""):

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
        z += f"""then 1
                else 0
                end as {outcol}_{smonth}_{emonth}"""
        return z

    def any_col(incols: str, outcol, condition='=1'):

        cols = incols.split(" ")
        listcols = [newcols for newcols in cols if newcols != '']
        cnt = len(listcols)
        z = """,case when """
        for c in range(1, cnt + 1):
            if c > 1:
                z += " or "
            z += f"""c {condition}"""

        z += f"""then 1 else 0
                end as {outcol}
            """
        return z

    # Create the backbone of unique state/msis_id to then left join each month back to
    # and loop through each month to left join back to the backbone

    def joinmonthly(self):
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
            for p in self.de.PYEARS:
                z += """
                    ,p{p}.{incol}
                """
            z += ")"

        z += f""" as {outcol} """
        return z

    def waiv_nonnull(self, outcol):
        z = """,case when """
        for m in range(1, 13):
            if m < 10:
                mm = str(m).zfill(2)
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

        if outcol == '':
            outcol = incol

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
                    nullif(m01. {incol}, {nullcond}) {condition} or
                    nullif(m02. {incol}, {nullcond}) {condition} or
                    nullif(m03. {incol}, {nullcond}) {condition} or
                    nullif(m04. {incol}, {nullcond}) {condition} or
                    nullif(m05. {incol}, {nullcond}) {condition} or
                    nullif(m06. {incol}, {nullcond}) {condition} or
                    nullif(m07. {incol}, {nullcond}) {condition} or
                    nullif(m08. {incol}, {nullcond}) {condition} or
                    nullif(m09. {incol}, {nullcond}) {condition} or
                    nullif(m10. {incol}, {nullcond}) {condition} or
                    nullif(m11. {incol}, {nullcond}) {condition} or
                    nullif(m12. {incol}, {nullcond}) {condition}"""

            if raw == 0:

                z = f"""case when
                    nullif( {incol}_01, {nullcond}) {condition} or
                    nullif( {incol}_02, {nullcond}) {condition} or
                    nullif( {incol}_03, {nullcond}) {condition} or
                    nullif( {incol}_04, {nullcond}) {condition} or
                    nullif( {incol}_05, {nullcond}) {condition} or
                    nullif( {incol}_06, {nullcond}) {condition} or
                    nullif( {incol}_07, {nullcond}) {condition} or
                    nullif( {incol}_08, {nullcond}) {condition} or
                    nullif( {incol}_09, {nullcond}) {condition} or
                    nullif( {incol}_10, {nullcond}) {condition} or
                    nullif( {incol}_11, {nullcond}) {condition} or
                    nullif( {incol}_12, {nullcond}) {condition}"""

        return f"{z} then 1 else 0 end as {outcol}"

    def ST_FILTER(self):
        return "and trim(submitting_state) not in ('94','96')"

    def max_run_id(self, file="", tbl="", inyear=""):
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
                FROM {self.de.DA_SCHEMA}.job_cntl_parms
                WHERE upper(substring(fil_type, 2)) = "{file}"
                    AND sucsfl_ind = 1
                    AND substring(job_parms_txt, 1, 4) = "{inyear}"
        """

        if inyear == self.de.PYEAR:
            z += """
                    AND substring(job_parms_txt, 6, 2) IN (
                            '10'
                            ,'11'
                            ,'12'
                            )
            """

        if inyear == self.de.FYEAR:
            z += """
                    AND substring(job_parms_txt, 6, 2) IN (
                        '01'
                        ,'02'
                        ,'03'
                        )
            """

        z += f"""
                    AND charindex('submtg_state_cd in', regexp_replace(job_parms_txt, '\\s+', ' ')) = 0
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
                    ,regexp_extract(substring(job_parms_txt, 10), '([0-9]{2})') AS submtg_state_cd
                    ,da_run_id
                FROM {self.de.DA_SCHEMA}.job_cntl_parms
                WHERE upper(substring(fil_type, 2)) = "{file}"
                    AND sucsfl_ind = 1
                    AND substring(job_parms_txt, 1, 4) = "{inyear}"
        """

        if inyear == self.de.PYEAR:
            z += """
                 AND substring(job_parms_txt, 6, 2) IN (
                            '10'
                            ,'11'
                            ,'12'
                            )
            """

        if inyear == self.de.FYEAR:
            z += """
                 AND substring(job_parms_txt, 6, 2) IN (
                        '01'
                        ,'02'
                        ,'03'
                        )
            """

        z += f"""
                    AND charindex('submtg_state_cd in', regexp_replace(job_parms_txt, '\\s+', ' ')) > 0
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
            FROM job_cntl_parms_both_{file}_{inyear} a
            INNER JOIN (
                SELECT da_run_id
                    ,incldd_state_cd AS submtg_state_cd
                FROM {self.de.DA_SCHEMA}.efts_fil_meta
                WHERE incldd_state_cd != 'Missing'
                ) b ON a.da_run_id = b.da_run_id
        """

        if self.ST_FILTER().count("ALL"):
            z += f"""WHERE {self.ST_FILTER()}
            """
        z += f"""
            GROUP BY a.{file}_fil_dt
                ,b.submtg_state_cd
        """
        self.de.append(type(self).__name__, z)

    def create_dates_out_root(self):
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
                    {DE.table_id_cols_sfx(self, suffix="", extra_cols=extra_cols)}
                from dates_out
                """

        self.de.append(type(self).__name__, z)

    def drop_table(self, tblname):
        z = f"""drop table {self.de.DA_SCHEMA}.{tblname}"""
        self.de.append(type(self).__name__, z)

    # Macro create_pyears to create a list of all prior years (from current year minus 1 to 2014).
    # Note for 2014 the list will be empty.
    def create_pyears(self):
        pyears = []

        for py in range(2014, self.de.YEAR):
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
