from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner


class DE0002(DE):
    def __init__(self, de: DE_Runner):
        DE.__init__(self, de, "DE00002", "TMSIS_PRMRY_DMGRPHC_ELGBLTY", "PRMRY_DMGRPHC_ELE_EFCTV_DT", "PRMRY_DMGRPHC_ELE_END_DT")

    def dates(self):
        super().create(self)

        z = f"""create or replace temporary view {self.dtype}_dates_long as
                select submtg_state_cd
                        ,msis_ident_num
                        ,{self.dtype}_ENRLMT_EFF_DT
                        ,{self.dtype}_ENRLMT_END_DT
                    from (
                    select *
                    """
        for m in range(1, 13):
            mm = "{:02d}".format(m)
            for a in range(1, 16):
                aa = "{:02d}".format(a)

                z += f"""
                    SELECT
                        ,CASE
                        WHEN {mm} = 1 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_01
                        ,CASE
                        WHEN {mm} = 2 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_02
                        ,CASE
                        WHEN {mm} = 3 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_03
                        ,CASE
                        WHEN {mm} = 4 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_04
                        ,CASE
                        WHEN {mm} = 5 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_05
                        ,CASE
                        WHEN {mm} = 6 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_06
                        ,CASE
                        WHEN {mm} = 7 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_07
                        ,CASE
                        WHEN {mm} = 8 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_08
                        ,CASE
                        WHEN {mm} = 9 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_09
                        ,CASE
                        WHEN {mm} = 10 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_10
                        ,CASE
                        WHEN {mm} = 11 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_11
                        ,CASE
                        WHEN {mm} = 12 THEN 1 ELSE cast(NULL AS INTEGER)
                        END AS {self.dtype}_ENRLMT_EFF_DT_12
                from
                (select a.submtg_state_cd
                       ,a.msis_ident_num
                """
                for m in range(1, 13):
                    mm = "{:02d}".format(m)
                    for a in range(1, 16):
                        aa = "{:02d}".format(a)

                        z += f"""
                            ,a.{self.dtype}_ENRLMT_EFF_DT_{aa}_{mm}
                            ,a.{self.dtype}_ENRLMT_END_DT_{aa}_{mm}
                ,b.slot
                ,b.month

        from eligibility_dates_{self.dtype} a
                join
                numbers b
                on true) sub ) sub2

        where {self.dtype}_ENRLMT_EFF_DT is not null

        order by submtg_state_cd,
                    msis_ident_num,
                    {self.dtype}_ENRLMT_EFF_DT,
                    {self.dtype}_ENRLMT_END_DT"""

        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view {self.dtype}_ids as
            select *

            /* Create a unique date ID to filter on later */

                ,trim(submtg_state_cd ||'-'||msis_ident_num || '-' ||cast(row_number() over
                    (partition by submtg_state_cd, msis_ident_num
                    order by submtg_state_cd, msis_ident_num, {self.dtype}_ENRLMT_EFF_DT, {self.dtype}_ENRLMT_END_DT)
                    as char(3))) as dateId

            from {self.dtype}_dates_long"""

        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view {self.dtype}_overlaps as
                select t1.*
                from {self.dtype}_ids t1
                    inner join
                    {self.dtype}_ids t2

                /* Join records for beneficiary to each other, but omit matches where it's the same record */

                    on t1.submtg_state_cd = t2.submtg_state_cd and
                        t1.msis_ident_num = t2.msis_ident_num and
                        t1.dateId <> t2.dateId

                /* Get every dateID where their effective date is greater than or equal to another record's effective date
                    AND their end date is less than or equal to that other record's end date. */

                where date_cmp(t1.{self.dtype}_ENRLMT_EFF_DT,t2.{self.dtype}_ENRLMT_EFF_DT) in (0,1) and
                    date_cmp(t1.{self.dtype}_ENRLMT_END_DT,t2.{self.dtype}_ENRLMT_END_DT) in (-1,0)"""

        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view {self.dtype}_nonoverlaps as
                select t1.*

                /* Join initial date to overlapping dateIDs and remove */

                from {self.dtype}_ids t1

                    left join
                    {self.dtype}_overlaps t2

                on t1.dateid = t2.dateid

                where t2.dateid is null"""

        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view {self.dtype}_dates_out as
            select submtg_state_cd
                ,msis_ident_num
                ,{self.dval} as ENRL_TYPE_FLAG
                ,min({self.dtype}_ENRLMT_EFF_DT) as {self.dtype}_ENRLMT_EFF_DT
                ,max({self.dtype}_ENRLMT_END_DT) as {self.dtype}_ENRLMT_END_DT

            from
            (
            select submtg_state_cd,
            msis_ident_num,
            {self.dtype}_ENRLMT_EFF_DT,
            {self.dtype}_ENRLMT_END_DT
            ,sum(C) over (partition by submtg_state_cd, msis_ident_num
                            order by {self.dtype}_ENRLMT_EFF_DT, {self.dtype}_ENRLMT_END_DT
                            rows UNBOUNDED PRECEDING) as G
            from
            (
            select submtg_state_cd
                ,msis_ident_num
                ,{self.dtype}_ENRLMT_EFF_DT
                ,{self.dtype}_ENRLMT_END_DT
                ,m_eff_dt
                ,m_end_dt
                ,decode(sign({self.dtype}_ENRLMT_EFF_DT-nvl(m_end_dt+1,{self.dtype}_ENRLMT_EFF_DT)),1,1,0) as C
            from

            (select submtg_state_cd
                ,msis_ident_num
                ,{self.dtype}_ENRLMT_EFF_DT
                ,{self.dtype}_ENRLMT_END_DT
                ,lag({self.dtype}_ENRLMT_EFF_DT) over (partition by submtg_state_cd, msis_ident_num
                                                    order by {self.dtype}_ENRLMT_EFF_DT, {self.dtype}_ENRLMT_END_DT)
                                                    as m_eff_dt
                ,lag({self.dtype}_ENRLMT_END_DT) over (partition by submtg_state_cd, msis_ident_num
                                                    order by {self.dtype}_ENRLMT_EFF_DT, {self.dtype}_ENRLMT_END_DT)
                                                    as m_end_dt
            from {self.dtype}_nonoverlaps
                order by {self.dtype}_ENRLMT_EFF_DT, {self.dtype}_ENRLMT_END_DT) s1 ) s2 ) s3

                group by submtg_state_cd, msis_ident_num, g"""

        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view {self.dtype}_enrolled_days
		    distkey(msis_ident_num)
		    sortkey(submtg_state_cd,msis_ident_num) as

			select submtg_state_cd,
			       msis_ident_num,
				   {self.dtype}_ENRLMT_EFF_DT,
				   {self.dtype}_ENRLMT_END_DT

			/* Loop through months and compare effective date to first day of month, and end date to last day of month.
			   If at all within month, count the number of days */
            """
        for m in range(1, 12):
            mm = str(m)
            if len(mm) == 1:
                mm.zfill(2)

            if mm in ("01", "03", "05", "07", "08", "10", "12"):
                lday = "31"
            if mm in ("09", "04", "06", "11"):
                lday = 30
            if mm == "02" and self.year % 4 == 0 and (self.year % 100 != 0 or self.year % 400 == 0):
                lday = 29
            elif mm == "02":
                lday = 28

            z += f"""case when DATEDIFF({self.dtype}_ENRLMT_EFF_DT,to_date('{lday} {mm} {self.year}'),'dd mm yyyy')) in (-1,0) and
                            DATEDIFF({self.dtype}_ENRLMT_END_DT,to_date('01 {mm} {self.year}'),'dd mm yyyy')) in (0,1) then

                        datediff(day,greatest({self.dtype}_ENRLMT_EFF_DT,to_date('01 {mm} {self.year}'),'dd mm yyyy')),
                                least({self.dtype}_ENRLMT_END_DT,to_date('{lday} {mm} {self.year}'),'dd mm yyyy'))) + 1

                        else 0
                        end as {self.dtype}_ENRLMT_DAYS_{mm}
                    from {self.dtype}_dates_out"""

        self.de.append(type(self).__name__, z)

        z += f"""create or replace temporary view {self.dtype}_days_out

                select *,
                    {self.dtype}_ENRLMT_DAYS_01 + {self.dtype}_ENRLMT_DAYS_02 + {self.dtype}_ENRLMT_DAYS_03 + {self.dtype}_ENRLMT_DAYS_04 +
                    {self.dtype}_ENRLMT_DAYS_05 + {self.dtype}_ENRLMT_DAYS_06 + {self.dtype}_ENRLMT_DAYS_07 + {self.dtype}_ENRLMT_DAYS_08 +
                    {self.dtype}_ENRLMT_DAYS_09 + {self.dtype}_ENRLMT_DAYS_10 + {self.dtype}_ENRLMT_DAYS_11 + {self.dtype}_ENRLMT_DAYS_12
                    as {self.dtype}_ENRLMT_DAYS_YR

                from
                (select submtg_state_cd,
                        msis_ident_num
                """
        for m in range(1, 12):
            mm = str(m)
            z += f""",sum({self.dtype}_ENRLMT_DAYS_{mm}) as {self.dtype}_ENRLMT_DAYS_{mm}"""

        z += f"""from {self.dtype}_enrolled_days
             group by submtg_state_cd,
                      msis_ident_num )"""

    def create():
        pass
