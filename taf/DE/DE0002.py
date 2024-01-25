from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner


class DE0002(DE):
    """
    Description:  Generate the annual DE segment 002: Eligibility Dates

    Notes:  This program reads in the 16 monthly effective/end date pairs for both Medicaid and CHIP,
            and takes from wide to long and combines/dedups overlapping or duplicated spells.
            It then inserts this temp table with one record/enrollee/spell into the permanent table.
            In the process, it creates monthly and yearly counts of enrolled days (Medicaid and 
            CHIP separately) to be added to the Base segment. This temp table is called
            enrolled_days_#YR and is the only temp table not deleted at the end of the program.
    """

    tblname: str = "eligibility_dates"
    tbl_abrv: str = "eldts"

    def __init__(self, runner: DE_Runner):
        DE.__init__(self, runner)
        self.de = runner

    #def __init__(self, de: DE_Runner):
        #super().__init__(de)

    def create(self):
        """
        Read in all eligibility dates - all 16 slots from all 12 months. The monthly_array_eldts Function
        truncates anything BEFORE the first of the month to the first of the month, and anything AFTER
        the last day of the month to the last day of the month. Must
        run each element as a separate subcol param because of Function text length issues.
        """

        super().create()
        self.numbers()
        self.create_temp(self.tblname)

        self.eligibility_dates('MDCD', 1)
        self.eligibility_dates('CHIP', 2)
        # Call this here to create the dates_out table to instert into
        self.create_dates_out_root()

    def create_temp(self, tname):
        """
        Create temporary table.
        """

        s = DE.monthly_array_eldts(self, incol='MDCD_ENRLMT_EFF_DT_', outcol="", nslots=16, truncfirst=1)
        s2 = DE.monthly_array_eldts(self, incol='MDCD_ENRLMT_END_DT_', outcol="", nslots=16, truncfirst=0)
        s3 = DE.monthly_array_eldts(self, incol='CHIP_ENRLMT_EFF_DT_', outcol="", nslots=16, truncfirst=1)
        s4 = DE.monthly_array_eldts(self, incol='CHIP_ENRLMT_END_DT_', outcol="", nslots=16, truncfirst=0)

        self.create_temp_table(tblname=tname, inyear=self.de.YEAR, subcols=s,
                               subcols2=s2, subcols3=s3, subcols4=s4)

    def numbers(self):
        """
        Create dummy table with one record per slot/month to join to dates and get to long form.
        """

        z = f"""create table if not exists {self.de.DA_SCHEMA}.numbers
                (slot int, month string)
                using delta"""
        self.de.append(type(self).__name__, z)

        z = f"""insert into {self.de.DA_SCHEMA}.numbers
                    values"""
        for s in range(1, 17):
            for m in range(1, 13):
                mm = str(m)
                if len(mm) == 1:
                    mm="0"+mm
                z += f"""({s}, '{mm}')"""
                if s < 16 or m < 12:
                    z += ","
        self.de.append(type(self).__name__, z)

        z = f"""CONVERT TO DELTA {self.de.DA_SCHEMA}.numbers"""
        self.de.append(type(self).__name__, z)

        z = f"""OPTIMIZE {self.de.DA_SCHEMA}.numbers ZORDER BY (slot, month)"""
        self.de.append(type(self).__name__, z)

        z = f"""VACUUM {self.de.DA_SCHEMA}.numbers"""
        self.de.append(type(self).__name__, z)

    def eligibility_dates(self, dtype, dval):
        """
        Get eligibility dates.  
        """

        z = f"""create or replace temporary view {dtype}_dates_long as
                select submtg_state_cd
                        ,msis_ident_num
                        ,{dtype}_ENRLMT_EFF_DT
                        ,{dtype}_ENRLMT_END_DT
                    from (
                    select *
                            ,case
                    """
        for s in range(1, 17):
            for m in range(1, 13):
                m = str(m).zfill(2)
                z += f""" when slot={s} and month='{m}' then {dtype}_ENRLMT_EFF_DT_{s}_{m}"""
        z += f""" end as {dtype}_ENRLMT_EFF_DT
                ,case
              """
        for s in range(1, 17):
            for m in range(1, 13):
                m = str(m).zfill(2)
                z += f""" when slot={s} and month='{m}' then {dtype}_ENRLMT_END_DT_{s}_{m}"""
        z += f""" end as {dtype}_ENRLMT_END_DT
                from
                (select a.submtg_state_cd
                       ,a.msis_ident_num
                """
        for s in range(1, 17):
            for m in range(1, 13):
                m = str(m).zfill(2)
                z += f"""
                    ,a.{dtype}_ENRLMT_EFF_DT_{s}_{m}
                    ,a.{dtype}_ENRLMT_END_DT_{s}_{m}
                """
        z += f""",b.slot
                ,b.month
            from eligibility_dates_{self.de.YEAR} a
                join
                {self.de.DA_SCHEMA}.numbers b
                on true) sub ) sub2

        where {dtype}_ENRLMT_EFF_DT is not null

        order by submtg_state_cd,
                    msis_ident_num,
                    {dtype}_ENRLMT_EFF_DT,
                    {dtype}_ENRLMT_END_DT"""

        self.de.append(type(self).__name__, z)
        print(f"""Creating Temp Table: {dtype}_dates_long""")

        # Create a unique date ID to filter on later
        z = f"""create or replace temporary view {dtype}_ids as
            select *
                ,trim(submtg_state_cd ||'-'||msis_ident_num || '-' ||cast(row_number() over
                    (partition by submtg_state_cd, msis_ident_num
                    order by submtg_state_cd, msis_ident_num, {dtype}_ENRLMT_EFF_DT, {dtype}_ENRLMT_END_DT)
                    as char(3))) as dateId

            from {dtype}_dates_long"""

        self.de.append(type(self).__name__, z)

        # Join records for beneficiary to each other, but omit matches where it's the same record */
        # Get every dateID where their effective date is greater than or equal to another record's effective date
        # AND their end date is less than or equal to that other record's end date.
        z = f"""create or replace temporary view {dtype}_overlaps as
                select t1.*
                from {dtype}_ids t1
                    inner join
                    {dtype}_ids t2

                    on t1.submtg_state_cd = t2.submtg_state_cd and
                        t1.msis_ident_num = t2.msis_ident_num and
                        t1.dateId <> t2.dateId

                where datediff(t1.{dtype}_ENRLMT_EFF_DT,t2.{dtype}_ENRLMT_EFF_DT) >= 0 and
                    datediff(t1.{dtype}_ENRLMT_END_DT,t2.{dtype}_ENRLMT_END_DT) <= 0"""

        self.de.append(type(self).__name__, z)

        # Join initial date to overlapping dateIDs and remove
        z = f"""create or replace temporary view {dtype}_nonoverlaps as
                select t1.*
                    from {dtype}_ids t1

                    left join
                    {dtype}_overlaps t2

                on t1.dateid = t2.dateid

                where t2.dateid is null"""

        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view {dtype}_dates_out as
            select submtg_state_cd
                ,msis_ident_num
                ,{dval} as ENRL_TYPE_FLAG
                ,min({dtype}_ENRLMT_EFF_DT) as {dtype}_ENRLMT_EFF_DT
                ,max({dtype}_ENRLMT_END_DT) as {dtype}_ENRLMT_END_DT

            from
            (
            select submtg_state_cd,
            msis_ident_num,
            {dtype}_ENRLMT_EFF_DT,
            {dtype}_ENRLMT_END_DT
            ,sum(C) over (partition by submtg_state_cd, msis_ident_num
                            order by {dtype}_ENRLMT_EFF_DT, {dtype}_ENRLMT_END_DT
                            rows UNBOUNDED PRECEDING) as G
            from
            (
            select submtg_state_cd
                ,msis_ident_num
                ,{dtype}_ENRLMT_EFF_DT
                ,{dtype}_ENRLMT_END_DT
                ,m_eff_dt
                ,m_end_dt
                ,decode(sign(datediff(cast({dtype}_ENRLMT_EFF_DT as DATE), nvl(cast(m_end_dt+1 as DATE),cast({dtype}_ENRLMT_EFF_DT as DATE)))), 1, 1, 0) as C
            from

            (select submtg_state_cd
                ,msis_ident_num
                ,{dtype}_ENRLMT_EFF_DT
                ,{dtype}_ENRLMT_END_DT
                ,lag({dtype}_ENRLMT_EFF_DT) over (partition by submtg_state_cd, msis_ident_num
                                                    order by {dtype}_ENRLMT_EFF_DT, {dtype}_ENRLMT_END_DT)
                                                    as m_eff_dt
                ,lag({dtype}_ENRLMT_END_DT) over (partition by submtg_state_cd, msis_ident_num
                                                    order by {dtype}_ENRLMT_EFF_DT, {dtype}_ENRLMT_END_DT)
                                                    as m_end_dt
            from {dtype}_nonoverlaps
                order by {dtype}_ENRLMT_EFF_DT, {dtype}_ENRLMT_END_DT) s1 ) s2 ) s3

                group by submtg_state_cd, msis_ident_num, g"""

        self.de.append(type(self).__name__, z)

        # Loop through months and compare effective date to first day of month, and end date to last day of month.
        # If at all within month, count the number of days
        z = f"""create or replace temporary view {dtype}_enrolled_days as

            select submtg_state_cd
                    ,msis_ident_num
                    ,{dtype}_ENRLMT_EFF_DT
                    ,{dtype}_ENRLMT_END_DT

            """
        for m in range(1, 13):
            lday = "31"
            mm = str(m)
            if len(mm) == 1:
                mm = mm.zfill(2)

            if mm in ("01", "03", "05", "07", "08", "10", "12"):
                lday = "31"
            if mm in ("09", "04", "06", "11"):
                lday = "30"
            if mm == "02" and self.de.YEAR % 4 == 0 and (self.de.YEAR % 100 != 0 or self.de.YEAR % 400 == 0):
                lday = "29"
            elif mm == "02":
                lday = "28"

            z += f""",case
                        when datediff({dtype}_ENRLMT_EFF_DT,to_date('{lday} {mm} {self.de.YEAR}','dd MM yyyy')) <= 0 and
                             datediff({dtype}_ENRLMT_END_DT,to_date('01 {mm} {self.de.YEAR}','dd MM yyyy')) >= 0
                        then
                            datediff(least({dtype}_ENRLMT_END_DT,to_date('{lday} {mm} {self.de.YEAR}','dd MM yyyy')),
                            greatest({dtype}_ENRLMT_EFF_DT,to_date('01 {mm} {self.de.YEAR}','dd MM yyyy'))) + 1

                        else 0
                        end as {dtype}_ENRLMT_DAYS_{mm}
                    """
        z += f"""
            from {dtype}_dates_out
              """

        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view {dtype}_days_out as
                select *,
                    {dtype}_ENRLMT_DAYS_01 + {dtype}_ENRLMT_DAYS_02 + {dtype}_ENRLMT_DAYS_03 + {dtype}_ENRLMT_DAYS_04 +
                    {dtype}_ENRLMT_DAYS_05 + {dtype}_ENRLMT_DAYS_06 + {dtype}_ENRLMT_DAYS_07 + {dtype}_ENRLMT_DAYS_08 +
                    {dtype}_ENRLMT_DAYS_09 + {dtype}_ENRLMT_DAYS_10 + {dtype}_ENRLMT_DAYS_11 + {dtype}_ENRLMT_DAYS_12
                    as {dtype}_ENRLMT_DAYS_YR

                from
                (select submtg_state_cd,
                        msis_ident_num
                """
        for m in range(1, 13):
            mm = str(m).zfill(2)
            z += f""",sum({dtype}_ENRLMT_DAYS_{mm}) as {dtype}_ENRLMT_DAYS_{mm}"""

        z += f"""
                from {dtype}_enrolled_days
                group by submtg_state_cd,
                      msis_ident_num )"""

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
# <http://creativecommons.org/publicdomain/zero/1.0/>
