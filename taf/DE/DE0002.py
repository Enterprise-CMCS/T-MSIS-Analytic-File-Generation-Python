from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner


class DE0002(DE):
    tblname: str = "eligibilitiy_dates"

    def __init__(self, de: DE_Runner):
        DE.__init__(self, de)

    def create(self):
        super().create(self)
        self.create_temp(self, self.tblname)
        self.eligibility_dates('MDCD', 1)
        self.eligibility_dates('CHIP', 2)

    def create_temp(self, tblname):
        s = DE.monthly_array_eldts('MDCD_ENRLMT_EFF_DT_', nslots=16, truncfirst=1)
        s2 = DE.monthly_array_eldts('MDCD_ENRLMT_END_DT_', nslots=16, truncfirst=0)
        s3 = DE.monthly_array_eldts('CHIP_ENRLMT_EFF_DT_', nslots=16, truncfirst=1)
        s4 = DE.monthly_array_eldts('CHIP_ENRLMT_END_DT_', nslots=16, truncfirst=0)

        self.create_temp_table(self, tblname=tblname, subcols=s,
                               subcols2=s2, subcols3=s3, subcols4=s4)
        z = """create or replace temporary view numbers as
                insert into numbers
                    values"""
        for s in range(1, 17):
            for m in range(1, 13):
                mm = str(m)
                if len(mm) == 1:
                    mm.zfill(2)
                z += f"""({s}, '{mm}')"""
                if {s} < 16 or {m} < 12:
                    z += ","

    def eligibility_dates(self, dtype, dval):
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

        self.de.append(type(self).__name__, z)

        # TODO: refactor to pass in tblname to create_temp_table
        z += f"""insert into &DA_SCHEMA..TAF_ANN_DE_{self.tblname}
                select
                    {DE.table_id_cols(self)}
                    ,ENRL_TYPE_FLAG
                    ,ENRLMT_EFCTV_CY_DT
                    ,ENRLMT_END_CY_DT

                from dates_out
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
# <http://creativecommons.org/publicdomain/zero/1.0/>
