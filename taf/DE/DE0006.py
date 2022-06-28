from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner
from taf.TAF_Closure import TAF_Closure


class DE0006(DE):
    tblname: str = "waiver"
    tbl_suffix: str = "wvr"

    def __init__(self, runner: DE_Runner):
        # TODO: Review this
        DE.__init__(self, runner)

    def create(self):
        super().create()
        self.create_temp()
        self.create_wvr_suppl_table()

    def create_temp(self):
        s = f"""{DE.run_waiv_slots(self, 1, 3)}
                ,{TAF_Closure.monthly_array(self, 'WVR_ID', nslots=self.de.NWAIVSLOTS)}
                ,{TAF_Closure.monthly_array(self, 'WVR_TYPE_CD', nslots=self.de.NWAIVSLOTS)}
                ,{TAF_Closure.monthly_array(self, 'SECT_1115A_DEMO_IND')}"""
        s2 = f"""{DE.run_waiv_slots(self, 4, 6)}"""
        s3 = f"""{DE.run_waiv_slots(self, 7, 9)}"""
        s4 = f"""{DE.run_waiv_slots(self, 10, 12)}"""
        s5 = f"""{DE.waiv_nonnull(self, 'WAIVER_SPLMTL')}"""
        os = f"""{DE.sum_months(self, '_1115_PHRMCY_PLUS_WVR')}
                 {DE.sum_months(self, '_1115_DSTR_REL_WVR')}
                 {DE.sum_months(self, '_1115_FP_ONLY_WVR')}
                 {DE.sum_months(self, '_1915C_WVR')}
                 {DE.sum_months(self, '_1915BC_WVR')}
                 {DE.sum_months(self, '_1915B_WVR')}
                 {DE.sum_months(self, '_1115_OTHR_WVR')}
                 {DE.sum_months(self, '_1115_HIFA_WVR')}
                 {DE.sum_months(self, '_OTHR_WVR')}
            """
        DE.create_temp_table(self, tblname=self.tblname, inyear="", subcols=s, subcols2=s2, subcols3=s3,
                             subcols4=s4, subcols5=s5, outercols=os)
        return

    def create_wvr_suppl_table(self):
        z = f"""create or replace temporary view WAIVER_SPLMTL_{self.de.YEAR} as
        select submtg_state_cd
                ,msis_ident_num
                ,WAIVER_SPLMTL

        from waiver_{self.de.YEAR}"""

        self.de.append(type(self).__name__, z)

        z = f"""create table if not exists {self.de.DA_SCHEMA}.numbers
                (slot int, month string)
                using parquet"""
        self.de.append(type(self).__name__, z)

        z = f"""
                insert into {self.de.DA_SCHEMA}.numbers
                values
            """
        for waiv in range(1, self.de.NWAIVSLOTS + 1):
            for m in range(1, 13):
                mm = str(m)
                if len(mm) == 1:
                    mm.zfill(2)

                z += f"""({waiv},'{mm}')"""
                if waiv < self.de.NWAIVSLOTS or m < 12:
                    z += ","

        self.de.append(type(self).__name__, z)

        z = """create or replace temporary view waiver_long as
                select distinct
                        submtg_state_cd
                        ,msis_ident_num
                        ,month
                        ,WVR_TYPE_CD
                        ,WAIVER_CAT
                from (
                    select *
                        ,case"""
        for waiv in range(1, self.de.NWAIVSLOTS + 1):
            for m in range(1, 13):
                m = str(m).zfill(2)

                z += f""" when slot={waiv} and month='{m}' then WVR_TYPE_CD{waiv}_{m}"""
        z += """ end as WVR_TYPE_CD
                ,case
             """

        for waiv in range(1, self.de.NWAIVSLOTS + 1):
            for m in range(1, 13):
                m = str(m).zfill(2)
                z += f""" when slot={waiv} and month='{m}' then WAIVER_CAT{waiv}_{m}"""
        z += """ end as WAIVER_CAT """

        z += """from
                (select a.submtg_state_cd
                    ,a.msis_ident_num
             """

        for waiv in range(1, self.de.NWAIVSLOTS + 1):
            for m in range(1, 13):
                m = str(m).zfill(2)
                z += f""",a.WVR_TYPE_CD{waiv}_{m}"""
                z += f""",case when a.WVR_TYPE_CD{waiv}_{m} is not null
                            and (a.WVR_TYPE_CD{waiv}_{m} >= '06' and a.WVR_TYPE_CD{waiv}_{m} <= '20')
                            or (WVR_TYPE_CD{waiv}_{m}='33')  then 1
                        when a.WVR_TYPE_CD{waiv}_{m} is not null
                            and (a.WVR_TYPE_CD{waiv}_{m} = '01'
                            or (a.WVR_TYPE_CD{waiv}_{m} >= '22' and a.WVR_TYPE_CD{waiv}_{m} <= 30)) then 2
                        else 0
                        end as WAIVER_CAT{waiv}_{m}
                    """
        z += f""",b.slot
                ,b.month

                from waiver_{self.de.YEAR} a
                    join
                    {self.de.DA_SCHEMA}.numbers b
                    on true) sub ) sub2

                where WAIVER_CAT > 0
            """
        self.de.append(type(self).__name__, z)

        z = """create or replace temporary view waiver_counts as
                select submtg_state_cd
                        ,msis_ident_num
                        ,month
                        ,FIRST_WVR_TYPE_CD
                        ,LAST_WVR_TYPE_CD
                        ,case when FIRST_WVR_TYPE_CD = LAST_WVR_TYPE_CD and FIRST_WVR_TYPE_CD is not NULL and
                                WAIVER_CAT=1 then WVR_TYPE_CD

                                when FIRST_WVR_TYPE_CD != LAST_WVR_TYPE_CD and FIRST_WVR_TYPE_CD is not NULL and
                                LAST_WVR_TYPE_CD is not NULL and WAIVER_CAT=1 then '89'
                                else null
                                end as _1915C_WVR_TYPE

                        ,case when FIRST_WVR_TYPE_CD = LAST_WVR_TYPE_CD and FIRST_WVR_TYPE_CD is not NULL and
                                WAIVER_CAT=2 then WVR_TYPE_CD
                                when FIRST_WVR_TYPE_CD != LAST_WVR_TYPE_CD and FIRST_WVR_TYPE_CD is not NULL and
                                LAST_WVR_TYPE_CD is not NULL and WAIVER_CAT=2 then '89'
                                else null
                                end as _1115_WVR_TYPE

                    from

                (select *
                        ,first_value( WVR_TYPE_CD ) over (partition by submtg_state_cd
                                                                        ,msis_ident_num
                                                                        ,month
                                                                        ,WAIVER_CAT) as FIRST_WVR_TYPE_CD

                            ,last_value( WVR_TYPE_CD ) over (partition by submtg_state_cd
                                                                        ,msis_ident_num
                                                                        ,month
                                                                        ,WAIVER_CAT) as LAST_WVR_TYPE_CD
                    from waiver_long
                    where WVR_TYPE_CD is not null
                ) as num """

        self.de.append(type(self).__name__, z)

        z = """create or replace temporary view waiver_latest as

            select enrl.submtg_state_cd
                    ,enrl.msis_ident_num
                    ,coalesce(m12._1915C_WVR_TYPE,m11._1915C_WVR_TYPE,m10._1915C_WVR_TYPE,
                                m09._1915C_WVR_TYPE,m08._1915C_WVR_TYPE,m07._1915C_WVR_TYPE,
                                m06._1915C_WVR_TYPE,m05._1915C_WVR_TYPE,m04._1915C_WVR_TYPE,
                                m03._1915C_WVR_TYPE,m02._1915C_WVR_TYPE,m01._1915C_WVR_TYPE)

                    as _1915C_WVR_TYPE

                    ,coalesce(m12._1115_WVR_TYPE,m11._1115_WVR_TYPE,m10._1115_WVR_TYPE,
                                m09._1115_WVR_TYPE,m08._1115_WVR_TYPE,m07._1115_WVR_TYPE,
                                m06._1115_WVR_TYPE,m05._1115_WVR_TYPE,m04._1115_WVR_TYPE,
                                m03._1115_WVR_TYPE,m02._1115_WVR_TYPE,m01._1115_WVR_TYPE)

                    as _1115_WVR_TYPE

            from

                (select submtg_state_cd,
                        msis_ident_num,
                        count(msis_ident_num) as nmonths

                    from waiver_counts

                        group by submtg_state_cd,
                                msis_ident_num) as enrl
            """

        for m in range(1, 13):
            m = str(m).zfill(2)

            z += f"""left join

            (select submtg_state_cd
                    ,msis_ident_num
                    ,max(_1915C_WVR_TYPE) as _1915C_WVR_TYPE
                    ,max(_1115_WVR_TYPE) as _1115_WVR_TYPE
            from waiver_counts
            where month=('{m}')

            group by submtg_state_cd
                    ,msis_ident_num) as m{m}

            on enrl.submtg_state_cd = m{m}.submtg_state_cd and
                enrl.msis_ident_num = m{m}.msis_ident_num
            """

        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view waiver_pit as
            select a.*,
                b._1915C_WVR_TYPE,
                b._1115_WVR_TYPE

            from waiver_{self.de.YEAR} a
                full join
                waiver_latest b

            on a.submtg_state_cd = b.submtg_state_cd and
            a.msis_ident_num = b.msis_ident_num

            where WAIVER_SPLMTL=1"""

        self.de.append(type(self).__name__, z)

        z = f"""insert into {self.de.DA_SCHEMA}.TAF_ANN_DE_{self.tbl_suffix}
                select

                    {DE.table_id_cols_sfx(self)}
                    ,_1915C_WVR_TYPE
                    ,_1115_WVR_TYPE
                    ,_1115_PHRMCY_PLUS_WVR_MOS
                    ,_1115_DSTR_REL_WVR_MOS
                    ,_1115_FP_ONLY_WVR_MOS
                    ,_1915C_WVR_MOS
                    ,_1915BC_WVR_MOS
                    ,_1915B_WVR_MOS
                    ,_1115_OTHR_WVR_MOS
                    ,_1115_HIFA_WVR_MOS
                    ,_OTHR_WVR_MOS
                    ,SECT_1115A_DEMO_IND_01
                    ,SECT_1115A_DEMO_IND_02
                    ,SECT_1115A_DEMO_IND_03
                    ,SECT_1115A_DEMO_IND_04
                    ,SECT_1115A_DEMO_IND_05
                    ,SECT_1115A_DEMO_IND_06
                    ,SECT_1115A_DEMO_IND_07
                    ,SECT_1115A_DEMO_IND_08
                    ,SECT_1115A_DEMO_IND_09
                    ,SECT_1115A_DEMO_IND_10
                    ,SECT_1115A_DEMO_IND_11
                    ,SECT_1115A_DEMO_IND_12
                    ,WVR_ID1_01
                    ,WVR_ID1_02
                    ,WVR_ID1_03
                    ,WVR_ID1_04
                    ,WVR_ID1_05
                    ,WVR_ID1_06
                    ,WVR_ID1_07
                    ,WVR_ID1_08
                    ,WVR_ID1_09
                    ,WVR_ID1_10
                    ,WVR_ID1_11
                    ,WVR_ID1_12
                    ,WVR_TYPE_CD1_01
                    ,WVR_TYPE_CD1_02
                    ,WVR_TYPE_CD1_03
                    ,WVR_TYPE_CD1_04
                    ,WVR_TYPE_CD1_05
                    ,WVR_TYPE_CD1_06
                    ,WVR_TYPE_CD1_07
                    ,WVR_TYPE_CD1_08
                    ,WVR_TYPE_CD1_09
                    ,WVR_TYPE_CD1_10
                    ,WVR_TYPE_CD1_11
                    ,WVR_TYPE_CD1_12
                    ,WVR_ID2_01
                    ,WVR_ID2_02
                    ,WVR_ID2_03
                    ,WVR_ID2_04
                    ,WVR_ID2_05
                    ,WVR_ID2_06
                    ,WVR_ID2_07
                    ,WVR_ID2_08
                    ,WVR_ID2_09
                    ,WVR_ID2_10
                    ,WVR_ID2_11
                    ,WVR_ID2_12
                    ,WVR_TYPE_CD2_01
                    ,WVR_TYPE_CD2_02
                    ,WVR_TYPE_CD2_03
                    ,WVR_TYPE_CD2_04
                    ,WVR_TYPE_CD2_05
                    ,WVR_TYPE_CD2_06
                    ,WVR_TYPE_CD2_07
                    ,WVR_TYPE_CD2_08
                    ,WVR_TYPE_CD2_09
                    ,WVR_TYPE_CD2_10
                    ,WVR_TYPE_CD2_11
                    ,WVR_TYPE_CD2_12
                    ,WVR_ID3_01
                    ,WVR_ID3_02
                    ,WVR_ID3_03
                    ,WVR_ID3_04
                    ,WVR_ID3_05
                    ,WVR_ID3_06
                    ,WVR_ID3_07
                    ,WVR_ID3_08
                    ,WVR_ID3_09
                    ,WVR_ID3_10
                    ,WVR_ID3_11
                    ,WVR_ID3_12
                    ,WVR_TYPE_CD3_01
                    ,WVR_TYPE_CD3_02
                    ,WVR_TYPE_CD3_03
                    ,WVR_TYPE_CD3_04
                    ,WVR_TYPE_CD3_05
                    ,WVR_TYPE_CD3_06
                    ,WVR_TYPE_CD3_07
                    ,WVR_TYPE_CD3_08
                    ,WVR_TYPE_CD3_09
                    ,WVR_TYPE_CD3_10
                    ,WVR_TYPE_CD3_11
                    ,WVR_TYPE_CD3_12
                    ,WVR_ID4_01
                    ,WVR_ID4_02
                    ,WVR_ID4_03
                    ,WVR_ID4_04
                    ,WVR_ID4_05
                    ,WVR_ID4_06
                    ,WVR_ID4_07
                    ,WVR_ID4_08
                    ,WVR_ID4_09
                    ,WVR_ID4_10
                    ,WVR_ID4_11
                    ,WVR_ID4_12
                    ,WVR_TYPE_CD4_01
                    ,WVR_TYPE_CD4_02
                    ,WVR_TYPE_CD4_03
                    ,WVR_TYPE_CD4_04
                    ,WVR_TYPE_CD4_05
                    ,WVR_TYPE_CD4_06
                    ,WVR_TYPE_CD4_07
                    ,WVR_TYPE_CD4_08
                    ,WVR_TYPE_CD4_09
                    ,WVR_TYPE_CD4_10
                    ,WVR_TYPE_CD4_11
                    ,WVR_TYPE_CD4_12
                    ,WVR_ID5_01
                    ,WVR_ID5_02
                    ,WVR_ID5_03
                    ,WVR_ID5_04
                    ,WVR_ID5_05
                    ,WVR_ID5_06
                    ,WVR_ID5_07
                    ,WVR_ID5_08
                    ,WVR_ID5_09
                    ,WVR_ID5_10
                    ,WVR_ID5_11
                    ,WVR_ID5_12
                    ,WVR_TYPE_CD5_01
                    ,WVR_TYPE_CD5_02
                    ,WVR_TYPE_CD5_03
                    ,WVR_TYPE_CD5_04
                    ,WVR_TYPE_CD5_05
                    ,WVR_TYPE_CD5_06
                    ,WVR_TYPE_CD5_07
                    ,WVR_TYPE_CD5_08
                    ,WVR_TYPE_CD5_09
                    ,WVR_TYPE_CD5_10
                    ,WVR_TYPE_CD5_11
                    ,WVR_TYPE_CD5_12
                    ,WVR_ID6_01
                    ,WVR_ID6_02
                    ,WVR_ID6_03
                    ,WVR_ID6_04
                    ,WVR_ID6_05
                    ,WVR_ID6_06
                    ,WVR_ID6_07
                    ,WVR_ID6_08
                    ,WVR_ID6_09
                    ,WVR_ID6_10
                    ,WVR_ID6_11
                    ,WVR_ID6_12
                    ,WVR_TYPE_CD6_01
                    ,WVR_TYPE_CD6_02
                    ,WVR_TYPE_CD6_03
                    ,WVR_TYPE_CD6_04
                    ,WVR_TYPE_CD6_05
                    ,WVR_TYPE_CD6_06
                    ,WVR_TYPE_CD6_07
                    ,WVR_TYPE_CD6_08
                    ,WVR_TYPE_CD6_09
                    ,WVR_TYPE_CD6_10
                    ,WVR_TYPE_CD6_11
                    ,WVR_TYPE_CD6_12
                    ,WVR_ID7_01
                    ,WVR_ID7_02
                    ,WVR_ID7_03
                    ,WVR_ID7_04
                    ,WVR_ID7_05
                    ,WVR_ID7_06
                    ,WVR_ID7_07
                    ,WVR_ID7_08
                    ,WVR_ID7_09
                    ,WVR_ID7_10
                    ,WVR_ID7_11
                    ,WVR_ID7_12
                    ,WVR_TYPE_CD7_01
                    ,WVR_TYPE_CD7_02
                    ,WVR_TYPE_CD7_03
                    ,WVR_TYPE_CD7_04
                    ,WVR_TYPE_CD7_05
                    ,WVR_TYPE_CD7_06
                    ,WVR_TYPE_CD7_07
                    ,WVR_TYPE_CD7_08
                    ,WVR_TYPE_CD7_09
                    ,WVR_TYPE_CD7_10
                    ,WVR_TYPE_CD7_11
                    ,WVR_TYPE_CD7_12
                    ,WVR_ID8_01
                    ,WVR_ID8_02
                    ,WVR_ID8_03
                    ,WVR_ID8_04
                    ,WVR_ID8_05
                    ,WVR_ID8_06
                    ,WVR_ID8_07
                    ,WVR_ID8_08
                    ,WVR_ID8_09
                    ,WVR_ID8_10
                    ,WVR_ID8_11
                    ,WVR_ID8_12
                    ,WVR_TYPE_CD8_01
                    ,WVR_TYPE_CD8_02
                    ,WVR_TYPE_CD8_03
                    ,WVR_TYPE_CD8_04
                    ,WVR_TYPE_CD8_05
                    ,WVR_TYPE_CD8_06
                    ,WVR_TYPE_CD8_07
                    ,WVR_TYPE_CD8_08
                    ,WVR_TYPE_CD8_09
                    ,WVR_TYPE_CD8_10
                    ,WVR_TYPE_CD8_11
                    ,WVR_TYPE_CD8_12
                    ,WVR_ID9_01
                    ,WVR_ID9_02
                    ,WVR_ID9_03
                    ,WVR_ID9_04
                    ,WVR_ID9_05
                    ,WVR_ID9_06
                    ,WVR_ID9_07
                    ,WVR_ID9_08
                    ,WVR_ID9_09
                    ,WVR_ID9_10
                    ,WVR_ID9_11
                    ,WVR_ID9_12
                    ,WVR_TYPE_CD9_01
                    ,WVR_TYPE_CD9_02
                    ,WVR_TYPE_CD9_03
                    ,WVR_TYPE_CD9_04
                    ,WVR_TYPE_CD9_05
                    ,WVR_TYPE_CD9_06
                    ,WVR_TYPE_CD9_07
                    ,WVR_TYPE_CD9_08
                    ,WVR_TYPE_CD9_09
                    ,WVR_TYPE_CD9_10
                    ,WVR_TYPE_CD9_11
                    ,WVR_TYPE_CD9_12
                    ,WVR_ID10_01
                    ,WVR_ID10_02
                    ,WVR_ID10_03
                    ,WVR_ID10_04
                    ,WVR_ID10_05
                    ,WVR_ID10_06
                    ,WVR_ID10_07
                    ,WVR_ID10_08
                    ,WVR_ID10_09
                    ,WVR_ID10_10
                    ,WVR_ID10_11
                    ,WVR_ID10_12
                    ,WVR_TYPE_CD10_01
                    ,WVR_TYPE_CD10_02
                    ,WVR_TYPE_CD10_03
                    ,WVR_TYPE_CD10_04
                    ,WVR_TYPE_CD10_05
                    ,WVR_TYPE_CD10_06
                    ,WVR_TYPE_CD10_07
                    ,WVR_TYPE_CD10_08
                    ,WVR_TYPE_CD10_09
                    ,WVR_TYPE_CD10_10
                    ,WVR_TYPE_CD10_11
                    ,WVR_TYPE_CD10_12

                from waiver_out"""

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
