from taf.PRV.PRV_Runner import PRV_Runner
from taf.TAF import TAF


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class PRV(TAF):

    srtlist = ['tms_run_id', 'submitting_state', 'submitting_state_prov_id']
    srtlistl = ['tms_run_id',
                'submitting_state',
                'submitting_state_prov_id',
                'prov_location_id']

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, prv: PRV_Runner):

        self.prv = prv
        self.st_fil_type = 'PRV'

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def set_end_dt(self, enddt):
        z = f"""
            case
                when to_date('{enddt}') is null then to_date('9999-12-31')
                when to_date('{enddt}') < to_date('1600-01-01') then to_date('1599-12-31')
                else to_date('{enddt}')
            end
            """
        return z.format(enddt)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def zero_pad(self, var_cd, var_len):
        z = f"""
            case
                when length(trim({var_cd})) < {var_len} and length(trim({var_cd})) > 0 and {var_cd} is not null
                then lpad(trim(upper({var_cd})),{var_len},'0')
                else nullif(trim(upper({var_cd})),'')
            end as {var_cd}
            """
        return z.format(var_cd, var_len)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def screen_runid(self, intbl, runtbl, runvars, outtbl, runtyp='C'):

        if runtyp == 'M':
            on = f"on { self.write_equalkeys(runvars, 'T', 'R') }"
        elif runtyp == 'L':
            on = """on T.tms_run_id = R.tms_run_id and
                        T.submitting_state = R.submitting_state and
                        upper(T.submitting_state_prov_id) = R.submitting_state_prov_id and
                        upper(T.prov_location_id) = R.prov_location_id"""
        else:
            on = """on T.tms_run_id = R.tms_run_id and
                        T.submitting_state = R.submitting_state and
                        upper(T.submitting_state_prov_id) = R.submitting_state_prov_id"""

        # diststyle key distkey(submitting_state_prov_id)
        # compound sortkey (&&&runvars) as
        z = f"""
            create or replace temporary view {outtbl} as
            select
                T.*
            from
                {intbl} T
            inner join {runtbl} R
                { on.format(runvars) }
            order by
                { self.write_keyprefix(runvars, 'T') }
            """

        self.prv.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def copy_activerows(self, intbl, collist, whr, outtbl):
        from taf.TAF_Closure import TAF_Closure

        if whr != '':
            whr = ' and (' + whr + ')'

        collist2 = map(TAF_Closure.parse, collist)
        select = ', '.join(collist2)

        # diststyle key distkey(submitting_state_prov_id)
        # compound sortkey(tms_run_id, submitting_state, submitting_state_prov_id) as
        z = f"""
            create or replace temporary view {outtbl} as
            select
                { select }
            from
                { intbl }
            where
                tms_is_active=1
                { whr }
            order by
                tms_run_id,
                submitting_state,
                submitting_state_prov_id
            """
        self.prv.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    # def copy_activerows_nts(self, intbl, collist, whr, outtbl):
    def copy_activerows_nts(self, intbl, collist, outtbl):

        # diststyle even compound sortkey(tms_run_id, submitting_state)
        z = f"""
                create or replace temporary view {outtbl} as
                select
                    { ', '.join(collist) }
                from (
                    select
                        *,
                        submitting_state as submtg_state_cd
                    from
                        {intbl}
                    where
                        tms_is_active = 1 and
                        tms_reporting_period is not null and
                        tot_rec_cnt > 0 and
                        trim(TRAILING FROM submitting_state) not in ('94','96')
                    )
                where
                    1=1 { self.prv.ST_FILTER() }
                order by
                    tms_run_id,
                    submitting_state
            """
        self.prv.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def screen_dates(self, intbl, keyvars, dtvar_beg, dtvar_end, outtbl):

        # diststyle key
        # distkey(submitting_state_prov_id)
        # compound sortkey (&&&keyvars) as
        z = f"""
            create or replace temporary view {outtbl} as
            select
                T.*
            from
                {intbl} T

            left join
                state_submsn_type s
            on
                T.submitting_state = s.submtg_state_cd
                and upper(s.fil_type) = 'PRV'

            where
                (
                    T.{dtvar_beg} <= to_date('{self.prv.RPT_PRD}')
                    and (
                        T.{dtvar_end} >= to_date('{self.prv.st_dt}')
                        or T.{dtvar_end} is NULL
                        )

                 and (
                    (
                         upper(coalesce(s.submsn_type, 'X')) <> 'CSO'
                         and T.tms_reporting_period >= to_date('{self.prv.st_dt}')
                     )
                     or (upper(coalesce(s.submsn_type, 'X')) = 'CSO')
                 ))

            order by { self.write_keyprefix(keyvars, 'T') }
            """
        self.prv.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def remove_duprecs(self, intbl, grpvars, dtvar_beg, dtvar_end, ordvar, outtbl):

        # limit data to the latest available reporting periods
        # distkey(submitting_state_prov_id)
        z = f"""
            create or replace temporary view TblCopyGrouped_{intbl} as
            select *,
                row_number() over (
                    partition by { ','.join(grpvars) }
                    order by tms_reporting_period desc,
                        {dtvar_beg} desc,
                        {dtvar_end} desc,
                        record_number desc,
                        {ordvar} asc
                ) as _wanted_{intbl}
            from
                {intbl}
            order by
                { ','.join(grpvars) }
            """
        self.prv.append(type(self).__name__, z)

        # distkey(submitting_state_prov_id)
        # compound sortkey (&&&grpvars)
        # final upduplication step
        z = f"""
            create or replace temporary view {outtbl} as
            select
                *
            from
                TblCopyGrouped_{intbl}
            where
                _wanted_{intbl} = 1
            order by
                { ','.join(grpvars) }
            """
        self.prv.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def count_rows(self, intbl, cntvar, outds):

        z = f"""
            create or replace temporary view {outds} as
            select
                submitting_state, count(*) as {cntvar}
            from
                {intbl}
            group by submitting_state
            order by submitting_state
            """
        self.prv.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def recode_lookup(self, intbl, srtvars, fmttbl, fmtnm, srcvar, newvar, outtbl, fldtyp, fldlen=None):

        if fldtyp == 'C':
            select = f"T.*, cast(F.label as varchar({fldlen})) as {newvar}"
        else:
            select = f"T.*, cast(F.label as smallint) as {newvar}"

        # diststyle key distkey(submitting_state_prov_id)
        # compound sortkey (&&&srtvars) as
        z = f"""
            create or replace temporary view {outtbl} as
            select
                { select }
            from
                {intbl} T
            left join
                {fmttbl} F
                on
                    F.fmtname="{fmtnm}" and (Trim(T.{srcvar})>=F.start and Trim(T.{srcvar})<=F.end)
            order by
                { self.write_keyprefix(srtvars, 'T') }
            """
        self.prv.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def recode_notnull(self, intbl, srtvars, fmttbl, fmtnm, srcvar, newvar, outtbl, fldtyp, fldlen):

        if fldtyp == 'C':
            # :: varchar({fldlen}) as {newvar}"
            case = f"case when F.label is null then T.{srcvar} else F.label end as {newvar}"
        else:
            case = f"case when F.label is null then T.{srcvar} else F.label end as {newvar}"

        # diststyle key distkey(submitting_state_prov_id)
        # compound sortkey (&&&srtvars) as
        z = f"""
            create or replace temporary view {outtbl} as
            select
                T.*,
                { case }
            from
                {intbl} T
            left join
                {fmttbl} F
                on
                    F.fmtname="{fmtnm}" and (Trim(T.{srcvar})>=F.start and Trim(T.{srcvar})<=F.end)
            order by
                { self.write_keyprefix(srtvars, 'T') }
            """
        self.prv.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def write_equalkeys(self, keyvars, t1, t2):

        klist = map(lambda x: f"{t1}.{x} = {t2}.{x}", keyvars)
        keylist = list(klist)
        return ' and '.join(str(k) for k in keylist)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def write_keyprefix(self, keyvars, prefix):

        klist = map(lambda x: f"{prefix}.{x}", keyvars)
        keylist = list(klist)
        # return ', '.join(str(k) for k in keylist)
        return ', '.join(keylist)


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
