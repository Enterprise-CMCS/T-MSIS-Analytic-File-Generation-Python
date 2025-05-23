from pandas import concat
from taf.BSF import BSF_Runner
from taf.BSF.BSF_Metadata import BSF_Metadata
from taf.BSF.ELG import ELG

class ELG00009(ELG):
    """
    Description:  Process TMSIS_LCKIN_INFO and create unique output for BSF.
    """

    def __init__(self, bsf: BSF_Runner):
        ELG.__init__(self, bsf, 'ELG00009', 'TMSIS_LCKIN_INFO', 'LCKIN_EFCTV_DT', 'LCKIN_END_DT')

    def lckin_prvdr(self, max_keep):

        lckin_prvdrs = []
        new_line = '\n\t\t\t'
        for i in list(range(1, 3 + 1)):
            if i <= max_keep:
                lckin_prvdrs.append(f"""
                    , t{i}.LCKIN_PRVDR_NUM as LCKIN_PRVDR_NUM{i}
                    , t{i}.LCKIN_PRVDR_TYPE_CODE as LCKIN_PRVDR_TYPE_CD{i}
                    , t{i}.LCKIN_SRVC as LCKIN_SRVC{i}
                """.format())
            else:
                lckin_prvdrs.append(f"""
                    , cast(null as varchar(30)) as LCKIN_PRVDR_NUM{i}
                    , cast(null as varchar(2)) as LCKIN_PRVDR_TYPE_CD{i}
                    , cast(null as varchar(3)) as LCKIN_SRVC{i}
                """.format())

        return new_line.join(lckin_prvdrs)

    def lckin_flag(self, max_keep):

        lckin_flags = []
        for i in list(range(1, 3 + 1)):
            if i <= max_keep:
                lckin_flags.append(f"t{i}.LCKIN_PRVDR_NUM is not null".format())

        return ' or '.join(lckin_flags)

    def create(self):

        z = f"""
            create or replace temporary view {self.tab_no}_step1 as

            select distinct
                submtg_state_cd,
                msis_ident_num,
                lckin_prvdr_num,
                TMSIS_RPTG_PRD,
                {self.eff_date},
                {self.end_date},
                REC_NUM,
                lpad(lckin_prvdr_type_cd,2,'0') as lckin_prvdr_type_code,
                lpad(lckd_in_srvc,3,'0') as lckin_srvc,

                row_number() over (partition by submtg_state_cd,
                                        msis_ident_num,
                                        lckin_prvdr_num,
                                        lpad(lckin_prvdr_type_cd,2,'0'),
                                        lpad(lckd_in_srvc,3,'0')
                                    order by submtg_state_cd,
                                        msis_ident_num,
                                        TMSIS_RPTG_PRD desc,
                                        {self.eff_date} desc,
                                        {self.end_date} desc,
                                        REC_NUM desc,
                                        lckin_prvdr_num,
                                        lpad(lckin_prvdr_type_cd,2,'0'),
                                        lpad(lckd_in_srvc,3,'0')
                                        ) 
                                as lckin_deduper

                from (select * from {self.tab_no} 
                      where lckin_prvdr_num is not null
                          or lckin_prvdr_type_cd is not null
                      ) t1
                """
        self.bsf.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view {self.tab_no}_step2 as

            select *,

                row_number() over (partition by submtg_state_cd,
                                        msis_ident_num
                                    order by submtg_state_cd,
                                        msis_ident_num,
                                        TMSIS_RPTG_PRD desc,
                                        {self.eff_date} desc,
                                        {self.end_date} desc,
                                        REC_NUM desc,
                                        lckin_prvdr_num,
                                        lckin_prvdr_type_code,
                                        lckin_srvc
                                  ) as keeper

                from {self.tab_no}_step1
                where lckin_deduper=1
                """
        self.bsf.append(type(self).__name__, z)

        #  Set Max number of Keeper Records
        MAX_KEEP = 3

        z = f"""
            create or replace temporary view {self.tab_no}_{self.bsf.BSF_FILE_DATE}_uniq as

            select
                t1.submtg_state_cd
                ,t1.msis_ident_num

                { self.lckin_prvdr(MAX_KEEP) }

                , case when ({ self.lckin_flag(MAX_KEEP) })
                  then 1 else 2 end as LOCK_IN_FLG

                from (select * from {self.tab_no}_step2 where keeper=1) t1

                { BSF_Metadata.dedup_tbl_joiner('ELG00009', range(2, 3 + 1), MAX_KEEP) }

                """
        self.bsf.append(type(self).__name__, z)


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
