# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
from taf.UP.UP import UP
from taf.UP.UP_Runner import UP_Runner
from calendar import monthrange, isleap


class BASE_LT(UP):
    """
    Description:  Read in the LT line-level file, aggregate to header, and count unique LT days
    """
     
    def __init__(self, up: UP_Runner):
        UP.__init__(self, up)
        self.up = up

    #def __init__(self, up: UP_Runner):
        #super().__init__(up)

    def create(self):
        """
        Take the header-level file with needed cols, and join to a rolled-up line level file to
        get min srvc_bgnng_dt and TOS_CD from the line. Only keep FFS/MC claims, and only keep
        claims with at least one line with specific TOS_CD values (009, 043-050, 059), OR with 
        all TOS_CD values = null.
        Then need to get daily indicators for each stay, and then get the MAX across all days for each bene
        to create a count of unique LT days ;
        """
         
        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW lt_hdr_days_{self.year} AS
            SELECT submtg_state_cd
                ,msis_ident_num
                ,lt_link_key
                ,clm_type_cd
                ,MDCD
                ,SCHIP
                ,NON_XOVR
                ,XOVR
                ,srvc_endg_dt
                ,srvc_bgnng_dt
                ,admsn_dt
                ,bgnng_dt
                ,case when ANY_TOS_KEEP=1 or TOS_CD_NULL=1 then 1
                     else 0
                 end as CLAIM_TOS_KEEP
        """

        # Loop over all days of the year, and based on service beginning and ending date (when both are
        # not null), create a daily indicator. Name in sequential order from 1-365/366 (to more easily loop over).
        num = 0

        for m in range(1, 13):
            mm = "{:02d}".format(m)
            lday = monthrange(self.year, m)[1]

            for d in range(1, lday + 1):
                dd = "{:02d}".format(d)

                num += 1

                z += f"""
                     ,case when bgnng_dt is not null and
                                srvc_endg_dt is not null and
                                bgnng_dt <= to_date('{self.year}-{mm}-{dd}') and
                                srvc_endg_dt >= to_date('{self.year}-{mm}-{dd}')
                           then 1 else 0
                           end as day{num}
                """

        z += f"""
             FROM (
                 SELECT a.*
                 ,b.srvc_bgnng_dt_ln_min
                 ,b.ANY_TOS_KEEP
                 ,b.TOS_CD_NULL
                 ,case
                      when srvc_bgnng_dt is not null then srvc_bgnng_dt
                      when srvc_bgnng_dt_ln_min is not null then srvc_bgnng_dt_ln_min
                      when admsn_dt is not null then admsn_dt
                  else null
                 end as bgnng_dt
                 FROM lth_2021 a
                 LEFT JOIN (
                     SELECT lt_link_key
                         ,min(srvc_bgnng_dt_ln) AS srvc_bgnng_dt_ln_min
                         ,max(CASE
                                 WHEN TOS_CD IN (
                                         '009'
                                         ,'043'
                                         ,'044'
                                         ,'045'
                                         ,'046'
                                         ,'047'
                                         ,'048'
                                         ,'049'
                                         ,'050'
                                         ,'059'
                                         )
                                     THEN 1
                                 ELSE 0
                                 END) AS ANY_TOS_KEEP
                         ,CASE
                             WHEN max(TOS_CD) IS NULL
                                 THEN 1
                             ELSE 0
                             END AS TOS_CD_NULL
                     FROM ltl_2021
                     GROUP BY lt_link_key
                     ) b
                on a.lt_link_key = b.lt_link_key 
                where a.clm_type_cd in ('1','A','3','C')
                 ) c
        """
        self.up.append(type(self).__name__, z)

        # Now by clm_type_cd, MDCD/SCHIP, and NON_XOVR/XOVR, get a count of unique days.
        # Subset to claims based on TOS_CD values.
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW lt_hdr_days2_{self.year} AS
            SELECT submtg_state_cd
               ,msis_ident_num
        """

        for ind1 in self.inds1:
            for ind2 in self.inds2:
                # create columns to assign claim types for MDCD or SCHIP
                if ind1.casefold() == "mdcd":
                    ffsval = "1"
                    mcval = "3"
                elif ind1.casefold() == "schip":
                    ffsval = "A"
                    mcval = "C"
                else:
                    ffsval = ""
                    mcval = ""

                # Create macro vars to assign claim types for MDCD or SCHIP
                # assign_toc
                for num in range(1, 366 + (isleap(self.year))):
                    if num == 1:
                        z += ","

                    if num > 1:
                        z += " + "
                    z += f"""max(case when {ind1}=1 and {ind2}=1 and clm_type_cd='{ffsval}' then day{num} else 0 end)"""
                z += f""" as {ind1}_{ind2}_FFS_LT_DAYS"""

                for num in range(1, 366 + (isleap(self.year))):
                    if num == 1:
                        z += ","

                    if num > 1:
                        z += " + "
                    z += f"""max(case when {ind1}=1 and {ind2}=1 and clm_type_cd='{mcval}' then day{num} else 0 end)"""
                z += f""" as {ind1}_{ind2}_MC_LT_DAYS"""

        z += " "

        z += f"""from lt_hdr_days_{self.year}
		         where CLAIM_TOS_KEEP=1
		         group by submtg_state_cd
		             ,msis_ident_num
        """

        self.up.append(type(self).__name__, z)





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
