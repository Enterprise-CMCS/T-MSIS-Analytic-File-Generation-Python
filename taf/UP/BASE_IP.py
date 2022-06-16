# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
from taf.UP.UP import UP
from taf.UP.UP_Runner import UP_Runner
from calendar import monthrange

# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class BASE_IP(UP):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, up: UP_Runner):
        super().__init__(up)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        # Stack all three years (prior year with three months, current year, and following year with
        # three months). Create claim_provider to be able to roll up to stay-level.
        # Keep FFS and encounters only

        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)
        z = f"""
            CREATE
                OR REPLACE TEMPORARY VIEW ipl_3yr AS

            SELECT *
                ,coalesce(blg_prvdr_num, blg_prvdr_npi_num) AS claim_provider
            FROM (
                SELECT *
                FROM ipl_{self.pyear}

                UNION

                SELECT *
                FROM ipl_{self.year}

                UNION

                SELECT *
                FROM ipl_{self.fyear}
                )
            WHERE clm_type_cd IN (
                    '1'
                    ,'A'
                    ,'3'
                    ,'C'
                    )
        """
        self.up.append(type(self).__name__, z)

        # Roll up to the header-level, creating claim admission and discharge date (using
        # line-level values if header is missing), and identifying claims to keep based on TOS_CD -
        # Identify claims where there is at least one claim with specific TOS_CD values we want included in the stay counts,
        # OR all TOS_CD values are null - we will keep all claims with either of these scenarios.
        # Drop claims with null claim provider

        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW ipl_3yr_hdr AS

            SELECT *
            FROM (
                SELECT *
                    ,CASE
                        WHEN admsn_dt IS NOT NULL
                            THEN admsn_dt
                        WHEN srvc_bgnng_dt_ln_min IS NOT NULL
                            THEN srvc_bgnng_dt_ln_min
                        ELSE NULL
                        END AS claim_admsn_dt
                    ,CASE
                        WHEN dschrg_dt IS NOT NULL
                            THEN dschrg_dt
                        WHEN srvc_endg_dt_ln_max IS NOT NULL
                            THEN srvc_endg_dt_ln_max
                        ELSE NULL
                        END AS claim_dschrg_dt

                    -- Create claim_tos_keep (keep claim based on TOS_CD values) and invalid_dates (will
                    --        drop based on dates if any of the following:
                    --            - admission OR discharge null
                    --            - discharge before admission
                    --            - > 1.5 years between admission and discharge )

                    ,CASE
                        WHEN any_tos_keep = 1
                            OR tos_cd_null = 1
                            THEN 1
                        ELSE 0
                        END AS claim_tos_keep
                    ,CASE
                        WHEN claim_admsn_dt IS NULL
                            OR claim_dschrg_dt IS NULL
                            OR claim_dschrg_dt < claim_admsn_dt
                            OR (claim_dschrg_dt - claim_admsn_dt + 1) > (365.25 * 1.5)
                            THEN 1
                        ELSE 0
                        END AS invalid_dates
                FROM (
                    SELECT submtg_state_cd
                        ,msis_ident_num
                        ,ip_link_key
                        ,claim_provider
                        ,ptnt_stus_cd
                        ,FFS
                        ,MDCD
                        ,XOVR
                        ,admsn_dt
                        ,dschrg_dt
                        ,min(srvc_bgnng_dt_ln) AS srvc_bgnng_dt_ln_min
                        ,max(srvc_endg_dt_ln) AS srvc_endg_dt_ln_max
                        ,max(CASE
                                WHEN TOS_CD IN (
                                        '001'
                                        ,'060'
                                        ,'084'
                                        ,'086'
                                        ,'090'
                                        ,'091'
                                        ,'092'
                                        ,'093'
                                        )
                                    THEN 1
                                ELSE 0
                                END) AS any_tos_keep
                        ,CASE
                            WHEN max(TOS_CD) IS NULL
                                THEN 1
                            ELSE 0
                            END AS tos_cd_null
                    FROM ipl_3yr
                    WHERE claim_provider IS NOT NULL
                    GROUP BY submtg_state_cd
                        ,msis_ident_num
                        ,ip_link_key
                        ,claim_provider
                        ,ptnt_stus_cd
                        ,FFS
                        ,MDCD
                        ,XOVR
                        ,admsn_dt
                        ,dschrg_dt
                    )
                )
            WHERE claim_tos_keep = 1
                AND invalid_dates = 0
        """
        self.up.append(type(self).__name__, z)

        # Now need to identify, for a given set of claims by discharge date only, whether there is ANY claim with that discharge date
        # that has an associated patient status code != 30 and != null. This is because of the rules for rolling up based on patient status code:
        # If there are two claims that fall into the category where we have a claim with a discharge date equal to or one day before the admission
        # date of the next stay, but one has a patient status code = 30 or null and one has a code indicating discharge,
        # the code indicating discharge  will take precedence

        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)
        z = f"""
            CREATE
                OR replace TEMPORARY VIEW ip_for_stays_unq_dschrg AS

            SELECT submtg_state_cd
                ,msis_ident_num
                ,claim_provider
                ,FFS
                ,MDCD
                ,XOVR
                ,claim_dschrg_dt
                ,max(CASE
                        WHEN ptnt_stus_cd != 30
                            AND ptnt_stus_cd IS NOT NULL
                            THEN 1
                        ELSE 0
                        END) AS stus_cd_dschrg
            FROM ipl_3yr_hdr
            GROUP BY submtg_state_cd
                ,msis_ident_num
                ,claim_provider
                ,FFS
                ,MDCD
                ,XOVR
                ,claim_dschrg_dt
        """
        self.up.append(type(self).__name__, z)

        # Create a table of distinct claim admission/discharge dates for all records,
        # joining back to the above table to add stus_cd_dschrg

        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)
        z = f"""
            CREATE
                OR replace TEMPORARY VIEW ip_for_stays_unq AS

            SELECT DISTINCT a.submtg_state_cd
                ,a.msis_ident_num
                ,a.claim_provider
                ,a.FFS
                ,a.MDCD
                ,a.XOVR
                ,a.claim_admsn_dt
                ,a.claim_dschrg_dt
                ,b.stus_cd_dschrg
            FROM ipl_3yr_hdr a
            LEFT JOIN ip_for_stays_unq_dschrg b ON a.submtg_state_cd = b.submtg_state_cd
                AND a.msis_ident_num = b.msis_ident_num
                AND a.claim_provider = b.claim_provider
                AND a.FFS = b.FFS
                AND a.MDCD = b.MDCD
                AND a.XOVR = b.XOVR
                AND a.claim_dschrg_dt = b.claim_dschrg_dt
        """
        self.up.append(type(self).__name__, z)

        # Create a unique date ID to filter on later when identifying stays completely contained within another

        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)
        z = f"""
            CREATE
                OR replace TEMPORARY VIEW ip_for_stays_unq_ids AS

            SELECT *
                ,trim(submtg_state_cd || '-' || msis_ident_num || '-' || claim_provider || '-' || FFS || '-' || MDCD || '-' || XOVR || '-' || cast(row_number() OVER (
                            PARTITION BY submtg_state_cd
                            ,msis_ident_num
                            ,claim_provider
                            ,FFS
                            ,MDCD
                            ,XOVR ORDER BY submtg_state_cd
                                ,msis_ident_num
                                ,claim_provider
                                ,FFS
                                ,MDCD
                                ,XOVR
                                ,claim_admsn_dt
                                ,claim_dschrg_dt
                            ) AS CHAR(3))) AS dateId
            FROM ip_for_stays_unq
        """
        self.up.append(type(self).__name__, z)

        # Create a table of claims by joining every stay to every other stay within the same group, and
        # identifying overlaps where one stay is completely contained within another

        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)

        z = f"""
            CREATE
                OR replace TEMPORARY VIEW ip_for_stays_overlaps AS

            SELECT a.*
            FROM ip_for_stays_unq_ids a
            INNER JOIN ip_for_stays_unq_ids b
                -- Join records to each other, but omit matches where same record
                ON a.submtg_state_cd = b.submtg_state_cd
                AND a.msis_ident_num = b.msis_ident_num
                AND a.claim_provider = b.claim_provider
                AND a.FFS = b.FFS
                AND a.MDCD = b.MDCD
                AND a.XOVR = b.XOVR
                AND a.dateId <> b.dateId

            -- Get every dateID where admission date is greater than or equal to another record's admission date
            --          AND discharge date is less than or equal to that other record's discharge date. These are records contained
            --          within another stay, which will be excluded from rollup

            WHERE a.claim_admsn_dt >= b.claim_admsn_dt
                AND a.claim_dschrg_dt <= b.claim_dschrg_dt
        """
        self.up.append(type(self).__name__, z)

        # Now create a table of non-overlap records which will be used for rollup

        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)
        z = f"""
            CREATE
                OR replace TEMPORARY VIEW ip_for_stays_nonoverlap AS

            SELECT a.*
            FROM ip_for_stays_unq_ids a
            LEFT JOIN ip_for_stays_overlaps b ON a.dateid = b.dateid
            WHERE b.dateid IS NULL
        """
        self.up.append(type(self).__name__, z)

        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)
        z = f"""
            CREATE
                OR replace TEMPORARY VIEW ip_stays_out AS

            SELECT submtg_state_cd
                ,msis_ident_num
                ,claim_provider
                ,FFS
                ,MDCD
                ,XOVR
                ,g
                ,min(claim_admsn_dt) AS stay_admsn_dt
                ,max(claim_dschrg_dt) AS stay_dschrg_dt
            FROM (
                SELECT submtg_state_cd
                    ,msis_ident_num
                    ,claim_provider
                    ,FFS
                    ,MDCD
                    ,XOVR
                    ,claim_admsn_dt
                    ,claim_dschrg_dt
                    ,sum(C) OVER (
                        PARTITION BY submtg_state_cd
                        ,msis_ident_num
                        ,claim_provider
                        ,FFS
                        ,MDCD
                        ,XOVR ORDER BY claim_admsn_dt
                            ,claim_dschrg_dt rows UNBOUNDED PRECEDING
                        ) AS G
                FROM (
                    SELECT submtg_state_cd
                        ,msis_ident_num
                        ,claim_provider
                        ,FFS
                        ,MDCD
                        ,XOVR
                        ,claim_admsn_dt
                        ,claim_dschrg_dt
                        ,claim_admsn_dt_lag
                        ,claim_dschrg_dt_lag

                        -- Determine when to create a second stay - if the difference between current admission date and prior
                        --          discharge date is more than 1 day, OR the difference is 0 or 1 day (either same or contiguous days)
                        --          AND the prior patient status code indicates discharge, then create a second stay

                        ,CASE
                            WHEN claim_admsn_dt - coalesce(claim_dschrg_dt_lag, claim_admsn_dt) > 1
                                OR (
                                    claim_admsn_dt - coalesce(claim_dschrg_dt_lag, claim_admsn_dt) IN (
                                        0
                                        ,1
                                        )
                                    AND (stus_cd_dschrg_lag = 1)
                                    )
                                THEN 1
                            ELSE 0
                            END AS C
                    FROM (
                        SELECT submtg_state_cd
                            ,msis_ident_num
                            ,claim_provider
                            ,FFS
                            ,MDCD
                            ,XOVR
                            ,claim_admsn_dt
                            ,claim_dschrg_dt
                            ,lag(claim_admsn_dt) OVER (
                                PARTITION BY submtg_state_cd
                                ,msis_ident_num
                                ,claim_provider
                                ,FFS
                                ,MDCD
                                ,XOVR ORDER BY claim_admsn_dt
                                    ,claim_dschrg_dt
                                ) AS claim_admsn_dt_lag
                            ,lag(claim_dschrg_dt) OVER (
                                PARTITION BY submtg_state_cd
                                ,msis_ident_num
                                ,claim_provider
                                ,FFS
                                ,MDCD
                                ,XOVR ORDER BY claim_admsn_dt
                                    ,claim_dschrg_dt
                                ) AS claim_dschrg_dt_lag
                            ,lag(stus_cd_dschrg) OVER (
                                PARTITION BY submtg_state_cd
                                ,msis_ident_num
                                ,claim_provider
                                ,FFS
                                ,MDCD
                                ,XOVR ORDER BY claim_admsn_dt
                                    ,claim_dschrg_dt
                                ) AS stus_cd_dschrg_lag
                        FROM ip_for_stays_nonoverlap
                        ORDER BY claim_admsn_dt
                            ,claim_dschrg_dt
                        ) s1
                    ) s2
                ) s3
            GROUP BY submtg_state_cd
                ,msis_ident_num
                ,claim_provider
                ,FFS
                ,MDCD
                ,XOVR
                ,g
        """
        self.up.append(type(self).__name__, z)

        # Now take the above stays and by MDCD/SCHIP, XOVR/NON_XOVR, and FFS/MC, subset to stay records to keep for the
        # current year:
        #   - stays with a discharge date in the current year
        #   - stays with no more than 1.5 years difference between stay admission and stay discharge date
        #
        # Create day-level indicators for every day from 2 years prior to 01/01 of the calendar year to 12/31 of the calendar year
        # (because the earliest stay-level date allowable would be 1.5 years prior to 01/01, but  for simplification,
        #  just count beginning with 6/1 of two years prior)
        #
        # Also recreate the SCHIP and NON_XOVR indicators (for simplicity had only rolled up by MDCD/XOVR)
        #
        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)
        z = f"""
            CREATE
                OR replace TEMPORARY VIEW ip_stays_days AS

            SELECT *
                ,CASE
                    WHEN MDCD = 0
                        THEN 1
                    ELSE 0
                    END AS SCHIP
                ,CASE
                    WHEN XOVR = 0
                        THEN 1
                    ELSE 0
                    END AS NON_XOVR
        """

        # Loop over all three years, and then all days of the year, and based on service beginning and ending date (when both are
        # not null), create a daily indicator.
        # Call macro leapyear for each year to determine whether year is a leap year so know number of days for February.
        # Create macro var alldays to be the total number of days (max of NUM macro var) to be able to count total days on roll-up
        num = 0

        for ipyear in range(self.pyear2, self.year + 1):
            if ipyear == self.pyear2:
                smonth = 6
            else:
                smonth = 1

            for m in range(1, 13):
                mm = "{:02d}".format(m)
                lday = monthrange(self.year, m)[1]

                for d in range(1, lday + 1):
                    dd = "{:02d}".format(d)

                    num += 1

                    z += f"""
                         ,case when stay_admsn_dt <= to_date({ipyear} + "-" + {mm} + "-" + {dd})) and
                                    stay_dschrg_dt >= to_date({ipyear}+ "-" + {mm} + "-" + {dd})
                             then 1 else 0
                             end as day{num}
                    """

                    alldays = num

        z += f"""
             FROM ip_stays_out
             WHERE (stay_dschrg_dt - stay_admsn_dt + 1) <= (365.25 * 1.5) and
                 date_part('year',stay_dschrg_dt)={self.year}
        """
        self.up.append(type(self).__name__, z)

        # Now by clm_type_cd, MDCD/SCHIP, and NON_XOVR/XOVR, get a count of unique days and stays.
        # Must create two separate tables (one for MDCD and one for SHCIP) because of column limits

        # distkey(msis_ident_num)
        # sortkey(submtg_state_cd,msis_ident_num)
        for ind1 in self.inds1:
            z = f"""
                CREATE
                    OR replace TEMPORARY VIEW ip_stays_days_{ind1} AS

                SELECT submtg_state_cd
                    ,msis_ident_num
            """

            for ind2 in self.inds2:
                # For FFS and MC, sum daily indicators to create day counts, and then just count once for
                # each matching record to get stay counts
                for num in range(1, alldays + 1):
                    if num > 1:
                        z += " " + "+" + " "

                    z += f"""
                         max(case when {ind1}=1 and {ind2}=1 and FFS=1 then day{num} else 0 end)
                    """

                z += f"""
                     AS {ind1}_{ind2}_FFS_IP_DAYS
                """
                z += f"""
                     ,sum(CASE
                             WHEN {ind1} = 1
                                 AND {ind2} = 1
                                 AND FFS = 1
                                 THEN 1
                             ELSE 0
                             END) AS {ind1}_{ind2}_FFS_IP_STAYS
                """

                for num in range(1, alldays + 1):
                    if num > 1:
                        z += " " + "+" + " "

                    z += f"""
                         max(case when {ind1}=1 and {ind2}=1 and FFS=0 then day{num} else 0 end)
                    """

                z += f"""
                     AS {ind1}_{ind2}_MC_IP_DAYS
                """
                z += f"""
                     ,sum(CASE
                             WHEN {ind1} = 1
                                 AND {ind2} = 1
                                 AND FFS = 0
                                 THEN 1
                             ELSE 0
                             END) AS {ind1}_{ind2}_MC_IP_STAYS
                """

            z += f"""
                 FROM ip_stays_days
                 WHERE {ind1} = 1
                 GROUP BY submtg_state_cd
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
