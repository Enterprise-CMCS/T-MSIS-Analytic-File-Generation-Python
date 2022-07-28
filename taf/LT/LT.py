from taf.LT.LT_Metadata import LT_Metadata
from taf.LT.LT_Runner import LT_Runner
from taf.TAF import TAF
from taf.TAF_Closure import TAF_Closure


class LT(TAF):

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def __init__(self, runner: LT_Runner):
        super().__init__(runner)
        self.st_fil_type = "LT"

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def AWS_Extract_Line(self, TMSIS_SCHEMA, fl2, fl, tab_no, _2x_segment):

        # Subset line file and attach row numbers to all records belonging to an ICN set.  Fix PA & IA
        # FIXME: change base table or view when TMSIS changes are made
        z = f"""
            create or replace temporary view {fl2}_LINE_IN as
            select
                SUBMTG_STATE_CD,
                { LT_Metadata.selectDataElements(tab_no, 'a') }

            from
                TAF_PYTHON.{_2x_segment}_TEMP_TAF A

            where
                a.TMSIS_ACTV_IND = 1
                and concat(a.submtg_state_cd,a.tmsis_run_id) in ({self.runner.get_combined_list()})
        """
        self.runner.append(self.st_fil_type, z)

        z = f"""
            create or replace temporary view {fl2}_LINE_PRE_NPPES as
            select
                a.*,
                row_number() over (
                    partition by
                        a.SUBMTG_STATE_CD,
                        a.ORGNL_CLM_NUM_LINE,
                        a.ADJSTMT_CLM_NUM_LINE,
                        a.ADJDCTN_DT_LINE,
                        a.LINE_ADJSTMT_IND
                    order by
                        a.SUBMTG_STATE_CD,
                        a.ORGNL_CLM_NUM_LINE,
                        a.ADJSTMT_CLM_NUM_LINE,
                        a.ADJDCTN_DT_LINE,
                        a.LINE_ADJSTMT_IND,
                        a.TMSIS_FIL_NAME,
                        a.REC_NUM
                ) as RN,
                , a.SUBMTG_STATE_CD AS NEW_SUBMTG_STATE_CD_LINE

            from
                {fl2}_LINE_IN as A

            inner join FA_HDR_{fl} H

            on
                H.TMSIS_RUN_ID = a.TMSIS_RUN_ID_LINE and
                H.SUBMTG_STATE_CD = a.SUBMTG_STATE_CD and
                H.ORGNL_CLM_NUM = a.ORGNL_CLM_NUM_LINE and
                H.ADJSTMT_CLM_NUM = a.ADJSTMT_CLM_NUM_LINE and
                H.ADJDCTN_DT = a.ADJDCTN_DT_LINE and
                H.ADJSTMT_IND = a.LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)

        # join line file with NPPES to pick up servicing provider taxonomy code
        z = f"""
            create or replace temporary view {fl2}_LINE as
            select
                a.*
               ,{TAF_Closure.var_set_taxo("SELECTED_TXNMY_CD",cond1="8888888888", cond2="9999999999", cond3="000000000X", cond4="999999999X",
									      cond5="NONE", cond6="XXXXXXXXXX", cond7="NO TAXONOMY", NEW="SRVCNG_PRVDR_NPPES_TXNMY_CD")}
            from {fl2}_LINE_PRE_NPPES as a
            left join NPPES_NPI n
                on n.prvdr_npi = a.SRVCNG_PRVDR_NPI_NUM

        """
        self.runner.append(self.st_fil_type, z)

        # Pull out maximum row_number for each partition and compute calculated variables here
        # Revisit coding for accommodation_paid, ancillary_paid, cvr_mh_days_over_65, cvr_mh_days_under_21 here
        z = f"""
            create or replace temporary view constructed_{fl2} as

            select
                  NEW_SUBMTG_STATE_CD_LINE
                , ORGNL_CLM_NUM_LINE
                , ADJSTMT_CLM_NUM_LINE
                , ADJDCTN_DT_LINE
                , LINE_ADJSTMT_IND
                , max(RN) as NUM_CLL
                , sum (case when substring(lpad(REV_CD,4,'0'),1,2)='01' or substring(lpad(REV_CD,4,'0'),1,3) in ('020', '021')
                        then MDCD_PD_AMT
                        when rev_cd is not null and mdcd_pd_amt is not null then 0
                        end
                    ) as ACCOMMODATION_PAID
                , sum (case when substring(lpad(REV_CD,4,'0'),1,2) <> '01' and
                            substring(lpad(REV_CD,4,'0'),1,3) not in ('020', '021')
                        then MDCD_PD_AMT
                        when REV_CD is not null and MDCD_PD_AMT is not null then 0
                        end
                    ) as ANCILLARY_PAID
                , max (case when lpad(TOS_CD,3,'0')in ('044','045') then 1
                    when TOS_CD is null then null
                    else 0
                    end
                    ) as MH_DAYS_OVER_65
                , max(case when lpad(TOS_CD,3,'0') = '048' then 1
                        when TOS_CD is null then null
                    else 0
                    end
                    ) as MH_DAYS_UNDER_21

            from
                {fl2}_LINE

            group by
                NEW_SUBMTG_STATE_CD_LINE,
                ORGNL_CLM_NUM_LINE,
                ADJSTMT_CLM_NUM_LINE,
                ADJDCTN_DT_LINE,
                LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)

        # Attach num_cll variable to header records as per instruction
        # Use this step to add in constructed variables for accomodation and ancillary paid amounts
        # Will probably need to move this step lower
        z = f"""
            create or replace temporary view {fl}_HEADER as
            select
                HEADER.*
                , coalesce(CONSTR.NUM_CLL,0) as NUM_CLL
                , CONSTR.ACCOMMODATION_PAID
                , CONSTR.ANCILLARY_PAID
                , case when CONSTR.MH_DAYS_OVER_65 = 1 then HEADER.MDCD_CVRD_IP_DAYS_CNT
                    when CONSTR.MH_DAYS_OVER_65 = 0 and  HEADER.MDCD_CVRD_IP_DAYS_CNT is not null then 0
                    end as CVRD_MH_DAYS_OVER_65
                , case when CONSTR.MH_DAYS_UNDER_21 = 1 then HEADER.MDCD_CVRD_IP_DAYS_CNT
                    when CONSTR.MH_DAYS_UNDER_21 = 0 and  HEADER.MDCD_CVRD_IP_DAYS_CNT is not null then 0
                    end as CVRD_MH_DAYS_UNDER_21

            from
                FA_HDR_{fl} HEADER left join constructed_{fl2} CONSTR

              on
                HEADER.NEW_SUBMTG_STATE_CD = CONSTR.NEW_SUBMTG_STATE_CD_LINE and
                HEADER.ORGNL_CLM_NUM = CONSTR.ORGNL_CLM_NUM_LINE and
                HEADER.ADJSTMT_CLM_NUM = CONSTR.ADJSTMT_CLM_NUM_LINE and
                HEADER.ADJDCTN_DT = CONSTR.ADJDCTN_DT_LINE and
                HEADER.ADJSTMT_IND = CONSTR.LINE_ADJSTMT_IND
        """
        self.runner.append(self.st_fil_type, z)


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
