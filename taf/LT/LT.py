from taf.LT.LT_Metadata import LT_Metadata
from taf.LT.LT_Runner import LT_Runner
from taf.TAF import TAF
from taf.TAF_Closure import TAF_Closure


class LT(TAF):
    """
    Long-Term Care Claims (LT) TAF: The LT TAF contains information about long-term care 
    institution claims, including nursing facilities, intermediate care facility services 
    for individuals with intellectual disabilities, mental health facility services, and 
    independent (free-standing) psychiatric wings of acute care hospitals. The claims in TAF 
    include fee-for-service claims, managed care encounter claims, service tracking claims, 
    and supplemental payments for Medicaid, Medicaid-expansion CHIP, and Separate CHIP. 
    Inclusion in the LT TAF is based on the month/year of the ending date of service in 
    the claim header. Each LT TAF is comprised of two files – a claim-header level file 
    and a claim-line level file. The claims included in these files are active, non-voided, 
    non-denied (at the header level), non-duplicate final action claims. Only claim header 
    records meeting these inclusion criteria, along with their associated claim line records, 
    are incorporated. Both files can be linked together using unique keys that are constructed 
    based on various claim header and claim line data elements. The two LT TAF are generated 
    for each calendar month in which the data are reported.
    """
    
    def __init__(self, runner: LT_Runner):
        super().__init__(runner)
        self.st_fil_type = "LT"

    def AWS_Extract_Line(self, TMSIS_SCHEMA, DA_SCHEMA, fl2, fl, tab_no, _2x_segment):
        """
        Pull line item records for header records linked with claims family table dataset.
        """
         
        # Subset line file and attach row numbers to all records belonging to an ICN set.  Fix PA & IA
        z = f"""
            create or replace temporary view {fl2}_LINE_IN as
            select
                SUBMTG_STATE_CD,
                { LT_Metadata.selectDataElements(tab_no, 'a') }

            from
                {TMSIS_SCHEMA}.{_2x_segment} A
            where
                a.TMSIS_ACTV_IND = 1
                and concat(a.submtg_state_cd,a.tmsis_run_id) in ({self.runner.get_combined_list()})
        """
        self.runner.append(self.st_fil_type, z)

        z = f"""
            --- will this fail?
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
                ) as RN
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
                upper(H.ADJSTMT_IND) = upper(a.LINE_ADJSTMT_IND)
        """
        self.runner.append(self.st_fil_type, z)

        # join line file with NPPES to pick up servicing provider taxonomy code
        z = f"""
            create or replace temporary view {fl2}_LINE as
            select
                a.*
               ,{TAF_Closure.var_set_taxo("SELECTED_TXNMY_CD",cond1="8888888888", cond2="9999999999", cond3="000000000X", cond4="999999999X",
									      cond5="NONE", cond6="XXXXXXXXXX", cond7="NO TAXONOMY", new="SRVCNG_PRVDR_NPPES_TXNMY_CD")}
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
                , upper(LINE_ADJSTMT_IND) as LINE_ADJSTMT_IND
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
                upper(HEADER.ADJSTMT_IND) = upper(CONSTR.LINE_ADJSTMT_IND)
        """
        self.runner.append(self.st_fil_type, z)

    def select_dx(self, TMSIS_SCHEMA, tab_no, _2x_segment, fl, header_in, numdx=0):
        """
        Added for T-MSIS v4 changes, as TAF 9.0

        Extract elements from DX table, keeping rows associated wtih headers currently being processed.
        Apply data cleaning to elements
        Prepare DX level table for output
        Transpose DX table

        Function inputs:
            TMSIS_SCHEMA:  Name of the schema holding the raw data
            tab_no:  number for the segment ie CLT00004
            _2x_segment:  name of the input T-MSIS DX segment
            fl:  file type (IP, OTHR_TOC, LT, RX)
            header_in:  name of header view to use for limiting dx records to only claims being processed.  Also used for joining back to header file.
            numdx = the number of DX fields to be transposed and joined to header file.
        """

        z = f"""
            create or replace temporary view dx_{fl} as
              select *,
                    row_number() over (Partition by new_submtg_state_cd, orgnl_clm_num, adjstmt_clm_num
                                                    ,adjstmt_ind,adjdctn_dt order by null_flag, admitting_flag, principal_flag, DGNS_SQNC_NUM,sort_val) + 1 as h_iteration
            from (
                select dx_all.*
                ,h.msis_ident_num
                ,h.new_submtg_state_cd
                ,case 
                  when trim(upper(dgns_type_cd)) = "A"
                    and null_flag = 0
                    and row_number() over (Partition by h.new_submtg_state_cd, dx_all.orgnl_clm_num,dx_all.adjstmt_clm_num
                                                 ,dx_all.adjstmt_ind,dx_all.adjdctn_dt, trim(dx_all.dgns_type_cd) order by null_flag, dx_all.DGNS_SQNC_NUM) = 1 then 1 else 0 end as admitting_flag
                ,case 
                  when trim(upper(dgns_type_cd)) = "P"
                    and null_flag = 0
                    and row_number() over (Partition by h.new_submtg_state_cd, dx_all.orgnl_clm_num,dx_all.adjstmt_clm_num
                                                 ,dx_all.adjstmt_ind,dx_all.adjdctn_dt, trim(dx_all.dgns_type_cd) order by null_flag, dx_all.DGNS_SQNC_NUM) = 1 then 1 else 0 end as principal_flag
                
                from
                    (
                    select
                    { LT_Metadata.selectDataElements(tab_no, 'a') }
                    ,case
                            when trim(upper(dgns_type_cd)) = "P" then 1
                            when trim(upper(dgns_type_cd)) = "A" then 2
                            when trim(upper(dgns_type_cd)) = "D" then 3
                            when trim(upper(dgns_type_cd)) = "O" then 4
                            when trim(upper(dgns_type_cd)) = "E" then 5
                            when trim(upper(dgns_type_cd)) = "R" then 6
                            else 7 end as sort_val
                    ,case when DGNS_SQNC_NUM is null then 1 else 0 end as null_flag
                    from
                        {TMSIS_SCHEMA}.{_2x_segment} as a
                    where
                        concat(a.submtg_state_cd, a.tmsis_run_id) in ({self.runner.get_combined_list()})
                    ) as dx_all
                inner join
                    {header_in} as h
                on (
                    dx_all.TMSIS_RUN_ID = h.TMSIS_RUN_ID and
                    dx_all.ORGNL_CLM_NUM = h.ORGNL_CLM_NUM and
                    dx_all.ADJSTMT_CLM_NUM = h.ADJSTMT_CLM_NUM and
                    dx_all.ADJDCTN_DT = h.ADJDCTN_DT and
                    dx_all.ADJSTMT_IND = h.ADJSTMT_IND
                    )
                where (length(trim(DGNS_CD)) - coalesce(length(regexp_replace(trim(DGNS_CD), '[^0]+', '')), 0))!= 0
                    and (length(trim(DGNS_CD)) - coalesce(length(regexp_replace(trim(DGNS_CD), '[^8]+', '')), 0)) != 0
                    and (length(trim(DGNS_CD)) - coalesce(length(regexp_replace(trim(DGNS_CD), '[^9]+', '')), 0)) != 0
                    and (length(trim(DGNS_CD)) - coalesce(length(regexp_replace(trim(DGNS_CD), '[^#]+', '')), 0)) != 0
                    and nullif(trim(DGNS_CD),'') is not null
            )
            """
        self.runner.append(fl, z)
        
        #transpose the DX file to the appropriate number of DX fields.
        z = f"""
            create or replace temporary view dx_wide as
            select
                new_submtg_state_cd
                ,orgnl_clm_num
                ,adjstmt_clm_num
                ,adjstmt_ind
                ,adjdctn_dt
                ,max(case when h_iteration > {numdx} and admitting_flag = 0 and principal_flag = 0 then 1
                          when null_flag = 1 then 1
                        else 0 end) as addtnl_dgns_prsnt
                ,max(case when admitting_flag = 1 then DGNS_CD else null end) as ADMTG_DGNS_CD
                ,max(case when admitting_flag = 1 then DGNS_CD_IND else null end) as ADMTG_DGNS_CD_IND
                ,max(case when principal_flag = 1 then DGNS_CD else null end) as DGNS_1_CD
                ,max(case when principal_flag = 1 then DGNS_CD_IND else null end) as DGNS_1_CD_IND
                ,max(case when principal_flag = 1 then DGNS_POA_IND else null end) as dgns_poa_1_cd_ind"""
                
        for i in range(2,numdx+1):
            z+= f"""
                ,max(case when h_iteration = {i} and admitting_flag = 0 and null_flag = 0 and principal_flag = 0 then DGNS_CD else null end) as dgns_{i}_cd
                ,max(case when h_iteration = {i} and admitting_flag = 0 and null_flag = 0 and principal_flag = 0 then dgns_cd_ind else null end) as DGNS_{i}_CD_IND
                ,max(case when h_iteration = {i} and admitting_flag = 0 and null_flag = 0 and principal_flag = 0 then DGNS_POA_IND else null end) as dgns_poa_{i}_cd_ind"""
        z += f"""
            from dx_{fl}
            group by
                new_submtg_state_cd
                ,orgnl_clm_num
                ,adjstmt_clm_num
                ,adjstmt_ind
                ,adjdctn_dt
        """
        self.runner.append(fl, z)


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
