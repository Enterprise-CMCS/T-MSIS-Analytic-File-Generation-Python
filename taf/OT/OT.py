from taf.OT.OT_Metadata import OT_Metadata
from taf.OT.OT_Runner import OT_Runner
from taf.TAF import TAF
from taf.TAF_Closure import TAF_Closure


class OT(TAF):
    """
    Other Services (OT) TAF: The OT TAF contains information about claims for services other
    than those provided by an inpatient hospital, long-term care facility, or pharmacy as
    submitted to T-MSIS by their respective state agencies. Services in the OT TAF include
    but are not limited to: physician services, outpatient hospital services, dental services,
    other physician services (i.e. chiropractors, podiatrists, psychologists, optometrists, etc.),
    clinic services, laboratory services, X-ray services, sterilizations, home health services,
    personal support services, and managed care capitation payments. The claims in TAF include
    fee-for-service claims, managed care encounter claims, service tracking claims, capitated payments,
    and supplemental payments for Medicaid, Medicaid-expansion CHIP, and Separate CHIP. Inclusion in
    the OT TAF is based on the month/year of the ending date of service from the claim header.
    If the service end date is missing, the OT TAF uses the service begin date from the claim header.
    If the service begin date on the claim header is missing, the OT TAF uses the most recent service
    end date from the claim line. Records in TAF begin when the state officially cut over to submitting
    T-MSIS data. Each file provides T-MSIS source data as well as constructed variables designed to
    support research and analysis. The constructed variables are designed to facilitate analysis such
    as outcomes measurement, public reporting, quality improvement initiatives, and quality monitoring,
    among other items.

    Each OT TAF is comprised of two files – a claim-header level file and a claim-line level file.
    The claims included in these files are active, final-action, non-voided, and non-denied claims.
    Only header claims with a date in the TAF month/year, along with their associated claim line records,
    are included. Both files can be linked together using a unique key that is constructed based on various
    claim header and claim line data elements. The two OT TAF are produced for each calendar month in
    which the data are reported.
    """

    def __init__(self, runner: OT_Runner):
        super().__init__(runner)
        self.st_fil_type = "OTHR_TOC"

    def AWS_Extract_Line(self, TMSIS_SCHEMA, DA_SCHEMA, fl2, fl, tab_no, _2x_segment):
        """
        Pull OT line item records for header records linked with claims family table dataset
        """

        # Create a temporary line file
        z = f"""
            create or replace temporary view {fl2}_LINE_IN as
            select
                SUBMTG_STATE_CD,
                { OT_Metadata.selectDataElements(tab_no, 'a') }

            from
                {TMSIS_SCHEMA}.{_2x_segment} A
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
                ) as RN ,
                    a.submtg_state_cd as new_submtg_state_cd_line

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
									      cond5="NONE", cond6="XXXXXXXXXX", cond7="NO TAXONOMY", new="SRVCNG_PRVDR_NPPES_TXNMY_CD")}
                ,ccs.ccs as prcdr_ccs_ctgry_cd
            from {fl2}_LINE_PRE_NPPES as a
            left join NPPES_NPI n
                on n.prvdr_npi = a.PRSCRBNG_PRVDR_NPI_NUM    ---misnomer on OT input
            left join ccs_proc ccs
                on ccs.cd_rng = a.prcdr_cd
        """
        self.runner.append(self.st_fil_type, z)

        z = f"""
            create or replace temporary view RN_{fl2} as
            select
                NEW_SUBMTG_STATE_CD_LINE
                , ORGNL_CLM_NUM_LINE
                , ADJSTMT_CLM_NUM_LINE
                , ADJDCTN_DT_LINE
                , LINE_ADJSTMT_IND
                , max(RN) as NUM_CLL

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
        # Use this step to add in transposed DX codes and flags
        # If there's no line in the dx file, set the additional dgns prsnt flag to 0.
        z = f"""
            create or replace temporary view {fl2}_HEADER as
            select
                HEADER.*
                ,coalesce(RN.NUM_CLL,0) as NUM_CLL
                ,dx.DGNS_1_CD
                ,dx.DGNS_1_CD_IND
                ,dx.DGNS_2_CD
                ,dx.DGNS_2_CD_IND
                ,coalesce(dx.addtnl_dgns_prsnt,0) as addtnl_dgns_prsnt

            from
                FA_HDR_{fl} HEADER left join RN_{fl2} RN

            on
                HEADER.NEW_SUBMTG_STATE_CD = RN.NEW_SUBMTG_STATE_CD_LINE and
                HEADER.ORGNL_CLM_NUM = RN.ORGNL_CLM_NUM_LINE and
                HEADER.ADJSTMT_CLM_NUM = RN.ADJSTMT_CLM_NUM_LINE and
                HEADER.ADJDCTN_DT = RN.ADJDCTN_DT_LINE and
                HEADER.ADJSTMT_IND = RN.LINE_ADJSTMT_IND
            left join dx_wide_{fl} as dx
            on (
                    HEADER.NEW_SUBMTG_STATE_CD = dx.NEW_SUBMTG_STATE_CD and
                    HEADER.ORGNL_CLM_NUM = dx.ORGNL_CLM_NUM and
                    HEADER.ADJSTMT_CLM_NUM = dx.ADJSTMT_CLM_NUM and
                    HEADER.ADJDCTN_DT = dx.ADJDCTN_DT and
                    HEADER.ADJSTMT_IND = dx.ADJSTMT_IND
            )
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
            tab_no:  number for the segment ie COT00004
            _2x_segment:  name of the input T-MSIS DX segment
            fl:  file type (IP, OTHR_TOC, LT, RX)
            header_in:  name of header view to use for limiting dx records to only claims being processed.  Also used for joining back to header file.
            numdx = the number of DX fields to be transposed and joined to header file.
        """

        z = f"""
            create or replace temporary view dx_{fl} as
                select dx_all.*
                ,h.msis_ident_num
                ,h.new_submtg_state_cd
                ,row_number() over (Partition by h.new_submtg_state_cd, dx_all.orgnl_clm_num,dx_all.adjstmt_clm_num
                                                ,dx_all.adjstmt_ind,dx_all.adjdctn_dt order by dx_all.DGNS_SQNC_NUM,sort_val) as h_iteration
                from
                    (
                    select
                    { OT_Metadata.selectDataElements(tab_no, 'a') }
                    ,case
                            when trim(upper(dgns_type_cd)) = "P" then 1
                            when trim(upper(dgns_type_cd)) = "D" then 2
                            when trim(upper(dgns_type_cd)) = "O" then 3
                            when trim(upper(dgns_type_cd)) = "E" then 4
                            when trim(upper(dgns_type_cd)) = "R" then 5
                            else 6 end as sort_val
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
            """
        self.runner.append(fl, z)
        
        #transpose the DX file to the appropriate number of DX fields.
        z = f"""
            create or replace temporary view dx_wide_{fl} as
            select 
                new_submtg_state_cd
                ,orgnl_clm_num
                ,adjstmt_clm_num
                ,adjstmt_ind
                ,adjdctn_dt
                ,max(case when h_iteration > {numdx} then 1 else 0 end) as addtnl_dgns_prsnt"""
        for i in range(1,numdx+1):
            z+= f"""
                ,max(case when h_iteration = {i} then DGNS_CD else null end) as dgns_{i}_cd
                ,max(case when h_iteration = {i} then dgns_cd_ind else null end) as DGNS_{i}_CD_IND """
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
