from taf.TAF import TAF
from taf.TAF_Closure import TAF_Closure
from taf.FTX.FTX_Runner import FTX_Runner
from taf.FTX.FTX_Metadata import FTX_Metadata
from taf.TAF_Claims import TAF_Claims
from taf.TAF_Metadata import TAF_Metadata


class FTX(TAF):

    def __init__(self, runner: FTX_Runner):
        super().__init__(runner)
        self.st_fil_type = "FTX"

    ### create functions to do the work in this class.
    def AWS_Extract_FTX_segment(
        self,
        TMSIS_SCHEMA,
        fl,
        tab_no,
        _2x_segment,
        analysis_date_start,
        analysis_date_end,
        rep_mo,
        rep_yr,
    ):
        """
        Pull line item records for header records linked with claims family table dataset.
        """

        # Create a temporary line file
        z = f"""
            create or replace temporary view {_2x_segment}_IN as
            select
            { FTX_Metadata.selectDataElements(tab_no, 'a') }
            from
                {TMSIS_SCHEMA}.{_2x_segment} A
            where
                a.TMSIS_ACTV_IND = 1
                and concat(a.submtg_state_cd,a.tmsis_run_id) in ({self.runner.get_combined_list()})
                and (A.ADJSTMT_IND <> '1' or A.ADJSTMT_IND IS NULL)
                and (
                        year(coalesce({analysis_date_end.replace('"', '')},{analysis_date_start.replace('"', '')})) = {rep_yr}
                        and date_part ('month', coalesce({analysis_date_end.replace('"', '')},{analysis_date_start.replace('"', '')})) = {rep_mo}
                    )
        """
        self.runner.append(self.st_fil_type, z)

        # dedup
        z = f"""
            create or replace temporary view {_2x_segment}_nodups as
            with nodups as 
                (
                    select
                        TMSIS_RUN_ID
                        ,SUBMTG_STATE_CD
                        ,ORGNL_CLM_NUM
                        ,ADJSTMT_CLM_NUM
                        ,ADJSTMT_IND
                    from {_2x_segment}_IN
                    group by TMSIS_RUN_ID
                        ,SUBMTG_STATE_CD
                        ,ORGNL_CLM_NUM
                        ,ADJSTMT_CLM_NUM
                        ,ADJSTMT_IND
                    having count(TMSIS_RUN_ID) = 1
                ) 
            select b.*
            from {_2x_segment}_IN as b inner join nodups
            on (
                    nodups.TMSIS_RUN_ID = b.TMSIS_RUN_ID and
                    nodups.SUBMTG_STATE_CD = b.SUBMTG_STATE_CD and
                    nodups.ORGNL_CLM_NUM = b.ORGNL_CLM_NUM and
                    nodups.ADJSTMT_CLM_NUM = b.ADJSTMT_CLM_NUM and
                    nodups.ADJSTMT_IND = b.ADJSTMT_IND
                )
            """
        self.runner.append(self.st_fil_type, z)

        # in with statement, limit clm fam table to final action = 1 and adjst indicator not null and not equal to 1.
        # clean keys, remove dups
        # then inner join that with ftx deduplicated records to limit to final action = 1
        z = f"""
            create or replace temporary view ALL_{_2x_segment} as
                with CLM_FMLY_{_2x_segment} as
                    (
                            select
                            TMSIS_RUN_ID
                            ,coalesce(upper(ORGNL_CLM_NUM), '~') as ORGNL_CLM_NUM
                            ,coalesce(upper(ADJSTMT_CLM_NUM), '~') as ADJSTMT_CLM_NUM
                            ,trim(SUBMTG_STATE_CD) as SUBMTG_STATE_CD
                            ,COALESCE(UPPER(ADJSTMT_IND),'X') as ADJSTMT_IND
                        from {TMSIS_SCHEMA}.tmsis_clm_fmly_{tab_no} as a
                        where clm_fmly_finl_actn_ind  = 1
                                and concat(submtg_state_cd,tmsis_run_id) in ({self.runner.get_combined_list()})
                        group by
                            1,2,3,4,5
                        having
                            count(tmsis_run_id) = 1
                    )
                select
                    H.*
                    ,'{tab_no}' as TMSIS_SGMT_NUM
                    ,case when MSIS_IDENT_NUM is not null then 1 else 0 end as INDVDL_BENE_IND
                from {_2x_segment}_nodups as H
                inner join
                CLM_FMLY_{_2x_segment} as F
                on (
                    H.ORGNL_CLM_NUM = F.ORGNL_CLM_NUM
                    and H.ADJSTMT_CLM_NUM = F.ADJSTMT_CLM_NUM
                    and H.ADJSTMT_IND = F.ADJSTMT_IND
                    and H.SUBMTG_STATE_CD = F.SUBMTG_STATE_CD
                    and H.TMSIS_RUN_ID = F.TMSIS_RUN_ID
                )
        """
        self.runner.append(self.st_fil_type, z)

    def stack_segments(self, input_tables):
        z = f"""
            create or replace temporary view COMBINED_FTX as"""

        for key, vals in input_tables.items():
            z += f"""
            select * from ALL_{key}
            union all"""
        z = z[:-9]

        self.runner.append(self.st_fil_type, z)

    def create(self, runner: FTX_Runner):
        """
        Create the FTX segment.
        """

        # for MBESCBES_FRM below, we are compressing spaces for the valid value check, but outputting the original value.
        z = f"""
            create or replace temporary view FTX as

            select

                {runner.DA_RUN_ID} as DA_RUN_ID,
                '{runner.VERSION}' as FTX_VRSN,
                '{runner.TAF_FILE_DATE}' as FTX_FIL_DT

                , TMSIS_RUN_ID
                , TMSIS_SGMT_NUM
                , INDVDL_BENE_IND
                , { TAF_Closure.var_set_type1('MSIS_IDENT_NUM') }
                , SUBMTG_STATE_CD
                , { TAF_Closure.var_set_type3('orgnl_clm_num', cond1='~') }
                , { TAF_Closure.var_set_type3('adjstmt_clm_num', cond1='~') }
                , ADJSTMT_IND_CLEAN as ADJSTMT_IND
                , case
                    when (PMT_OR_RCPMT_DT < to_date('1600-01-01')) then to_date('1599-12-31')
                    when PMT_OR_RCPMT_DT=to_date('1960-01-01') then NULL
                    else PMT_OR_RCPMT_DT
                end as PMT_OR_RCPMT_DT
                , { TAF_Closure.var_set_type6('PMT_OR_RCPMT_AMT', cond1='888888888.88', cond2='99999999.90', cond3='9999999.99', cond4='999999.99', cond5='999999.00') }
                , case
                    when (CHK_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
                    when CHK_EFCTV_DT=to_date('1960-01-01') then NULL
                    else CHK_EFCTV_DT
                end as CHK_EFCTV_DT
                , { TAF_Closure.var_set_type1('PYR_ID') }
                , { TAF_Closure.var_set_type2('PYR_ID_TYPE_CD', 2, cond1='01', cond2='02', cond3='03', cond4 = '04', cond5='95') }
                , case when PYR_MC_PLN_TYPE_CD is not null 
                    and lpad(trim(PYR_MC_PLN_TYPE_CD), 2, '0') in ('01','02','03','04','05','06','07','08','09',
                                                                    '10','11','12','13','14','15','16','17','18','19','20',
                                                                    '60','70','80') then lpad(trim(PYR_MC_PLN_TYPE_CD), 2, '0') else null end as PYR_MC_PLN_TYPE_CD
                , { TAF_Closure.var_set_type1('PYEE_ID') }
                , { TAF_Closure.var_set_type2('PYEE_ID_TYPE_CD', 2, cond1='01', cond2='02', cond3='03', cond4 = '04', cond5='05'
                                                                , cond6='06', cond7='07', cond8='08', cond9='09', cond10='95') }
                , case when PYEE_MC_PLN_TYPE_CD is not null
                    and lpad(trim(PYEE_MC_PLN_TYPE_CD ), 2, '0') in ('01','02','03','04','05','06','07','08','09',
                                                                    '10','11','12','13','14','15','16','17','18','19','20',
                                                                    '60','70','80') then lpad(trim(PYEE_MC_PLN_TYPE_CD ), 2, '0') else null end as PYEE_MC_PLN_TYPE_CD
                , { TAF_Closure.var_set_type1('PYEE_TAX_ID') }
                , { TAF_Closure.var_set_type2('PYEE_TAX_ID_TYPE_CD', 2, cond1='01', cond2='02', cond3='03', cond4 = '04', cond5='95') }
                , lpad(trim(SSN_NUM),9,'0') as SSN_NUM
                , { TAF_Closure.var_set_type1('PLCY_MMBR_ID') }
                , { TAF_Closure.var_set_type1('PLCY_GRP_NUM') }
                , { TAF_Closure.var_set_type1('PLCY_OWNR_CD') }
                , { TAF_Closure.var_set_type1('INSRNC_PLN_ID') }
                , { TAF_Closure.var_set_type1('INSRNC_CARR_ID_NUM') }
                , case
                    when (PMT_PRD_EFF_DT < to_date('1600-01-01')) then to_date('1599-12-31')
                    when PMT_PRD_EFF_DT=to_date('1960-01-01') then NULL
                    else PMT_PRD_EFF_DT
                end as PMT_PRD_EFF_DT
                , case
                    when (PMT_PRD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
                    when PMT_PRD_END_DT=to_date('1960-01-01') then NULL
                    else PMT_PRD_END_DT
                end as PMT_PRD_END_DT
                , { TAF_Closure.var_set_type2('PMT_PRD_TYPE_CD', 2, cond1='01', cond2='02', cond3='03', cond4 = '04', cond5='05', cond6 = '95') }
                , { TAF_Closure.var_set_type2('TRNS_TYPE_CD', 2, cond1='01', cond2='02', cond3='03', cond4 = '04', cond5='95') }
                , { TAF_Closure.var_set_type2('FED_RIMBRSMT_CTGRY', 2, cond1='01', cond2='02', cond3='03', cond4 = '04', cond5='95') }
                , { TAF_Closure.var_set_type2('MBESCBES_FRM_GRP', 0, cond1='1', cond2='2', cond3='3') }
                , case when replace(upper(trim(MBESCBES_FRM)),' ','') in {tuple(x.replace(" ","") for x in TAF_Metadata.MBESCBES_FRM_values)} then upper(trim(MBESCBES_FRM)) else NULL end as MBESCBES_FRM

                , case when upper(lpad(trim(MBESCBES_SRVC_CTGRY),5,'0')) in {tuple(TAF_Metadata.MBESCBES_SRVC_CTGRY_values)}
                    then upper(lpad(trim(MBESCBES_SRVC_CTGRY),5,'0'))
                    else NULL end as MBESCBES_SRVC_CTGRY

                , { TAF_Closure.var_set_type1('WVR_ID') }
                , case when lpad(wvr_type_cd, 2, '0') = '88' then NULL
                    else { TAF_Closure.var_set_type5('wvr_type_cd', lpad=2, lowerbound=1, upperbound=33, multiple_condition='YES') }
                , { TAF_Closure.var_set_type4('FUNDNG_CD', 'YES', cond1='A', cond2='B', cond3='C', cond4='D', cond5='E', cond6='F', cond7='G', cond8='H', cond9='I') }
                , { TAF_Closure.var_set_type2('fundng_src_non_fed_shr_cd', 2, cond1='01', cond2='02', cond3='03', cond4='04', cond5='05', cond6='06') }
                , { TAF_Closure.var_set_type2('OFST_TYPE_CD', 0, cond1='1', cond2='2', cond3='3') }
                , { TAF_Closure.var_set_type2('SDP_IND', 0, cond1='0', cond2='1') }
                , case when SRC_LCTN_CD is NOT NULL and lpad(trim(SRC_LCTN_CD), 2, '0')
                    in ('01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '20', '22', '23') then lpad(trim(SRC_LCTN_CD), 2, '0')
                    else NULL end as SRC_LCTN_CD
                , { TAF_Closure.var_set_type1('SPA_NUM') }
                , { TAF_Closure.var_set_type2('SUBCPTATN_IND', 0, cond1='1', cond2='2') }
                , { TAF_Closure.var_set_type1('PMT_CTGRY_XREF') }
                , { TAF_Closure.var_set_type2('APM_MODEL_TYPE_CD', 0, cond1='2A', cond2='2B', cond3='2C',
                                                                    cond4='3A', cond5='3B', cond6='3N',
                                                                    cond7='4A', cond8='4B', cond9='4C', cond10='4N') }
                , { TAF_Closure.var_set_type2('EXPNDTR_AUTHRTY_TYPE_CD', 2, cond1='01', cond2='95') }
                , from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                , from_utc_timestamp(current_timestamp(), 'EST') as REC_UPDT_TS             --this must be equal to REC_ADD_TS for CCW pipeline
            from (
                select
                    *,
                    case when ADJSTMT_IND is NOT NULL and
                    trim(ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6')
                    then trim(ADJSTMT_IND) else NULL end as ADJSTMT_IND_CLEAN
                from
                    COMBINED_FTX
                ) H
            """
        runner.append("FTX", z)

    def build(self, runner: FTX_Runner):
        """
        Build the OT claim-header level segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(
                f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **"
            )
            return

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.TAF_FTX
                SELECT
                    { FTX_Metadata.finalFormatter(FTX_Metadata.ftx_cols) }
                FROM FTX
        """
        runner.append(type(self).__name__, z)


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
