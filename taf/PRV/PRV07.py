from taf.PRV import PRV_Runner

from taf.PRV.PRV import PRV


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class PRV07(PRV):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, prv: PRV_Runner):
        super().__init__(prv)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def process_07_medicaid(self, maintbl, outtbl):

        # screen out all but the latest(selected) run id - provider id
        runlist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id']

        self.screen_runid('tmsis.Prov_Medicaid_Enrollment',
                          maintbl,
                          runlist,
                          'Prov07_Medicaid_Latest1')

        # row count
        # self.prv.countrows(Prov07_Medicaid_Latest1, cnt_latest, PRV07_Latest)

        cols07 = ['tms_run_id',
                  'tms_reporting_period',
                  'record_number',
                  'submitting_state',
                  '%upper_case(submitting_state_prov_id) as submitting_state_prov_id',
                  '%zero_pad(prov_medicaid_enrollment_status_code, 2)',
                  'state_plan_enrollment',
                  'prov_enrollment_method',
                  '%fix_old_dates(appl_date)',
                  '%fix_old_dates(prov_medicaid_eff_date)',
                  '%set_end_dt(prov_medicaid_end_date) as prov_medicaid_end_date']

        whr07 = 'prov_medicaid_enrollment_status_code is not null'

        self.copy_activerows('Prov07_Medicaid_Latest1',
                             cols07,
                             whr07,
                             'Prov07_Medicaid_Copy')

        # row count
        # self.prv.countrows(Prov07_Medicaid_Copy, cnt_active, PRV07_Active)

        # screen for licensing during the month
        keylist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_medicaid_enrollment_status_code']

        self.screen_dates('Prov07_Medicaid_Copy',
                          keylist,
                          'prov_medicaid_eff_date',
                          'prov_medicaid_end_date',
                          'Prov07_Medicaid_Latest2')

        # row count
        # self.prv.countrows(Prov07_Medicaid_Latest2, cnt_date, PRV07_Date)

        # remove duplicate records
        grplist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_medicaid_enrollment_status_code']

        self.remove_duprecs('Prov07_Medicaid_Latest2',
                            grplist,
                            'prov_medicaid_eff_date',
                            'prov_medicaid_end_date',
                            'prov_medicaid_enrollment_status_code',
                            outtbl)

        # row count
        # self.prv.countrows(&outtbl, cnt_final, PRV07_Final)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        self.process_07_medicaid('Prov02_Main',
                                 'Prov07_Medicaid')

        # add code to validate and recode source variables (when needed), use SAS variable names, add linking variables, and sort records

        self.recode_notnull('Prov07_Medicaid',
                            self.srtlist,
                            'prv_formats_sm',
                            'STFIPC',
                            'submitting_state',
                            'SUBMTG_STATE_CD',
                            'Prov07_Medicaid_ST',
                            'C',
                            2)

        self.recode_lookup('Prov07_Medicaid_ST',
                           self.srtlist,
                           'prv_formats_sm',
                           'ENRSTCDV',
                           'prov_medicaid_enrollment_status_code',
                           'PRVDR_MDCD_ENRLMT_STUS_CD',
                           'Prov07_Medicaid_STS',
                           'C',
                           2)

        self.recode_lookup('Prov07_Medicaid_STS',
                           self.srtlist,
                           'prv_formats_sm',
                           'ENRSTCAT',
                           'PRVDR_MDCD_ENRLMT_STUS_CD',
                           'PRVDR_MDCD_ENRLMT_STUS_CTGRY',
                           'Prov07_Medicaid_STC',
                           'N')

        self.recode_lookup('Prov07_Medicaid_STC',
                           self.srtlist,
                           'prv_formats_sm',
                           'ENRCDV',
                           'state_plan_enrollment',
                           'STATE_PLAN_ENRLMT_CD',
                           'Prov07_Medicaid_ENR',
                           'C',
                           1)

        self.recode_lookup('Prov07_Medicaid_ENR',
                           self.srtlist,
                           'prv_formats_sm',
                           'ENRMDCDV',
                           'prov_enrollment_method',
                           'PRVDR_MDCD_ENRLMT_MTHD_CD',
                           'Prov07_Medicaid_MTD',
                           'C',
                           1)

        z = f"""
            create or replace temporary view Prov07_Medicaid_CNST as
            select { ','.join(self.srtlist) }, SUBMTG_STATE_CD, SPCL,
                    PRVDR_MDCD_ENRLMT_STUS_CD,
                    STATE_PLAN_ENRLMT_CD,
                    PRVDR_MDCD_ENRLMT_MTHD_CD,
                    appl_date as APLCTN_DT,
                    PRVDR_MDCD_ENRLMT_STUS_CTGRY,
                    prov_medicaid_eff_date as PRVDR_MDCD_EFCTV_DT,
                    prov_medicaid_end_date as PRVDR_MDCD_END_DT,
                    case
                    when STATE_PLAN_ENRLMT_CD=1 then 1
                    when STATE_PLAN_ENRLMT_CD is null then null
                    else 0
                    end as MDCD_ENRLMT_IND,
                    case
                    when STATE_PLAN_ENRLMT_CD=2 then 1
                    when STATE_PLAN_ENRLMT_CD is null then null
                    else 0
                    end as CHIP_ENRLMT_IND,
                    case
                    when STATE_PLAN_ENRLMT_CD=3 then 1
                    when STATE_PLAN_ENRLMT_CD is null then null
                    else 0
                    end as MDCD_CHIP_ENRLMT_IND,
                    case
                    when STATE_PLAN_ENRLMT_CD=4 then 1
                    when STATE_PLAN_ENRLMT_CD is null then null
                    else 0
                    end as NOT_SP_AFLTD_IND,
                    case
                    when PRVDR_MDCD_ENRLMT_STUS_CTGRY=1 then 1
                    when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
                    else 0
                    end as PRVDR_ENRLMT_STUS_ACTV_IND,
                    case
                    when PRVDR_MDCD_ENRLMT_STUS_CTGRY=2 then 1
                    when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
                    else 0
                    end as PRVDR_ENRLMT_STUS_DND_IND,
                    case
                    when PRVDR_MDCD_ENRLMT_STUS_CTGRY=3 then 1
                    when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
                    else 0
                    end as PRVDR_ENRLMT_STUS_PENDG_IND,
                    case
                    when PRVDR_MDCD_ENRLMT_STUS_CTGRY=4 then 1
                    when PRVDR_MDCD_ENRLMT_STUS_CTGRY is null then null
                    else 0
                    end as PRVDR_ENRLMT_STUS_TRMNTD_IND,
                    /* grouping code */
                    row_number() over (
                    partition by { ','.join(self.srtlist) }
                    order by record_number asc
                    ) as _ndx
            from Prov07_Medicaid_MTD
            where PRVDR_MDCD_ENRLMT_STUS_CD is not null
            order by { ','.join(self.srtlist) }
            """
        self.prv.append(type(self).__name__, z)

        # create the separate linked child table

        z = f"""
            create or replace temporary view Prov07_Medicaid_ENRPOP as
            select {self.prv.DA_RUN_ID} as DA_RUN_ID,
                    case
                    when SPCL is not null then
                    cast (('{self.prv.version}' || '-' || { self.prv.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || SPCL) as varchar(50))
                    else
                    cast (('{self.prv.version}' || '-' || { self.prv.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')) as varchar(50))
                    end as PRV_LINK_KEY,
                    '{self.prv.TAF_FILE_DATE}' as PRV_FIL_DT,
                    '{self.prv.version}' as PRV_VRSN,
                    tms_run_id as TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
                    PRVDR_MDCD_EFCTV_DT,
                    PRVDR_MDCD_END_DT,
                    PRVDR_MDCD_ENRLMT_STUS_CD,
                    STATE_PLAN_ENRLMT_CD,
                    PRVDR_MDCD_ENRLMT_MTHD_CD,
                    APLCTN_DT,
                    PRVDR_MDCD_ENRLMT_STUS_CTGRY,
                    to_timestamp('{self.prv.DA_RUN_ID}', 'yyyyMMddHHmmss') as REC_ADD_TS,
                    current_timestamp() as REC_UPDT_TS
                    from Prov07_Medicaid_CNST
            order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID
            """
        self.prv.append(type(self).__name__, z)

        # insert contents of temp table into final TAF file for segment 7

        # insert into &DA_SCHEMA..TAF_PRV_ENR
        # select *
        # from Prov07_Medicaid_ENRPOP

        # final step in creating constructed variables
        z = f"""
            create or replace temporary view Prov07_Medicaid_Mapped as
            select { ','.join(self.srtlist) },
                    1 as PRVDR_MDCD_ENRLMT_IND,
                    max(MDCD_ENRLMT_IND) as MDCD_ENRLMT_IND,
                    max(CHIP_ENRLMT_IND) as CHIP_ENRLMT_IND,
                    max(MDCD_CHIP_ENRLMT_IND) as MDCD_CHIP_ENRLMT_IND,
                    max(NOT_SP_AFLTD_IND) as NOT_SP_AFLTD_IND,
                    max(PRVDR_ENRLMT_STUS_ACTV_IND) as PRVDR_ENRLMT_STUS_ACTV_IND,
                    max(PRVDR_ENRLMT_STUS_DND_IND) as PRVDR_ENRLMT_STUS_DND_IND,
                    max(PRVDR_ENRLMT_STUS_TRMNTD_IND) as PRVDR_ENRLMT_STUS_TRMNTD_IND,
                    max(PRVDR_ENRLMT_STUS_PENDG_IND) as PRVDR_ENRLMT_STUS_PENDG_IND
            from Prov07_Medicaid_CNST
            group by { ','.join(self.srtlist) }
            """
        self.prv.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view Prov02_Base as
                select M.DA_RUN_ID,
                        M.prv_link_key,
                        M.PRV_FIL_DT,
                        M.PRV_VRSN ,
                        M.tmsis_run_id,
                        M.submtg_state_cd,
                        M.submtg_state_prvdr_id,
                        M.REG_FLAG,
                        M.prvdr_dba_name,
                        M.prvdr_lgl_name,
                        M.prvdr_org_name,
                        M.prvdr_tax_name,
                        M.fac_grp_indvdl_cd,
                        M.tchng_ind,
                        M.prvdr_1st_name,
                        M.prvdr_mdl_initl_name,
                        M.prvdr_last_name,
                        M.gndr_cd,
                        M.ownrshp_cd,
                        M.ownrshp_cat,
                        M.prvdr_prft_stus_cd,
                        M.birth_dt,
                        M.death_dt,
                        M.acpt_new_ptnts_ind,
                        M.age_num,
                        coalesce(E.PRVDR_MDCD_ENRLMT_IND, 0) as PRVDR_MDCD_ENRLMT_IND,
                        case
                        when E.MDCD_CHIP_ENRLMT_IND=1 then 0
                        else E.MDCD_ENRLMT_IND
                        end as MDCD_ENRLMT_IND,
                        case
                        when E.MDCD_CHIP_ENRLMT_IND=1 then 0
                        else E.CHIP_ENRLMT_IND
                        end as CHIP_ENRLMT_IND,
                        E.MDCD_CHIP_ENRLMT_IND,
                        E.NOT_SP_AFLTD_IND,
                        E.PRVDR_ENRLMT_STUS_ACTV_IND,
                        E.PRVDR_ENRLMT_STUS_DND_IND,
                        E.PRVDR_ENRLMT_STUS_TRMNTD_IND,
                        E.PRVDR_ENRLMT_STUS_PENDG_IND,
                        T.MLT_SNGL_SPCLTY_GRP_IND,
                        T.ALPTHC_OSTPTHC_PHYSN_IND,
                        T.BHVRL_HLTH_SCL_SRVC_PRVDR_IND,
                        T.CHRPRCTIC_PRVDR_IND,
                        T.DNTL_PRVDR_IND,
                        T.DTRY_NTRTNL_SRVC_PRVDR_IND,
                        T.emer_MDCL_srvc_prvdr_ind,
                        T.eye_VSN_srvc_prvdr_ind,
                        T.nrsng_srvc_prvdr_ind,
                        T.nrsng_srvc_rltd_ind,
                        T.othr_INDVDL_srvc_prvdr_ind,
                        T.PHRMCY_SRVC_PRVDR_IND,
                        T.PA_ADVCD_PRCTC_NRSNG_PRVDR_IND,
                        T.POD_MDCN_SRGRY_SRVCS_IND,
                        T.resp_dev_reh_restor_prvdr_ind,
                        T.SPCH_LANG_HEARG_SRVC_PRVDR_IND,
                        T.STDNT_HLTH_CARE_PRVDR_IND,
                        T.TT_OTHR_TCHNCL_SRVC_PRVDR_IND,
                        T.agncy_prvdr_ind,
                        T.amb_hlth_CARE_fac_prvdr_ind,
                        T.hosp_unit_prvdr_ind,
                        T.hosp_prvdr_ind,
                        T.lab_prvdr_ind,
                        T.mco_prvdr_ind,
                        T.NRSNG_CSTDL_CARE_FAC_IND,
                        T.OTHR_NONINDVDL_SRVC_PRVDRS_IND,
                        T.RSDNTL_TRTMT_FAC_PRVDR_IND,
                        T.RESP_CARE_FAC_PRVDR_IND,
                        T.SUPLR_PRVDR_IND,
                        T.TRNSPRTN_SRVCS_PRVDR_IND,
                        T.sud_srvc_prvdr_ind,
                        T.mh_srvc_prvdr_ind,
                        T.emer_srvcs_prvdr_ind,
                        to_timestamp(cast(M.da_run_id as string), 'yyyyMMddHHmmss') as REC_ADD_TS,
                        current_timestamp() as REC_UPDT_TS
                from Prov02_Main_CNST M
                left join Prov07_Medicaid_Mapped E
                    on {self.write_equalkeys(self.srtlist, 'M', 'E')}
                left join Prov06_Taxonomies_Mapped T
                    on {self.write_equalkeys(self.srtlist, 'M', 'T')}
                order by M.tmsis_run_id, M.submtg_state_cd, M.submtg_state_prvdr_id
            """
        self.prv.append(type(self).__name__, z)

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def build(self, runner: PRV_Runner):

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_prv
                SELECT
                    *
                FROM
                    Prov02_Base
        """

        self.prv.append(type(self).__name__, z)

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_prv_enr
                SELECT
                    *
                FROM
                    Prov07_Medicaid_ENRPOP
        """

        self.prv.append(type(self).__name__, z)


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
