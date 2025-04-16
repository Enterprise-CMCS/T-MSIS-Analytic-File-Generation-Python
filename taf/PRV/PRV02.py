from taf.PRV import PRV_Runner
from taf.PRV.PRV import PRV
from taf.TAF_Closure import TAF_Closure

# from taf.PRV.PRV import PRV


class PRV02(PRV):
     
    def __init__(self, prv: PRV_Runner):
        super().__init__(prv)

    def process_02_main(self, runtbl, outtbl):
        """
        000-02 main segment
        """
         
        # %put NOTE: ****PROCESS_02_MAIN Start ******

        # screen out all but the latest run id
        runlist = ['tms_run_id', 'submitting_state']

        self.screen_runid('tmsis.Prov_Attributes_Main',
                          runtbl,
                          runlist,
                          'Prov02_Main_Latest1',
                          'M')

        # row count
        self.count_rows('Prov02_Main_Latest1',
                        'cnt_latest',
                        'PRV02_Latest')

        cols02 = ['tms_run_id',
                  'tms_reporting_period',
                  'submitting_state',
                  'submitting_state as submtg_state_cd',
                  'record_number',
                  '%upper_case(submitting_state_prov_id) as submitting_state_prov_id',
                  'prov_attributes_eff_date',
                  'prov_attributes_end_date',
                  'prov_doing_business_as_name',
                  'prov_legal_name',
                  'prov_organization_name',
                  'prov_tax_name',
                  'facility_group_individual_code',
                  'teaching_ind',
                  'prov_first_name',
                  'prov_middle_initial',
                  'prov_last_name',
                  'sex',
                  'ownership_code',
                  'prov_profit_status',
                  '%fix_old_dates(date_of_birth)',
                  '%fix_old_dates(date_of_death)',
                  'accepting_new_patients_ind']

        whr02 = 'upper(submitting_state_prov_id) is not null'

        self.copy_activerows('Prov02_Main_Latest1',
                             cols02,
                             whr02,
                             'Prov02_Main_Copy')
        # row count
        self.count_rows('Prov02_Main_Copy',
                        'cnt_active',
                        'PRV02_Active')

        # screen for eligibility during the month
        keylist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id']

        self.screen_dates('Prov02_Main_Copy',
                          keylist,
                          'prov_attributes_eff_date',
                          'prov_attributes_end_date',
                          'Prov02_Main_Latest2')

        # row count
        self.count_rows('Prov02_Main_Latest2',
                        'cnt_date',
                        'PRV02_Date')

        # remove duplicate records
        grplist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id']

        self.remove_duprecs('Prov02_Main_Latest2',
                            grplist,
                            'prov_attributes_eff_date',
                            'prov_attributes_end_date',
                            'facility_group_individual_code',
                            outtbl)

        # row count
        self.count_rows(outtbl,
                        'cnt_final',
                        'PRV02_Final')

    def create(self):
        """
        Create the PRV02 main segment.  
        """
         
        self.process_02_main('Prov01_Header', 'Prov02_Main')

        self.recode_notnull('Prov02_Main',
                             self.srtlist,
                            'prv_formats_sm',
                            'STFIPC',
                            'submitting_state',
                            'SUBMTG_STATE_rcd',
                            'Prov02_Main_STV',
                            'C',
                             2)

        self.recode_lookup('Prov02_Main_STV',
                            self.srtlist,
                           'prv_formats_sm',
                           'STFIPN',
                           'SUBMTG_STATE_rcd',
                           'State',
                           'Prov02_Main_ST',
                           'C',
                            7)

        self.recode_lookup('Prov02_Main_ST',
                            self.srtlist,
                           'prv_formats_sm',
                           'REGION',
                           'State',
                           'REG_FLAG',
                           'Prov02_Main_RG',
                           'N')

        self.recode_lookup('Prov02_Main_RG',
                            self.srtlist,
                           'prv_formats_sm',
                           'PROVCLSS',
                           'facility_group_individual_code',
                           'FAC_GRP_INDVDL_CD',
                           'Prov02_Main_PC',
                           'C',
                            2)

        self.recode_lookup('Prov02_Main_PC',
                            self.srtlist,
                           'prv_formats_sm',
                           'TEACHV',
                           'teaching_ind',
                           'TCHNG_IND',
                           'Prov02_Main_TF',
                           'C',
                            1)

        self.recode_lookup('Prov02_Main_TF',
                            self.srtlist,
                           'prv_formats_sm',
                           'OWNERV',
                           'ownership_code',
                           'OWNRSHP_CD',
                           'Prov02_Main_OV',
                           'C',
                            2)

        self.recode_lookup('Prov02_Main_OV',
                            self.srtlist,
                           'prv_formats_sm',
                           'OWNER',
                           'ownership_code',
                           'OWNRSHP_CAT',
                           'Prov02_Main_O',
                           'N')

        self.recode_lookup('Prov02_Main_O',
                            self.srtlist,
                           'prv_formats_sm',
                           'PROFV',
                           'prov_profit_status',
                           'PRVDR_PRFT_STUS_CD',
                           'Prov02_Main_PS',
                           'C',
                            2)

        self.recode_lookup('Prov02_Main_PS',
                            self.srtlist,
                           'prv_formats_sm',
                           'NEWPATV',
                           'accepting_new_patients_ind',
                           'ACPT_NEW_PTNTS_IND',
                           'Prov02_Main_NP',
                           'C',
                            1)

        # diststyle key distkey(submitting_state_prov_id)
        # compound sortkey (tms_run_id, submitting_state, submitting_state_prov_id) as
        z = f"""
            create or replace temporary view Prov02_Main_INDV as
            select
                *,
                case
                    when date_of_birth is null or date_of_birth > to_date('{self.prv.RPT_PRD}') then null
                    when date_of_death is not null and date_of_death <= to_date('{self.prv.RPT_PRD}') then floor((datediff(date_of_death, date_of_birth))/365.25)
                    else floor( (datediff(to_date('{self.prv.RPT_PRD}'), date_of_birth))/365.25 )
                    end as AGE_NUM,
                case
                    when date_of_death > to_date('{self.prv.RPT_PRD}') then null else date_of_death
                    end as DEATH_DT
            from
                Prov02_Main_NP
            where
                FAC_GRP_INDVDL_CD='03' or FAC_GRP_INDVDL_CD is null
            order by
                { ','.join(self.srtlist) }
            """
        self.prv.append(type(self).__name__, z)

        self.recode_lookup('Prov02_Main_INDV',
                           self.srtlist,
                           'prv_formats_sm',
                           'SEXV',
                           'sex',
                           'SEX_CD',
                           'Prov02_Main_GC',
                           'C',
                           1)

        var02nind = ['tms_run_id',
                     'tms_reporting_period',
                     'submitting_state',
                     'record_number',
                     'submitting_state_prov_id',
                     'prov_attributes_eff_date',
                     'prov_attributes_end_date',
                     'prov_doing_business_as_name',
                     'prov_legal_name',
                     'prov_organization_name',
                     'prov_tax_name',
                     'SUBMTG_STATE_CD',
                     'State',
                     'REG_FLAG',
                     'FAC_GRP_INDVDL_CD',
                     'TCHNG_IND',
                     'OWNRSHP_CD',
                     'OWNRSHP_CAT',
                     'PRVDR_PRFT_STUS_CD',
                     'ACPT_NEW_PTNTS_IND']

        var02ind = ['prov_first_name',
                    'prov_middle_initial',
                    'prov_last_name',
                    'SEX_CD',
                    'AGE_NUM',
                    'date_of_birth',
                    'DEATH_DT']

        # diststyle key distkey(submitting_state_prov_id)
        # compound sortkey (tms_run_id, submitting_state, submitting_state_prov_id) as
        z = f"""
                create or replace temporary view Prov02_Main_All as
                select { self.write_keyprefix(var02nind, 'R')}, { self.write_keyprefix(var02ind, 'T') }
                from Prov02_Main_NP R
                    left join Prov02_Main_GC T
                    on { self.write_equalkeys(self.srtlist,'R', 'T') }
                order by { self.write_keyprefix(self.srtlist, 'R') }
            """
        self.prv.append(type(self).__name__, z)

        # diststyle key distkey(submitting_state_prov_id) as
        z = f"""
                create or replace temporary view Prov02_Main_CNST as
                select { ','.join(self.srtlist) },
                    tms_run_id as TMSIS_RUN_ID,
                    cast({self.prv.TAF_FILE_DATE} as string) as PRV_FIL_DT,
                    '{self.prv.VERSION}' as PRV_VRSN,
                    {self.prv.DA_RUN_ID} as DA_RUN_ID,
                    SUBMTG_STATE_CD,
                    submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
                    REG_FLAG,
                    case
                      when nullif(trim(TRAILING FROM prov_doing_business_as_name),'')='888888888888888888888888888888888888888888888888888888888888888' then NULL
                      when nullif(trim(TRAILING FROM prov_doing_business_as_name),'')='99999999999999999999999999999999999999999999999999' then NULL
                      else { TAF_Closure.upper_case('prov_doing_business_as_name') } 
                    end as PRVDR_DBA_NAME,
                    { TAF_Closure.upper_case('prov_legal_name') } as PRVDR_LGL_NAME,
                    { TAF_Closure.upper_case('prov_organization_name') } as PRVDR_ORG_NAME,
                    { TAF_Closure.upper_case('prov_tax_name') } as PRVDR_TAX_NAME,
                    FAC_GRP_INDVDL_CD,
                    TCHNG_IND,
                    { TAF_Closure.upper_case('prov_first_name') } as PRVDR_1ST_NAME,
                    { TAF_Closure.upper_case('prov_middle_initial') } as PRVDR_MDL_INITL_NAME,
                    { TAF_Closure.upper_case('prov_last_name') } as PRVDR_LAST_NAME,
                    SEX_CD,
                    OWNRSHP_CD,
                    OWNRSHP_CAT,
                    PRVDR_PRFT_STUS_CD,
                    date_of_birth as BIRTH_DT,
                    DEATH_DT,
                    ACPT_NEW_PTNTS_IND,
                    case
                      when AGE_NUM < 15 then null
                      when AGE_NUM > 125 then 125
                      else AGE_NUM
                    end as AGE_NUM,
                    cast (('{self.prv.VERSION}' || '-' || { self.prv.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')) as varchar(50)) as PRV_LINK_KEY
                from Prov02_Main_All
                order by { ','.join(self.srtlist) }
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
