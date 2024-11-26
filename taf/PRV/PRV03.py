from taf.PRV import PRV_Runner

from taf.PRV.PRV import PRV


class PRV03(PRV):

    def __init__(self, prv: PRV_Runner):
        super().__init__(prv)

    def process_03_locations(self, maintbl, outtbl):
        """
        000-03 location data extract
        """

        # screen out all but the latest(selected) run id - provider id
        runlist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id']

        self.screen_runid('tmsis.Prov_Location_And_Contact_Info',
                          maintbl,
                          runlist,
                          'Prov03_Locations_Latest1')

        # row count
        # self.prv.countrows(Prov03_Locations_Latest1, cnt_latest, PRV03_Latest)

        cols03 = ['tms_run_id',
                  'tms_reporting_period',
                  'record_number',
                  'submitting_state',
                  'submitting_state as submtg_state_cd',
                  '%upper_case(submitting_state_prov_id) as submitting_state_prov_id',
                  '%upper_case(prov_location_id) as prov_location_id',
                  'prov_addr_type',
                  'prov_location_and_contact_info_eff_date',
                  'prov_location_and_contact_info_end_date',
                  'addr_ln1',
                  'addr_ln2',
                  'addr_ln3',
                  'addr_city',
                  '%upper_case(addr_state) as addr_state',
                  'addr_zip_code',
                  'addr_county',
                  'addr_border_state_ind']

        # copy 03(Location) provider rows
        whr03 = 'prov_addr_type=1 or prov_addr_type=3 or prov_addr_type=4'

        self.copy_activerows('Prov03_Locations_Latest1',
                             cols03,
                             whr03,
                             'Prov03_Locations_Copy')

        # row count
        # self.prv.countrows(Prov03_Locations_Copy, cnt_active, PRV03_Active)

        # screen for locations during the month
        keylist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_addr_type',
                   'prov_location_id']

        self.screen_dates('Prov03_Locations_Copy',
                          keylist,
                          'prov_location_and_contact_info_eff_date',
                          'prov_location_and_contact_info_end_date',
                          'Prov03_Locations_Latest2')

        # row count
        # self.prv.countrows(Prov03_Locations_Latest2, cnt_date, PRV03_Date)

        # remove duplicate records
        grplist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_addr_type',
                   'prov_location_id']

        self.remove_duprecs('Prov03_Locations_Latest2',
                            grplist,
                            'prov_location_and_contact_info_eff_date',
                            'prov_location_and_contact_info_end_date',
                            'prov_location_id',
                            outtbl)

        # row count
        # self.prv.countrows(&outtbl, cnt_final PRV03_Final)

        z = f"""
            create or replace temporary view loc_g as
            select
                M.tms_run_id,
                M.submitting_state,
                M.submitting_state_prov_id,
                '000' as prov_location_id
            from
                {maintbl} M

            left join
                (select
                    tms_run_id,
                    submitting_state,
                    submitting_state_prov_id,
                    prov_location_id
                from
                    TMSIS.Prov_Licensing_Info
                where
                    prov_location_id='000') L
            on
                M.tms_run_id=L.tms_run_id and
                M.submitting_state=L.submitting_state and
                M.submitting_state_prov_id=upper(L.submitting_state_prov_id)

             left join
                (select
                    tms_run_id,
                    submitting_state,
                    submitting_state_prov_id,
                    prov_location_id
                from
                    TMSIS.Prov_Identifiers
                where
                    prov_location_id='000') I
			 on
                M.tms_run_id=I.tms_run_id and
                M.submitting_state=I.submitting_state and
                M.submitting_state_prov_id=upper(I.submitting_state_prov_id)

             left join
                (select
                    tms_run_id,
                    submitting_state,
                    submitting_state_prov_id,
                    prov_location_id
                from
                    TMSIS.Prov_Bed_Type_Info
                where
                    prov_location_id='000') B
			 on
                M.tms_run_id=B.tms_run_id and
                M.submitting_state=B.submitting_state and
                M.submitting_state_prov_id=upper(B.submitting_state_prov_id)

            where
                L.prov_location_id='000' or
                I.prov_location_id='000' or
                B.prov_location_id='000'
            group by
                M.tms_run_id,
                M.submitting_state,
                M.submitting_state_prov_id
            order by
                M.tms_run_id,
                M.submitting_state,
                M.submitting_state_prov_id
        """
        self.prv.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view Prov03_Locations_g0 as
            select
                tms_run_id,
                submitting_state,
                submitting_state_prov_id,
                prov_location_id
            from
                (select
                    tms_run_id,
                    submitting_state,
                    submitting_state_prov_id,
                    prov_location_id
                from
                    {outtbl}

			    union all

			    select
                    *
                from
                    loc_g)
            group by
                tms_run_id,
                submitting_state,
                submitting_state_prov_id,
                prov_location_id
            order by
                tms_run_id,
                submitting_state,
                submitting_state_prov_id,
                prov_location_id
        """
        self.prv.append(type(self).__name__, z)

    def create(self):
        """
        Create the PRV03 location data extract
        """

        keyl = 'PRV_LOC_LINK_KEY'

        self.process_03_locations('Prov02_Main',
                                  'Prov03_Locations')

        srtlistl = ['tms_run_id',
                    'submitting_state',
                    'submitting_state_prov_id',
                    'prov_location_id']

        self.recode_notnull('Prov03_Locations',
                            srtlistl,
                            'prv_formats_sm',
                            'STFIPC',
                            'submitting_state',
                            'SUBMTG_STATE_rcd',
                            'Prov03_Locations_STV',
                            'C',
                            2)

        z = f"""
                create or replace temporary view Prov03_Locations_link as
                select *,
                        cast (('{self.prv.VERSION}' || '-' || { self.prv.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**')) as varchar(74)) as PRV_LOC_LINK_KEY
                from Prov03_Locations_STV
                order by { ','.join(self.srtlistl) }
            """
        self.prv.append(type(self).__name__, z)

        self.recode_notnull('Prov03_Locations_link',
                            srtlistl,
                            'prv_formats_sm',
                            'STCDN',
                            'addr_state',
                            'addr_state2',
                            'Prov03_Locations_AST1',
                            'C',
                            2)

        self.recode_lookup('Prov03_Locations_AST1',
                           srtlistl,
                           'prv_formats_sm',
                           'STFIPV',
                           'addr_state2',
                           'ADR_STATE_CD',
                           'Prov03_Locations_AST',
                           'C',
                           2)

        self.recode_lookup('Prov03_Locations_AST',
                           srtlistl,
                           'prv_formats_sm',
                           'BRDRSTV',
                           'addr_border_state_ind',
                           'ADR_BRDR_STATE_IND',
                           'Prov03_Locations_PRE_ZIP',
                           'C',
                           1)
        
        # apply regex validation to zip code; retain only all numeric values
        z = f"""
        
            -- applying regex filter to retain all numeric zip code values only
            create or replace temporary view Prov03_Locations_BS as
            select *, 
                    case 
                      when ADDR_ZIP_CODE regexp '[^.0-9a-zA-Z _-]' then null
                      else ADDR_ZIP_CODE
                    end as ADDR_ZIP_CODE_CLEAN
            from Prov03_Locations_PRE_ZIP
            
        """
        self.prv.append(type(self).__name__, z)

        # create separate variables for provider address type values
        z = f"""
            create or replace temporary view Prov03_Location_TYPE as
            select *,
                    case
                    when prov_addr_type='1' then 1
                    when prov_addr_type='3' or prov_addr_type='4' then 0
                    else null
                    end as PRVDR_ADR_BLG_IND,
                    case
                    when prov_addr_type='3' then 1
                    when prov_addr_type='1' or prov_addr_type='4' then 0
                    else null
                    end as PRVDR_ADR_PRCTC_IND,
                    case
                    when prov_addr_type='4' then 1
                    when prov_addr_type='3' or prov_addr_type='1' then 0
                    else null
                    end as PRVDR_ADR_SRVC_IND
                    from Prov03_Locations_BS
            order by {keyl}
        """
        self.prv.append(type(self).__name__, z)

        # group by location ID
        z = f"""
            create or replace temporary view Prov03_Location_mapt as
            select { ','.join(self.srtlistl) }, {keyl},
                    max(PRVDR_ADR_BLG_IND) as PRVDR_ADR_BLG_IND,
                    max(PRVDR_ADR_PRCTC_IND) as PRVDR_ADR_PRCTC_IND,
                    max(PRVDR_ADR_SRVC_IND) as PRVDR_ADR_SRVC_IND
            from Prov03_Location_TYPE
            group by { ','.join(self.srtlistl) }, {keyl}
        """
        self.prv.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view Prov03_Location_BSM as
            select { ','.join(self.srtlistl) },
                        tms_reporting_period,
                        record_number,
                        prov_addr_type,
                        prov_location_and_contact_info_eff_date,
                        prov_location_and_contact_info_end_date,
                        addr_ln1,
                        addr_ln2,
                        addr_ln3,
                        addr_city,
                        addr_state,
                        ADDR_ZIP_CODE_CLEAN,
                        addr_county,
                        addr_border_state_ind,
                        SUBMTG_STATE_CD,
                        SUBMTG_STATE_rcd,
                        ADR_STATE_CD,
                        ADR_BRDR_STATE_IND,
                        PRV_LOC_LINK_KEY
                    from Prov03_Locations_BS
            order by {keyl}
        """
        self.prv.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view Prov03_Location_CNST as
            select {self.prv.DA_RUN_ID} as DA_RUN_ID,
                    cast (('{self.prv.VERSION}' || '-' || { self.prv.monyrout } || '-' || T.SUBMTG_STATE_CD || '-' || coalesce(T.submitting_state_prov_id, '*')) as varchar(50)) as PRV_LINK_KEY,
                    T.PRV_LOC_LINK_KEY,
                    '{self.prv.TAF_FILE_DATE}' as PRV_FIL_DT,
                    '{self.prv.VERSION}' as PRV_VRSN,
                    T.tms_run_id as TMSIS_RUN_ID,
                    T.SUBMTG_STATE_CD,
                    T.submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
                    T.prov_location_id as PRVDR_LCTN_ID,
                    L.PRVDR_ADR_BLG_IND,
                    L.PRVDR_ADR_PRCTC_IND,
                    L.PRVDR_ADR_SRVC_IND,
                    upper(T.addr_ln1) as ADR_LINE_1_TXT,
                    upper(T.addr_ln2) as ADR_LINE_2_TXT,
                    upper(T.addr_ln3) as ADR_LINE_3_TXT,
                    upper(T.addr_city) as ADR_CITY_NAME,
                    T.ADR_STATE_CD,
                    T.ADDR_ZIP_CODE_CLEAN as ADR_ZIP_CD,
                    T.addr_county as ADR_CNTY_CD,
                    T.ADR_BRDR_STATE_IND,
                    case
                    when L.PRVDR_ADR_SRVC_IND=1 and T.SUBMTG_STATE_rcd=T.ADR_STATE_CD then 0
                    when L.PRVDR_ADR_SRVC_IND=1 and T.SUBMTG_STATE_rcd<>T.ADR_STATE_CD then 1
                    else null
                    end as PRVDR_SRVC_ST_DFRNT_SUBMTG_ST,
                    from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS,
                    cast(NULL as timestamp) as REC_UPDT_TS
            from Prov03_Location_BSM T
                left join Prov03_Location_mapt L
                    on T.{keyl}=L.{keyl}
            where T.prov_addr_type='4' or (T.prov_addr_type='3' and (L.PRVDR_ADR_SRVC_IND is null or L.PRVDR_ADR_SRVC_IND=0)) or (T.prov_addr_type='1' and (L.PRVDR_ADR_SRVC_IND is null or L.PRVDR_ADR_SRVC_IND=0) and (L.PRVDR_ADR_PRCTC_IND is null or L.PRVDR_ADR_PRCTC_IND=0))
            order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID, PRVDR_LCTN_ID
        """
        self.prv.append(type(self).__name__, z)

    def build(self, runner: PRV_Runner):
        """
        Build the PRV03 location data extract.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_prv_loc
                SELECT
                    *
                FROM
                    Prov03_Location_CNST
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
