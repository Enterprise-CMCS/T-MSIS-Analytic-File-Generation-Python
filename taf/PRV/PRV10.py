from taf.PRV import PRV_Runner

from taf.PRV.PRV import PRV

class PRV10(PRV):

    def __init__(self, prv: PRV_Runner):
        super().__init__(prv)

    def process_10_beds(self, loctbl, outtbl):
        """
        000-10 bed type segment
        """

        # screen out all but the latest(selected) run id - provider id - location id
        runlist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_location_id']

        self.screen_runid('tmsis.Prov_Bed_Type_Info',
                          loctbl,
                          runlist,
                          'Prov10_BedType_Latest1',
                          'L')

        # row count
        # self.prv.countrows(Prov10_BedType_Latest1, cnt_latest, PRV10_Latest)

        cols10 = ['tms_run_id',
                  'tms_reporting_period',
                  'record_number',
                  'submitting_state',
                  'submitting_state as submtg_state_cd',
                  '%upper_case(submitting_state_prov_id) as submitting_state_prov_id',
                  '%upper_case(prov_location_id) as prov_location_id',
                  'bed_count',
                  """case
                      when trim(TRAILING FROM bed_type_code) in ('1','2','3','4') then trim(TRAILING FROM bed_type_code)
                      else null
                   end as bed_type_code""",
                  'bed_type_eff_date',
                  'bed_type_end_date']

        # copy 10(bed type) provider rows
        whr10 = "(trim(TRAILING FROM bed_type_code) in ('1','2','3','4')) or (bed_count is not null and bed_count<>0)"

        self.copy_activerows('Prov10_BedType_Latest1',
                             cols10,
                             whr10,
                             'Prov10_BedType_Copy')

        # row count
        # self.prv.countrows(Prov10_BedType_Copy, cnt_active, PRV10_Active)

        # screen for licensing during the month
        keylist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_location_id',
                   'bed_type_code']

        self.screen_dates('Prov10_BedType_Copy',
                          keylist,
                          'bed_type_eff_date',
                          'bed_type_end_date',
                          'Prov10_BedType_Latest2')

        # row count
        # self.prv.countrows(Prov10_BedType_Latest2, cnt_date, PRV10_Date)

        # remove duplicate records
        grplist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_location_id',
                   'bed_type_code']

        self.remove_duprecs('Prov10_BedType_Latest2',
                            grplist,
                            'bed_type_eff_date',
                            'bed_type_end_date',
                            'bed_type_code',
                            outtbl)

    def create(self):
        """
        Create the PRV10 bed type segment.
        """

        # row count
        # self.prv.countrows(&outtbl, cnt_final, PRV10_Final)

        self.process_10_beds('Prov03_Locations_g0',
                             'Prov10_BedType')

        self.recode_lookup('Prov10_BedType',
                           self.srtlistl,
                           'prv_formats_sm',
                           'BEDCDV',
                           'bed_type_code',
                           'BED_TYPE_CD',
                           'Prov10_BedType_TYP',
                           'C',
                           1)

        z = f"""
            create or replace temporary view Prov10_BedType_CNST as
                select {self.prv.DA_RUN_ID} as DA_RUN_ID,
                    cast (('{self.prv.VERSION}' || '-' || { self.prv.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**')) as varchar(74)) as PRV_LOC_LINK_KEY,
                    '{self.prv.TAF_FILE_DATE}' as PRV_FIL_DT,
                    '{self.prv.VERSION}' as PRV_VRSN,
                    tms_run_id as TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
                    prov_location_id as PRVDR_LCTN_ID,
                    BED_TYPE_CD,
                    bed_count as BED_CNT,
                    from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS,
                    cast(NULL as timestamp) as REC_UPDT_TS
                from Prov10_BedType_TYP
                where BED_TYPE_CD is not null or (bed_count is not null and bed_count<>0)
                order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID, PRVDR_LCTN_ID
            """
        self.prv.append(type(self).__name__, z)


        z = f"""
            create or replace temporary view loc__g as
                select
                    DA_RUN_ID,
                    PRV_LOC_LINK_KEY,
                    PRV_FIL_DT,
                    PRV_VRSN,
                    TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    SUBMTG_STATE_PRVDR_ID,
                    PRVDR_LCTN_ID
                from
                    (select
                        DA_RUN_ID,
                        PRV_LOC_LINK_KEY,
                        PRV_FIL_DT,
                        PRV_VRSN,
                        TMSIS_RUN_ID,
                        SUBMTG_STATE_CD,
                        SUBMTG_STATE_PRVDR_ID,
                        PRVDR_LCTN_ID
                    from
                        Prov04_Licensing_CNST
                    where
                        PRVDR_LCTN_ID='000'

				    union all

				    select
                        DA_RUN_ID,
                        PRV_LOC_LINK_KEY,
                        PRV_FIL_DT,
                        PRV_VRSN,
                        TMSIS_RUN_ID,
                        SUBMTG_STATE_CD,
                        SUBMTG_STATE_PRVDR_ID,
                        PRVDR_LCTN_ID
                    from
                        Prov05_Identifiers_CNST
                    where
                        PRVDR_LCTN_ID='000'

				    union all

				    select
                        DA_RUN_ID,
                        PRV_LOC_LINK_KEY,
                        PRV_FIL_DT,
                        PRV_VRSN,
                        TMSIS_RUN_ID,
                        SUBMTG_STATE_CD,
                        SUBMTG_STATE_PRVDR_ID,
                        PRVDR_LCTN_ID
                    from
                        Prov10_BedType_CNST
                    where
                        PRVDR_LCTN_ID='000'

				    except

				    select
                        DA_RUN_ID,
                        PRV_LOC_LINK_KEY,
                        PRV_FIL_DT,
                        PRV_VRSN,
                        TMSIS_RUN_ID,
                        SUBMTG_STATE_CD,
                        SUBMTG_STATE_PRVDR_ID,
                        PRVDR_LCTN_ID
                    from
                        Prov03_Location_CNST
                    where
                        PRVDR_LCTN_ID='000')

                group by
                    DA_RUN_ID,
                    PRV_LOC_LINK_KEY,
                    PRV_FIL_DT,
                    PRV_VRSN,
                    TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    SUBMTG_STATE_PRVDR_ID,
                    PRVDR_LCTN_ID

                order by
                    DA_RUN_ID,
                    PRV_LOC_LINK_KEY,
                    PRV_FIL_DT,
                    PRV_VRSN,
                    TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    SUBMTG_STATE_PRVDR_ID,
                    PRVDR_LCTN_ID
            """
        self.prv.append(type(self).__name__, z)


# added: REC_ADD_TS
# added: REC_UPDT_TS

        z = f"""
                create or replace temporary view loc__g2 as
                select
                    DA_RUN_ID,
                    cast (('{self.prv.VERSION}' || '-' || { self.prv.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(SUBMTG_STATE_PRVDR_ID, '*')) as varchar(50)) as PRV_LINK_KEY,
                    PRV_LOC_LINK_KEY,
                    PRV_FIL_DT,
                    PRV_VRSN,
                    TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    SUBMTG_STATE_PRVDR_ID,
                    PRVDR_LCTN_ID,
                    NULL AS PRVDR_ADR_BLG_IND,
                    NULL AS PRVDR_ADR_PRCTC_IND,
                    NULL AS PRVDR_ADR_SRVC_IND,
                    NULL AS ADR_LINE_1_TXT,
                    NULL AS ADR_LINE_2_TXT,
                    NULL AS ADR_LINE_3_TXT,
                    NULL AS ADR_CITY_NAME,
                    NULL AS ADR_STATE_CD,
                    NULL AS ADR_ZIP_CD,
                    NULL AS ADR_CNTY_CD,
                    NULL AS ADR_BRDR_STATE_IND,
                    NULL AS PRVDR_SRVC_ST_DFRNT_SUBMTG_ST,
                    from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS,
                    cast(NULL as timestamp) as REC_UPDT_TS
                from
                    loc__g
        """
        self.prv.append(type(self).__name__, z)

    def build(self, runner: PRV_Runner):
        """
        Build the PRV10 bed type segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_prv_bed
                SELECT
                    *
                FROM
                    Prov10_BedType_CNST
        """
        self.prv.append(type(self).__name__, z)

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_prv_loc
                SELECT
                    *
                FROM
                    loc__g2
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
