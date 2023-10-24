from taf.PRV import PRV_Runner

from taf.PRV.PRV import PRV


class PRV05(PRV):

    def __init__(self, prv: PRV_Runner):
        super().__init__(prv)

    def process_05_identifiers(self, loctbl, outtbl):
        """
        000-05 identifiers segment
        """

        # screen out all but the latest(selected) run id - provider id - location id
        runlist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_location_id']

        self.screen_runid('tmsis.Prov_Identifiers',
                          loctbl,
                          runlist,
                          'Prov05_Identifiers_Latest1',
                          'L')

        # row count
        # self.prv.countrows(Prov05_Identifiers_Latest1, cnt_latest, PRV05_Latest)

        cols05 = ['tms_run_id',
                  'tms_reporting_period',
                  'record_number',
                  'submitting_state',
                  'submitting_state as submtg_state_cd',
                  '%upper_case(submitting_state_prov_id) as submitting_state_prov_id',
                  '%upper_case(prov_location_id) as prov_location_id',
                  '%upper_case(prov_identifier) as prov_identifier',
                  'prov_identifier_type',
                  '%upper_case(prov_identifier_issuing_entity_id) as prov_identifier_issuing_entity_id',
                  'prov_identifier_eff_date',
                  'prov_identifier_end_date']

        # copy 05(identifiers) provider rows
        whr05 = 'prov_identifier_type is not null and upper(prov_identifier) is not null'

        self.copy_activerows('Prov05_Identifiers_Latest1',
                             cols05,
                             whr05,
                             'Prov05_Identifiers_Copy')

        # row count
        # self.prv.countrows(Prov05_Identifiers_Copy, cnt_active, PRV05_Active)

        # screen for Identifiers during the month
        keylist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_location_id',
                   'prov_identifier_type',
                   'prov_identifier',
                   'prov_identifier_issuing_entity_id']

        self.screen_dates('Prov05_Identifiers_Copy',
                          keylist,
                          'prov_identifier_eff_date',
                          'prov_identifier_end_date',
                          'Prov05_Identifiers_Latest2')

        # row count
        # self.prv.countrows(Prov05_Identifiers_Latest2, cnt_date, PRV05_Date)

        # remove duplicate records
        grplist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_location_id',
                   'prov_identifier_type',
                   'prov_identifier',
                   'prov_identifier_issuing_entity_id']

        self.remove_duprecs('Prov05_Identifiers_Latest2',
                            grplist,
                            'prov_identifier_eff_date',
                            'prov_identifier_end_date',
                            'prov_identifier_type',
                            outtbl)

        # row count
        # self.prv.countrows(&outtbl, cnt_final, PRV05_Final)

    def create(self):
        """
        Create the PRV05 identifiers segment.
        """

        self.process_05_identifiers('Prov03_Locations_g0',
                                    'Prov05_Identifiers')

        self.recode_lookup('Prov05_Identifiers',
                           self.srtlistl,
                           'prv_formats_sm',
                           'IDCDV',
                           'prov_identifier_type',
                           'PRVDR_ID_TYPE_CD',
                           'Prov05_Identifiers_TYP',
                           'C',
                           1)

        z = f"""
            create or replace temporary view Prov05_Identifiers_CNST as
            select {self.prv.DA_RUN_ID} as DA_RUN_ID,
                    cast (('{self.prv.VERSION}' || '-' || { self.prv.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*') || '-' || coalesce(prov_location_id, '**')) as varchar(74)) as PRV_LOC_LINK_KEY,
                    '{self.prv.TAF_FILE_DATE}' as PRV_FIL_DT,
                    '{self.prv.VERSION}' as PRV_VRSN,
                    tms_run_id as TMSIS_RUN_ID,
                    SUBMTG_STATE_CD,
                    submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
                    prov_location_id as PRVDR_LCTN_ID,
                    PRVDR_ID_TYPE_CD,
                    substr(prov_identifier from 1 for 12) as PRVDR_ID,
                    prov_identifier_issuing_entity_id as PRVDR_ID_ISSG_ENT_ID_TXT,
                    from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS,
                    cast(NULL as timestamp) as REC_UPDT_TS
                    from Prov05_Identifiers_TYP
                    where PRVDR_ID_TYPE_CD is not null
            order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID, PRVDR_LCTN_ID
            """
        self.prv.append(type(self).__name__, z)

    def build(self, runner: PRV_Runner):
        """
        Build the PRV05 identifiers segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_prv_idt
                SELECT
                    *
                FROM
                    Prov05_Identifiers_CNST
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
