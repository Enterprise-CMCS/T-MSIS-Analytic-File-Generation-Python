from taf.PRV import PRV_Runner

from taf.PRV.PRV import PRV


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class PRV08(PRV):

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
    def process_08_groups(self, maintbl, outtbl):

        # screen out all but the latest(selected) run id - provider id
        runlist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id']

        self.screen_runid('tmsis.Prov_Affiliated_Groups',
                          maintbl,
                          runlist,
                          'Prov08_AffGrps_Latest1')

        # row count
        # self.prv.countrows(Prov08_AffGrps_Latest1, cnt_latest, PRV08_Latest)

        cols08 = ['tms_run_id',
                  'tms_reporting_period',
                  'record_number',
                  'submitting_state',
                  '%upper_case(submitting_state_prov_id) as submitting_state_prov_id',
                  '%upper_case(submitting_state_prov_id_of_affiliated_entity) as submitting_state_prov_id_of_affiliated_entity',
                  'prov_affiliated_group_eff_date',
                  'prov_affiliated_group_end_date']

        # copy 08(Affiliated Groups) provider rows
        whr08 = 'submitting_state_prov_id_of_affiliated_entity is not null'

        self.copy_activerows('Prov08_AffGrps_Latest1',
                             cols08,
                             whr08,
                             'Prov08_AffGrps_Copy')

        # row count
        # self.prv.countrows(Prov08_AffGrps_Copy, cnt_active, PRV08_Active)

        # screen for licensing during the month
        keylist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id']

        self.screen_dates('Prov08_AffGrps_Copy',
                          keylist,
                          'prov_affiliated_group_eff_date',
                          'prov_affiliated_group_end_date',
                          'Prov08_AffGrps_Latest2')

        # row count
        # self.prv.countrows(Prov08_AffGrps_Latest2, cnt_date, PRV08_Date)

        # remove duplicate records
        grplist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'submitting_state_prov_id_of_affiliated_entity']

        self.remove_duprecs('Prov08_AffGrps_Latest2',
                            grplist,
                            'prov_affiliated_group_eff_date',
                            'prov_affiliated_group_end_date',
                            'submitting_state_prov_id_of_affiliated_entity',
                            outtbl)

        # row count
        # self.prv.countrows(&outtbl, cnt_final, PRV08_Final)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):

        self.process_08_groups('Prov02_Main',
                               'Prov08_Groups')

        # add code to validate and recode source variables (when needed), use SAS variable names, add linking variables, and sort records

        self.recode_notnull('Prov08_Groups',
                            self.srtlist,
                            'prv_formats_sm',
                            'STFIPC',
                            'submitting_state',
                            'SUBMTG_STATE_CD',
                            'Prov08_Groups_STV',
                            'C',
                            2)

        z = f"""
            create or replace temporary view Prov08_Groups_CNST as
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
                    submitting_state_prov_id_of_affiliated_entity as SUBMTG_STATE_AFLTD_PRVDR_ID,
                    to_timestamp('{self.prv.DA_RUN_ID}', 'yyyyMMddHHmmss') as REC_ADD_TS,
                    current_timestamp() as REC_UPDT_TS
                    from Prov08_Groups_STV
            order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID
            """
        self.prv.append(type(self).__name__, z)

    # -----------------------------------------------------------------------------
    #
    #
    #
    # -----------------------------------------------------------------------------
    def build(self, runner: PRV_Runner):

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_prv_grp
                SELECT
                    *
                FROM
                    Prov08_Groups_CNST
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
