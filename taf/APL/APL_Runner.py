from taf.TAF_Runner import TAF_Runner


class APL_Runner(TAF_Runner):
    """
    The TAF-specific module contains executable statements as well as function definitions to
    generate and execute SQL to produce individual segment as well as final output.
    These statements are intended to initialize the module.
    """

    def __init__(self,
                 da_schema: str,
                 reporting_period: str,
                 state_code: str,
                 run_id: str,
                 job_id: int,
                 file_version: str,
                 run_stats_only: int = 0):
        super().__init__(da_schema,
                         reporting_period,
                         state_code,
                         run_id,
                         job_id,
                         file_version,
                         run_stats_only)

        self.run_stats_only = self.__forceBool__(run_stats_only)
        self.monyrout = self.reporting_period.strftime("%Y%m").upper()

    def ST_FILTER(self):
        """
        Use the trim function to remove extraneous space characters from start and end of state names.
        """

        return "and trim(submitting_state) not in ('94','96')"

    def init(self):
        """
        Import, create, and build out each segment for a given file type.
        At this point, a dictionary has been created for each file segment containing
        SQL queries that will be sequential executed by the run definition to produce output.
        """

        from taf.APL.BASE import BASE
        from taf.APL.LCTN import LCTN
        from taf.APL.SAREA import SAREA
        from taf.APL.OA import OA
        from taf.APL.ENRLMT import ENRLMT

        self.determine_max_run_ids()

        LCTN(self).create()
        SAREA(self).create()
        ENRLMT(self).create()
        OA(self).create()
        BASE(self).create()

        LCTN(self).build()
        SAREA(self).build()
        ENRLMT(self).build()
        OA(self).build()
        BASE(self).build()

    def determine_max_run_ids(self):
        self.max_run_id("MCP", "", self.reporting_period.year)

    def max_run_id(self, file: str, tbl: str, inyear):

        """Function max_run_id to get the highest da_run_id for the given state for input monthly TAF. This
        table will then be merged back to the monthly TAF to pull all records for that state, month, and da_run_id.
        It is also inserted into the metadata table to keep a record of the state/month DA_RUN_IDs that make up 
        each annual run.  To get the max run ID, must go to the job control table and get the latest national run, and then also
        get the latest state-specific run. Determine the later by state and month and then pull those IDs.

        Function parms:
   	    inyear=input year, set to the current year
        """
        
        filel = file.lower()
        if tbl == "":
            tbl = f"taf_{filel}"

        # For NON state-specific runs (where job_parms_text does not include submtg_state_cd in),
        #    pull highest da_run_id by time ;

        z = f"""
                CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear}_nat AS
                SELECT {file}_fil_dt
                    ,max(da_run_id) AS da_run_id
                FROM (
                    SELECT substring(job_parms_txt, 1, 4) || substring(job_parms_txt, 6, 2) AS {file}_fil_dt
                        ,da_run_id
                    FROM {self.DA_SCHEMA}.job_cntl_parms
                    WHERE upper(fil_type) = '{file}'
                        AND sucsfl_ind = 1
                        AND substring(job_parms_txt, 1, 4) = '{inyear}'
                        AND charindex('submtg_state_cd in', regexp_replace(job_parms_txt, '\\\s+', ' ')) = 0
                    )
                GROUP BY {file}_fil_dt
        """
        self.prepend(z)

        # For state-specific runs (where job_parms_text includes submtg_state_cd in),
        #    pull highest da_run_id by time and state;

        z = f"""
                CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear}_ss AS
                SELECT {file}_fil_dt
                    ,submtg_state_cd
                    ,max(da_run_id) AS da_run_id
                FROM (
                    SELECT substring(job_parms_txt, 1, 4) || substring(job_parms_txt, 6, 2) AS {file}_fil_dt
                        ,regexp_extract(substring(job_parms_txt, 10), '([0-9]{{2}})') AS submtg_state_cd
                        ,da_run_id
                    FROM {self.DA_SCHEMA}.job_cntl_parms
                    WHERE upper(fil_type) = '{file}'
                        AND sucsfl_ind = 1
                        AND substring(job_parms_txt, 1, 4) = '{inyear}'
                        AND charindex('submtg_state_cd in', regexp_replace(job_parms_txt, '\\\s+', ' ')) > 0
                    )
                GROUP BY {file}_fil_dt
                    ,submtg_state_cd
        """
        self.prepend(z)

        # Now join the national and state lists by month - take the national run ID if higher than
        #    the state-specific, otherwise take the state-specific.
        #    Must ALSO stack with the national IDs so they are not lost.
        #    In outer query, get a list of unique IDs to pull;

        z = f"""
                CREATE OR REPLACE TEMPORARY VIEW job_cntl_parms_both_{file}_{inyear} AS
                SELECT DISTINCT {file}_fil_dt
                    ,da_run_id
                FROM (
                    SELECT coalesce(a.{file}_fil_dt, b.{file}_fil_dt) AS {file}_fil_dt
                        ,CASE
                            WHEN a.da_run_id > b.da_run_id
                                OR b.da_run_id IS NULL
                                THEN a.da_run_id
                            ELSE b.da_run_id
                            END AS da_run_id
                    FROM max_run_id_{file}_{inyear}_nat a
                    FULL JOIN max_run_id_{file}_{inyear}_ss b ON a.{file}_fil_dt = b.{file}_fil_dt

                    UNION ALL

                    SELECT {file}_fil_dt
                        ,da_run_id
                    FROM max_run_id_{file}_{inyear}_nat
                    )
        """
        self.prepend(z)

        # Now join to EFTS data to get table of month/state/run IDs to use for data pull.
        #    Note must then take the highest da_run_id by state/month (if any state-specific runs
        #    were identified as being later than a national run):

        z = f"""
                CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear} AS
                SELECT a.{file}_fil_dt
                    ,b.submtg_state_cd
                    ,max(b.da_run_id) AS da_run_id
                    ,max(b.fil_cret_dt) AS fil_cret_dt
                FROM job_cntl_parms_both_{file}_{inyear} a
                INNER JOIN (
                    SELECT da_run_id
                        ,incldd_state_cd AS submtg_state_cd
                        ,fil_cret_dt
                    FROM {self.DA_SCHEMA}.efts_fil_meta
                    WHERE incldd_state_cd != 'Missing'
                        AND otpt_name = 'TAF_{file}'
            """

        if str(self.ST_FILTER).find("ALL") != -1:
            z += f"""WHERE {self.ST_FILTER}"""

        z += f"""
                    ) b ON a.da_run_id = b.da_run_id
                GROUP BY a.{file}_fil_dt
                    ,b.submtg_state_cd
        """
        self.prepend(z)

        #    Insert into metadata table so we keep track of all monthly DA_RUN_IDs
        #    that go into each annual {fil_typ} file

        z = f"""
            insert into {self.DA_SCHEMA}.TAF_ANN_INP_SRC
            select
                {self.DA_RUN_ID} as ann_da_run_id,
                'apl' as ann_fil_type,
                submtg_state_cd,
                '{file.lower()}' as src_fil_type,
                {file}_fil_dt as src_fil_dt,
                da_run_id as src_da_run_id,
                date_format(date(fil_cret_dt),'MM/dd/yyyy') as src_fil_creat_dt,
                from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS,
                null as REC_UPDT_TS

            from max_run_id_{file}_{inyear}
        """
        self.prepend(z)

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
