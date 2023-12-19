import logging


class TAF_Run_Stack:
    """
    A class to represent a T-MSIS analytic file run_ids.
    """

    def __init__(self,
                 state_cds: list,
                 file_type: str,
                 reporting_period: str,
                 ignore_errors: int = 0):
        """
        Constructs all the necessary attributes for the T-MSIS analytic file
        state run-id dictionary.

            Parameters:
                state_cds: list,
                file_type: str,
                reporting_period: str,
                ignore_errors: int = 0

            Returns:
                self
        """

        self.logger = logging.getLogger('taf_log')
        self.logger.setLevel(logging.DEBUG)

        ch = logging.StreamHandler()

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        if (self.logger.hasHandlers()):
            self.logger.handlers.clear()

        self.logger.addHandler(ch)

        if state_cds is None:
            self.__input_state_cds = ["01", "02", "04", "05", "06", "08", "09",
                                      "10", "11", "12", "13", "15", "16", "17",
                                      "18", "19", "20", "21", "22", "23", "24",
                                      "25", "26", "27", "28", "29", "30", "31",
                                      "32", "33", "34", "35", "36", "37", "38",
                                      "39", "40", "41", "42", "44", "45", "46",
                                      "47", "48", "49", "50", "51", "53", "54",
                                      "55", "56", "72", "78", "97"]
        else:
            self.__input_state_cds = state_cds

        self.state_cds = []
        self.da_run_id = []
        self.file_type = file_type
        self.reporting_period = reporting_period
        self.__taf_stack__ = None

        self.__da_run_id_query__ = f"""
            select
                max(tms_run_id) as tmsis_run_id,
                submitting_state
                from
                (
                    select
                        distinct submitting_state,
                        max(taf.tms_run_id) as tms_run_id
                    from
                        uat_val_catalog.tmsis.{self.file_type_tables.get(self.file_type)} taf
                    where
                        taf.tms_reporting_period = "{self.reporting_period}"
                        and taf.submitting_state in ("{'","'.join(self.__input_state_cds)}")
                    group by
                        taf.submitting_state,
                        taf.tms_run_id
                    order by
                        taf.submitting_state,
                        tms_run_id
                )
                group by
                    submitting_state
                order by
                    submitting_state
        """
        self.init_stack()

    # -------------------------------------------------
    # This dictionary is to reference which file header
    # we should reference for run_ids
    # -------------------------------------------------
    file_type_tables = {
            "BSF": "file_header_record_eligibility",
            "IP" : "file_header_record_ip",
            "LT" : "file_header_record_lt",
            "MCP": "file_header_record_lt",
            "OT" : "file_header_record_ot",
            "PRV": "file_header_record_provider",
            "RX" : "file_header_record_rx",
            "TPL": "file_header_record_tpl"
        }

    # ---------------------------------------------------------------
    # This function inits the run_id stack and populates
    # the dictionary on the instance with the state_codes and run_ids
    # ---------------------------------------------------------------
    def init_stack(self):
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        self.stack_df = spark.sql(self.__da_run_id_query__)
        self.logger.debug(self.__da_run_id_query__)
        run_stack: dict = self.stack_df.pandas_api().to_dict()
        self.__taf_stack__ = run_stack
        # error check here for skipped states

        tmsis_run_id = run_stack.get("tmsis_run_id")
        submitting_state = run_stack.get("submitting_state")

        run_dict: dict = {}

        for index, state in enumerate(submitting_state):
            run_dict.update({f"{str(state).zfill(2)}": f"{tmsis_run_id[index]}"})

        self.run_dict_ref = dict(reversed(list(run_dict.items())))
        self.run_dict = self.run_dict_ref.copy()

    # -----------------------------------------------
    # Not sure if this should be moved down the line
    # to the TAF run id class we're discussing
    # -----------------------------------------------
    """ Return: tuple """
    def get_next_run_id_tuple(self) -> tuple:

        return self.run_dict_ref.popitem()

    # -----------------------------------------------
    # Return full dictionary if so desired
    # -----------------------------------------------
    """ Return: dict """
    def get_run_id_dict(self) -> dict:
        return self.run_dict

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
