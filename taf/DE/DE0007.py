from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner
from taf.TAF_Closure import TAF_Closure


class DE0007(DE):
    table_name: str = "mfp"
    tbl_suffix: str = "mfp"

    def __init__(self, de: DE_Runner):
        # TODO: Review this
        DE.__init__(self, DE, 'DE00007')

    def create(self):
        super().create()
        self.create_temp()
        self.create_mfp_suppl_table()

    def create_temp(self):
        # Create an indicator for ANY of the MFP monthly flags = 1 which we will
        # use to create MFP_SPLMTL

        s = f"""{TAF_Closure.last_best('MFP_PRTCPTN_ENDD_RSN_CD')}
                {TAF_Closure.last_best('MFP_LVS_WTH_FMLY_CD')}
                {TAF_Closure.last_best('MFP_QLFYD_INSTN_CD')}
                {TAF_Closure.last_best('MFP_RINSTLZD_RSN_CD')}
                {TAF_Closure.last_best('MFP_QLFYD_RSDNC_CD')}
                {TAF_Closure.monthly_array('MFP_PRTCPNT_FLAG')}
                {TAF_Closure.last_best('MFP_PRTCPNT_FLAG', outcol='MFP_PRTCPNT_FLAG_LTST')}
                {TAF_Closure.ever_year('MFP_PRTCPNT_FLAG')}
            """

        # Create MFP_SPLMTL (which will go onto the base segment AND determines
        # the records that go into the permanent MFP table)
        os = """,case when MFP_PRTCPNT_FLAG_EVR=1 or
                        nullif(MFP_PRTCPTN_ENDD_RSN_CD,'00') is not null or
                        nullif(MFP_QLFYD_INSTN_CD,'00') is not null or
                        nullif(MFP_QLFYD_RSDNC_CD,'00') is not null or
                        nullif(MFP_RINSTLZD_RSN_CD,'00') is not null or
                        nullif(MFP_LVS_WTH_FMLY_CD,'2') is not null

                then 1 else 0
                end as MFP_SPLMTL
            """
        DE.create_temp_table(tblname=self.table_name, subcols=s, outercols=os)
        return

    def create_mfp_suppl_table(self):
        z = f"""create or replace temporary view MFP_SPLMTL_{self.YEAR} as
        select submtg_state_cd
                ,msis_ident_num
                ,MFP_SPLMTL

        from mfp_{self.YEAR}"""

        self.de.append(type(self).__name__, z)

        z = f"""insert into {self.DA_SCHEMA}.TAF_ANN_DE_{self.tbl_suffix}
                select

                    {DE.table_id_cols}
                    ,MFP_PRTCPTN_ENDD_RSN_CD
                    ,MFP_LVS_WTH_FMLY_CD
                    ,MFP_QLFYD_INSTN_CD
                    ,MFP_RINSTLZD_RSN_CD
                    ,MFP_QLFYD_RSDNC_CD
                    ,MFP_PRTCPNT_FLAG_01
                    ,MFP_PRTCPNT_FLAG_02
                    ,MFP_PRTCPNT_FLAG_03
                    ,MFP_PRTCPNT_FLAG_04
                    ,MFP_PRTCPNT_FLAG_05
                    ,MFP_PRTCPNT_FLAG_06
                    ,MFP_PRTCPNT_FLAG_07
                    ,MFP_PRTCPNT_FLAG_08
                    ,MFP_PRTCPNT_FLAG_09
                    ,MFP_PRTCPNT_FLAG_10
                    ,MFP_PRTCPNT_FLAG_11
                    ,MFP_PRTCPNT_FLAG_12
                    ,MFP_PRTCPNT_FLAG_LTST

                from mfp_{self.YEAR}
                where MFP_SPLMTL=1"""

        self.de.append(type(self).__name__, z)
        return

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
# <http://creativecommons.org/publicdomain/zero/1.0/>elg00005
