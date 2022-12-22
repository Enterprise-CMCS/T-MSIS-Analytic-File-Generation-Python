# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
from taf.APR.APR import APR
from taf.APR.APR_Runner import APR_Runner


class IDT(APR):
    """
    The TAF Annual Provider (APR) is comprised of nine files: base, affiliated group, affiliated program,
    taxonomy, Medicaid enrollment, location, license or accreditation, identifier, and bed type.  
    A unique TAF APR link key is used to link the first six APR files listed.  The last three files are linked
    to the location file with a unique TAF APR location link key.  The TAF APR includes records for any provider
    with an active record in one of the twelve monthly TAF PRV files. 

    Description:  Generate the annual PR segment for identifiers

    Note:   This program aggregates unique values across the CY year for variables in collist.
            It creates _SPLMTL flag for base.
            Then inserts identifiers records into the permanent TAF table.
            A separate table with NPI information is also created which has one record per 
            provider(submtg_state_cd, submtg_state_prvdr_id)and is used 
            to linking prvdr_npi_01 prvdr_npi_02 prvdr_npi_cnt back to the PR base segment.
    """

    def __init__(self, apr: APR_Runner):
        super().__init__(apr)
        self.fileseg = 'IDT'

        # create table with prvdr_npi_01 prvdr_npi_02 prvdr_npi_cnt with
        # one record per provider(submtg_state_cd, submtg_state_prvdr_id, splmtl_submsn_type)
        # which link to PR base

        # select all monthly taf_prv_idt records with prvdr_id_type_cd='2' and prvdr_id is not null, and
        # create splmtl_submsn_type for the link back to PR base

        z = f"""
            create or replace temporary view npi_main as
            select 
                b.submtg_state_cd, 
                b.submtg_state_prvdr_id, 
                b.prvdr_id, 
                substring(a.prv_fil_dt,5,2) as month
            from 
                max_run_id_prv_{self.year} a 
            inner join 
                {self.apr.DA_SCHEMA}.taf_PRV_IDT b 
            on
                a.submtg_state_cd = b.submtg_state_cd and 
                a.prv_fil_dt = b.prv_fil_dt and 
                a.da_run_id = b.da_run_id
            where 
                b.prvdr_id_type_cd='2' and 
                b.prvdr_id is not null
        """
        self.apr.append(type(self).__name__, z)

        # group to unique NPIs in the year and add prvdr_npi_cnt and max(month) as maxmo,
        # prvdr_npi_cnt value is the same for each provider's npi records
        # add row numbers (_ndx) with partition and order for prvdr_npi array assignment

        z = f"""
            create or replace temporary view npi_all as
            select 
                a.submtg_state_cd, 
                a.submtg_state_prvdr_id, 
                a.maxmo, 
                a.prvdr_id as prvdr_npi, 
                b.prvdr_npi_cnt,
            /* grouping code */
                row_number() over (
                    partition by 
                        a.submtg_state_cd, 
                        a.submtg_state_prvdr_id
                    order by 
                        a.maxmo desc, 
                        a.prvdr_id desc
                        ) as _ndx
            from (select 
                    submtg_state_cd, 
                    submtg_state_prvdr_id, 
                    prvdr_id, 
                    max(month) as maxmo
                from 
                    npi_main 
                group by 
                    submtg_state_cd, 
                    submtg_state_prvdr_id, 
                    prvdr_id) as a
            left join
                (select 
                    submtg_state_cd, 
                    submtg_state_prvdr_id, 
                    count(distinct(prvdr_id)) as prvdr_npi_cnt
                from 
                    npi_main 
                group by 
                    submtg_state_cd, 
                    submtg_state_prvdr_id) as b
            on 
                a.submtg_state_cd=b.submtg_state_cd and 
                a.submtg_state_prvdr_id=b.submtg_state_prvdr_id
        """
        self.apr.append(type(self).__name__, z)

        # create one record per provider, retain prvdr_npi_cnt, and use row numbers (_ndx) to create array prvdr_npi_01 prvdr_npi_02

        z = f"""
            create or replace temporary view npi_final as
            select 
                submtg_state_cd, 
                submtg_state_prvdr_id, 
                prvdr_npi_cnt
                { APR.map_arrayvars(varnm='prvdr_npi', N=3) }
            from 
                npi_all
            group by 
                submtg_state_cd, 
                submtg_state_prvdr_id, 
                prvdr_npi_cnt
        """
        self.apr.append(type(self).__name__, z)

    def create(self):
        """
        Create the identifiers segment.  
        """
         
        # Create identifiers segment. Select records and select or create data elements

        collist_s = ['PRVDR_LCTN_ID',
                     'PRVDR_ID_TYPE_CD',
                     'PRVDR_ID',
                     'PRVDR_ID_ISSG_ENT_ID_TXT']

        self.annual_segment(fileseg='IDT', dtfile='PRV', collist=collist_s, mnths='PRVDR_ID_FLAG', outtbl="id_pr_" + str(self.year))

        # Create temp table with just GRP_SPLMTL to join to base

        self.create_splmlt(segname='ID', segfile="id_pr_" + str(self.year))

        # Insert into permanent table

        basecols = [ 'PRVDR_LCTN_ID'
                    ,'PRVDR_ID_TYPE_CD'
                    ,'PRVDR_ID'
                    ,'PRVDR_ID_ISSG_ENT_ID_TXT'
                    ,'PRVDR_ID_FLAG_01'
                    ,'PRVDR_ID_FLAG_02'
                    ,'PRVDR_ID_FLAG_03'
                    ,'PRVDR_ID_FLAG_04'
                    ,'PRVDR_ID_FLAG_05'
                    ,'PRVDR_ID_FLAG_06'
                    ,'PRVDR_ID_FLAG_07'
                    ,'PRVDR_ID_FLAG_08'
                    ,'PRVDR_ID_FLAG_09'
                    ,'PRVDR_ID_FLAG_10'
                    ,'PRVDR_ID_FLAG_11'
                    ,'PRVDR_ID_FLAG_12']

        z = f"""
            INSERT INTO {self.apr.DA_SCHEMA}.TAF_ANN_PR_ID
            SELECT
                {self.table_id_cols(loctype=2)}
                ,{ ', '.join(basecols) }
                ,to_timestamp('{self.apr.DA_RUN_ID}', 'yyyyMMddHHmmss') as REC_ADD_TS
                ,current_timestamp() as REC_UPDT_TS
            FROM id_pr_{self.year}"""
        self.apr.append(type(self).__name__, z)


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
