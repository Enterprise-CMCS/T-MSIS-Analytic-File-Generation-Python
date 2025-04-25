from taf.APR.APR import APR
from taf.APR.APR_Runner import APR_Runner
from taf.TAF_Closure import TAF_Closure


class BASE(APR):
    """
    The TAF Annual Provider (APR) is comprised of nine files: base, affiliated group, affiliated program,
    taxonomy, Medicaid enrollment, location, license or accreditation, identifier, and bed type.
    A unique TAF APR link key is used to link the first six APR files listed.  The last three files are linked
    to the location file with a unique TAF APR location link key.  The TAF APR includes records for any provider
    with an active record in one of the twelve monthly TAF PRV files.

    Description: Generate the annual PRR segment for base.

    """

    def __init__(self, apr: APR_Runner):
        super().__init__(apr)
        self.fileseg = 'BSP'

    def create(self):

        # Create the partial base segment, pulling in only columns are not acceditation

        subcols = [
            f"""{TAF_Closure.last_best(incol='REG_FLAG')}""",
            f"""{TAF_Closure.last_best('PRVDR_DBA_NAME')}""",
            f"""{TAF_Closure.last_best('PRVDR_LGL_NAME')}""",
            f"""{TAF_Closure.last_best('PRVDR_ORG_NAME')}""",
            f"""{TAF_Closure.last_best('PRVDR_TAX_NAME')}""",
            f"""{TAF_Closure.last_best('FAC_GRP_INDVDL_CD')}""",
            f"""{ TAF_Closure.monthly_array(self, incol='PRVDR_1ST_NAME') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='PRVDR_MDL_INITL_NAME') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='PRVDR_LAST_NAME') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='SEX_CD') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='BIRTH_DT') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='DEATH_DT') }""",
            f"""{ TAF_Closure.monthly_array(self, incol='AGE_NUM') }""",
            f"""{ APR.nonmiss_month(self,'FAC_GRP_INDVDL_CD') }""",
            f"""{ APR.ind_nonmiss_month(self,'ind_any_MN') }""",
            f"""{TAF_Closure.last_best('OWNRSHP_CD')}""",
            f"""{TAF_Closure.last_best('OWNRSHP_CAT')}""",
            f"""{TAF_Closure.last_best('PRVDR_PRFT_STUS_CD')}""",
            f"""{TAF_Closure.ever_year('PRVDR_MDCD_ENRLMT_IND')}""",
            f"""{TAF_Closure.ever_year('MDCD_ENRLMT_IND')}""",
            f"""{TAF_Closure.ever_year('CHIP_ENRLMT_IND')}""",
            f"""{TAF_Closure.ever_year('MDCD_CHIP_ENRLMT_IND')}""",
            f"""{TAF_Closure.ever_year('NOT_SP_AFLTD_IND')}""",
            f"""{TAF_Closure.ever_year('PRVDR_ENRLMT_STUS_ACTV_IND')}""",
            f"""{TAF_Closure.ever_year('PRVDR_ENRLMT_STUS_DND_IND')}""",
            f"""{TAF_Closure.ever_year('PRVDR_ENRLMT_STUS_TRMNTD_IND')}""",
            f"""{TAF_Closure.ever_year('PRVDR_ENRLMT_STUS_PENDG_IND')}""",
            f"""{TAF_Closure.ever_year('MLT_SNGL_SPCLTY_GRP_IND')}""",
            f"""{TAF_Closure.ever_year('ALPTHC_OSTPTHC_PHYSN_IND')}""",
            f"""{TAF_Closure.ever_year('BHVRL_HLTH_SCL_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('CHRPRCTIC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('DNTL_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('DTRY_NTRTNL_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('EMER_MDCL_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('EYE_VSN_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('NRSNG_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('NRSNG_SRVC_RLTD_IND')}""",
            f"""{TAF_Closure.ever_year('OTHR_INDVDL_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('PHRMCY_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('PA_ADVCD_PRCTC_NRSNG_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('POD_MDCN_SRGRY_SRVCS_IND')}""",
            f"""{TAF_Closure.ever_year('RESP_DEV_REH_RESTOR_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('SPCH_LANG_HEARG_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('STDNT_HLTH_CARE_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('TT_OTHR_TCHNCL_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('AGNCY_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('AMB_HLTH_CARE_FAC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('HOSP_UNIT_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('HOSP_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('LAB_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('MCO_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('NRSNG_CSTDL_CARE_FAC_IND')}""",
            f"""{TAF_Closure.ever_year('OTHR_NONINDVDL_SRVC_PRVDRS_IND')}""",
            f"""{TAF_Closure.ever_year('RSDNTL_TRTMT_FAC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('RESP_CARE_FAC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('SUPLR_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('TRNSPRTN_SRVCS_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('SUD_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('MH_SRVC_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('EMER_SRVCS_PRVDR_IND')}""",
            f"""{TAF_Closure.ever_year('TCHNG_IND')}""",
            f"""{TAF_Closure.ever_year('ACPT_NEW_PTNTS_IND')}""",
            f"""{TAF_Closure.ever_year('ATYPICAL_PRVDR_IND')}""",
            f"""{TAF_Closure.any_month(incols='SUBMTG_STATE_PRVDR_ID',outcol='PRVDR_FLAG', condition='IS NOT NULL')}"""]

        outercols = [
            f"""{ APR.assign_nonmiss_month(self,'PRVDR_1ST_NAME','FAC_GRP_INDVDL_CD_MN','PRVDR_1ST_NAME','ind_any_MN','PRVDR_1ST_NAME') }""",
            f"""{ APR.assign_nonmiss_month(self,'PRVDR_MDL_INITL_NAME','FAC_GRP_INDVDL_CD_MN','PRVDR_MDL_INITL_NAME','ind_any_MN','PRVDR_MDL_INITL_NAME') }""",
            f"""{ APR.assign_nonmiss_month(self,'PRVDR_LAST_NAME','FAC_GRP_INDVDL_CD_MN','PRVDR_LAST_NAME','ind_any_MN','PRVDR_LAST_NAME') }""",
            f"""{ APR.assign_nonmiss_month(self,'SEX_CD','FAC_GRP_INDVDL_CD_MN','SEX_CD','ind_any_MN','SEX_CD') }""",
            f"""{ APR.assign_nonmiss_month(self,'BIRTH_DT','FAC_GRP_INDVDL_CD_MN','BIRTH_DT','ind_any_MN','BIRTH_DT') }""",
            f"""{ APR.assign_nonmiss_month(self,'DEATH_DT','FAC_GRP_INDVDL_CD_MN','DEATH_DT','ind_any_MN','DEATH_DT') }""",
            f"""{ APR.assign_nonmiss_month(self,'AGE_NUM','FAC_GRP_INDVDL_CD_MN','AGE_NUM','ind_any_MN','AGE_NUM') }"""]

        subcols_ = map(TAF_Closure.parse, subcols)
        outercols_ = map(TAF_Closure.parse, outercols)

        self.create_temp_table(fileseg='PRV', tblname='base_pr', inyear=self.year, subcols=', '.join(subcols_), outercols=', '.join(outercols_))

        # Join to the monthly _SPLMTL flags.
        z = f"""
            create or replace temporary view base_{self.year}_final as
            select a.*
                ,d2.PRVDR_NPI_01
                ,d2.PRVDR_NPI_02
                ,d2.PRVDR_NPI_CNT
                ,case when b.LCTN_SPLMTL_CT>0 then 1 else 0 end as LCTN_SPLMTL
                ,case when c.LCNS_SPLMTL_CT>0 then 1 else 0 end as LCNS_SPLMTL
                ,case when d.ID_SPLMTL_CT>0 then 1 else 0 end as ID_SPLMTL
                ,case when e.GRP_SPLMTL_CT>0 then 1 else 0 end as GRP_SPLMTL
                ,case when f.PGM_SPLMTL_CT>0 then 1 else 0 end as PGM_SPLMTL
                ,case when g.TXNMY_SPLMTL_CT>0 then 1 else 0 end as TXNMY_SPLMTL
                ,case when h.ENRLMT_SPLMTL_CT>0 then 1 else 0 end as ENRLMT_SPLMTL
                ,case when i.BED_SPLMTL_CT>0 then 1 else 0 end as BED_SPLMTL

            from base_pr_{self.year} a
            left join
                LCTN_SPLMTL_{self.year} b on a.SUBMTG_STATE_CD = b.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = b.SUBMTG_STATE_PRVDR_ID
            left join
                LCNS_SPLMTL_{self.year} c on a.SUBMTG_STATE_CD = c.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = c.SUBMTG_STATE_PRVDR_ID
            left join
                ID_SPLMTL_{self.year} d on a.SUBMTG_STATE_CD = d.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = d.SUBMTG_STATE_PRVDR_ID
            left join
                npi_final d2 on a.SUBMTG_STATE_CD = d2.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = d2.SUBMTG_STATE_PRVDR_ID
            left join
                GRP_SPLMTL_{self.year} e on a.SUBMTG_STATE_CD = e.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = e.SUBMTG_STATE_PRVDR_ID
            left join
                PGM_SPLMTL_{self.year} f on a.SUBMTG_STATE_CD = f.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = f.SUBMTG_STATE_PRVDR_ID
            left join
                TXNMY_SPLMTL_{self.year} g on a.SUBMTG_STATE_CD = g.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = g.SUBMTG_STATE_PRVDR_ID
            left join
                ENRLMT_SPLMTL_{self.year} h on a.SUBMTG_STATE_CD = h.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = h.SUBMTG_STATE_PRVDR_ID
            left join
                BED_SPLMTL_{self.year} i on a.SUBMTG_STATE_CD = i.SUBMTG_STATE_CD and a.SUBMTG_STATE_PRVDR_ID = i.SUBMTG_STATE_PRVDR_ID

            order by SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID
        """
        self.apr.append(type(self).__name__, z)

        # Insert into permanent table

        basecols = [
            'REG_FLAG',
            'PRVDR_DBA_NAME',
            'PRVDR_LGL_NAME',
            'PRVDR_ORG_NAME',
            'PRVDR_TAX_NAME',
            'FAC_GRP_INDVDL_CD',
            'PRVDR_1ST_NAME',
            'PRVDR_MDL_INITL_NAME',
            'PRVDR_LAST_NAME',
            'SEX_CD',
            'BIRTH_DT',
            'DEATH_DT',
            'AGE_NUM',
            'cast(TCHNG_IND as string) as TCHNG_IND',
            'OWNRSHP_CD',
            'OWNRSHP_CAT',
            'PRVDR_PRFT_STUS_CD',
            'ACPT_NEW_PTNTS_IND',
            'PRVDR_MDCD_ENRLMT_IND',
            'MDCD_ENRLMT_IND',
            'CHIP_ENRLMT_IND',
            'MDCD_CHIP_ENRLMT_IND',
            'NOT_SP_AFLTD_IND',
            'PRVDR_ENRLMT_STUS_ACTV_IND',
            'PRVDR_ENRLMT_STUS_DND_IND',
            'PRVDR_ENRLMT_STUS_TRMNTD_IND',
            'PRVDR_ENRLMT_STUS_PENDG_IND',
            'MLT_SNGL_SPCLTY_GRP_IND',
            'ALPTHC_OSTPTHC_PHYSN_IND',
            'BHVRL_HLTH_SCL_SRVC_PRVDR_IND',
            'CHRPRCTIC_PRVDR_IND',
            'DNTL_PRVDR_IND',
            'DTRY_NTRTNL_SRVC_PRVDR_IND',
            'EMER_MDCL_SRVC_PRVDR_IND',
            'EYE_VSN_SRVC_PRVDR_IND',
            'NRSNG_SRVC_PRVDR_IND',
            'NRSNG_SRVC_RLTD_IND',
            'OTHR_INDVDL_SRVC_PRVDR_IND',
            'PHRMCY_SRVC_PRVDR_IND',
            'PA_ADVCD_PRCTC_NRSNG_PRVDR_IND',
            'POD_MDCN_SRGRY_SRVCS_IND',
            'RESP_DEV_REH_RESTOR_PRVDR_IND',
            'SPCH_LANG_HEARG_SRVC_PRVDR_IND',
            'STDNT_HLTH_CARE_PRVDR_IND',
            'TT_OTHR_TCHNCL_SRVC_PRVDR_IND',
            'AGNCY_PRVDR_IND',
            'AMB_HLTH_CARE_FAC_PRVDR_IND',
            'HOSP_UNIT_PRVDR_IND',
            'HOSP_PRVDR_IND',
            'LAB_PRVDR_IND',
            'MCO_PRVDR_IND',
            'NRSNG_CSTDL_CARE_FAC_IND',
            'OTHR_NONINDVDL_SRVC_PRVDRS_IND',
            'RSDNTL_TRTMT_FAC_PRVDR_IND',
            'RESP_CARE_FAC_PRVDR_IND',
            'SUPLR_PRVDR_IND',
            'TRNSPRTN_SRVCS_PRVDR_IND',
            'SUD_SRVC_PRVDR_IND',
            'MH_SRVC_PRVDR_IND',
            'EMER_SRVCS_PRVDR_IND',
            'PRVDR_NPI_01',
            'PRVDR_NPI_02',
            'PRVDR_NPI_CNT',
            'PRVDR_FLAG_01',
            'PRVDR_FLAG_02',
            'PRVDR_FLAG_03',
            'PRVDR_FLAG_04',
            'PRVDR_FLAG_05',
            'PRVDR_FLAG_06',
            'PRVDR_FLAG_07',
            'PRVDR_FLAG_08',
            'PRVDR_FLAG_09',
            'PRVDR_FLAG_10',
            'PRVDR_FLAG_11',
            'PRVDR_FLAG_12',
            'LCTN_SPLMTL',
            'LCNS_SPLMTL',
            'ID_SPLMTL',
            'GRP_SPLMTL',
            'PGM_SPLMTL',
            'TXNMY_SPLMTL',
            'ENRLMT_SPLMTL',
            'BED_SPLMTL',
            "from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS",
            'cast(NULL as timestamp) as REC_UPDT_TS',
            'ATYPICAL_PRVDR_IND',
            ]

        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if self.apr.run_stats_only:
            self.apr.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return
        else:
            z = f"""
                INSERT INTO {self.apr.DA_SCHEMA}.TAF_ANN_PR_BASE
                SELECT
                    { self.table_id_cols() }
                    ,{ ','.join(basecols) }
                FROM base_{self.year}_final"""

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
