from taf.BSF import BSF_Runner
from taf.BSF.ELG import ELG


class ELG00024(ELG):
    """
    Description:  Process TMSIS_CMNTY_ENGGMT_INFO (Community Engagement) and create unique output for BSF.
                  TAF v10.0 - adds 10 Community Engagement data elements.

    """

    EFF_DT = 'CMNTY_ENGGMT_EFF_DT'
    END_DT = 'CMNTY_ENGGMT_END_DT'

    def __init__(self, bsf: BSF_Runner):
        ELG.__init__(self, bsf, 'ELG00024', 'TMSIS_CMNTY_ENGGMT_INFO', self.EFF_DT, self.END_DT)

    @property
    def base_population(self) -> str:
        """Base enrollee dataset produced by ELG00021 for this BSF run."""
        return f"ELG00021_{self.bsf.BSF_FILE_DATE}_uniq"

    def MultiIds(self, created_vars: str, sort_key: str = '', where: str = '', suffix: str = '', val: str = ''):
        """
        Override of ELG.MultiIds: same view names, but ranks with the BSF_070 sort order for this
        segment (no TMSIS_RPTG_PRD). sort_key is only a final deterministic tie-breaker.
        """
        where = f"where {where}" if where else ''
        tie_break = f",\n                        {sort_key}" if sort_key else ''

        z = f"""
            create or replace temporary view {self.tab_no}{suffix}_multi_all as
            select
                {','.join(filter(None, ['t1.*', created_vars]))}
            from {self.tab_no}{val} as t1
            inner join {self.tab_no}{suffix}_recCt as t2
                on t1.submtg_state_cd = t2.submtg_state_cd
                and t1.msis_ident_num = t2.msis_ident_num
                and t2.recCt > 1
            {where}
            """
        self.bsf.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view {self.tab_no}{suffix}_multi_step2 as
            select
                *,
                row_number() over (
                    partition by submtg_state_cd, msis_ident_num
                    order by
                        submtg_state_cd,
                        msis_ident_num,
                        {self.eff_date} desc,
                        {self.end_date} desc,
                        REC_NUM desc{tie_break}
                ) as KEEP_FLAG
            from {self.tab_no}{suffix}_multi_all
            """
        self.bsf.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view {self.tab_no}{suffix}_multi as
            select * from {self.tab_no}{suffix}_multi_step2
            where KEEP_FLAG = 1
            """
        self.bsf.append(type(self).__name__, z)

    @staticmethod
    def pad_if_numeric(column: str, width: int = 2):
        """
        Spec: Trim and left pad with zeroes to two digits if the source value is numeric.
        Non-numeric values are trimmed only; blanks become null. Values already >= width are not truncated.
        """
        return f"""case when trim({column}) rlike '^[0-9]+$' and length(trim({column})) < {width}
                    then lpad(trim({column}), {width}, '0')
                    else nullif(trim({column}), '') end"""

    def create(self):

        #  Source -> BSF renames and two-digit padding. Aliases that would collide with source
        #  columns in t1.* use a _CODE suffix and are renamed in the final segment view.
        created_vars = f"""
            trim(ADLT_GRP_QUALN) as ADLT_GRP_QUALN_CD,
            {self.pad_if_numeric('APLCBL_INDVDL_EXCPTN')} as APLCBL_INDVDL_EXCPTN_CODE,
            {self.pad_if_numeric('EXCLSN_TYPE_CD')} as EXCLSN_TYPE_CODE,
            {self.pad_if_numeric('MDCL_FRLTY_TYPE_CD')} as MDCL_FRLTY_TYPE_CODE
        """

        #  Rule 2: restrict to enrollees in the base ELG00021 dataset with a non-blank beneficiary ID.
        #  Done before counting so recCt reflects only eligible records.
        z = f"""
            create or replace temporary view {self.tab_no}_base as

            select t1.*
                from {self.tab_no} t1
                left semi join {self.base_population} b
                on t1.submtg_state_cd = b.submtg_state_cd
                and t1.msis_ident_num = b.msis_ident_num
                where nullif(trim(t1.msis_ident_num), '') is not null
                """
        self.bsf.append(type(self).__name__, z)

        #  Create temp table to determine which beneficiaries have multiple records
        z = f"""
            create or replace temporary view {self.tab_no}_recCt as

            select
                submtg_state_cd,
                msis_ident_num,
                count(*) as recCt
                from {self.tab_no}_base
                group by submtg_state_cd, msis_ident_num
                """
        self.bsf.append(type(self).__name__, z)

        #  Set aside table data for beneficiaries with only one record
        z = f"""
            create or replace temporary view {self.tab_no}_uniq as

            select t1.*,

                {created_vars},

                1 as KEEP_FLAG

                from {self.tab_no}_base t1
                inner join {self.tab_no}_recCt as t2
                on t1.submtg_state_cd = t2.submtg_state_cd
                and t1.msis_ident_num  = t2.msis_ident_num
                and t2.recCt=1
                """
        self.bsf.append(type(self).__name__, z)

        #  De-dupe beneficiaries with multiple records (BSF_070: latest eff dt, end dt, REC_NUM wins).
        #  sort_key is a deterministic tie-breaker only.
        sort_key = """coalesce(trim(adlt_grp_qualn),'xx') || coalesce(trim(aplcbl_indvdl_qualn_type_cd),'xx') ||
                       coalesce(trim(aplcbl_indvdl_excptn),'xx') || coalesce(trim(exclsn_type_cd),'xx') ||
                       coalesce(trim(mdcl_frlty_type_cd),'xx') || coalesce(trim(work_flag),'xx') ||
                       coalesce(trim(cmnty_srvc_flag),'xx') || coalesce(trim(jobs_pgm_flag),'xx') ||
                       coalesce(trim(edctn_flag),'xx') || coalesce(trim(incm_flag),'xx')"""

        self.MultiIds(created_vars, sort_key, val='_base')

        #  Union together tables for a permanent table, keeping only the BSF output elements.
        #  Output names/order match dc_val_data_sources_catalog.taf_v10.taf_mon_bsf (all STRING).
        z = f"""
            create or replace temporary view {self.tab_no}_{self.bsf.BSF_FILE_DATE}_uniq as

            select
                submtg_state_cd,
                msis_ident_num,
                ADLT_GRP_QUALN_CD,
                APLCBL_INDVDL_QUALN_TYPE_CD,
                APLCBL_INDVDL_EXCPTN_CODE as APLCBL_INDVDL_EXCPTN_CD,
                EXCLSN_TYPE_CODE as EXCLSN_TYPE_CD,
                MDCL_FRLTY_TYPE_CODE as MDCL_FRLTY_TYPE_CD,
                WORK_FLAG,
                CMNTY_SRVC_FLAG,
                JOBS_PGM_FLAG,
                EDCTN_FLAG,
                INCM_FLAG

            from (
                select * from {self.tab_no}_uniq
                union all
                select * from {self.tab_no}_multi
            )
            """
        self.bsf.append(type(self).__name__, z)
