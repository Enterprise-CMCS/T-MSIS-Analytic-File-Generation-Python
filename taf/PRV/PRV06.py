from taf.PRV import PRV_Runner

from taf.PRV.PRV import PRV


class PRV06(PRV):

    def __init__(self, prv: PRV_Runner):
        super().__init__(prv)

    def process_06_taxonomy(self, maintbl, outtbl):
        """
        000-06 taxonomy segment
        """

        # screen out all but the latest(selected) run id - provider id
        runlist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id']

        self.screen_runid('tmsis.Prov_Taxonomy_Classification',
                          maintbl,
                          runlist,
                          'Prov06_Taxonomy_Latest1')

        # row count
        # self.prv.countrows(Prov06_Taxonomy_Latest1, cnt_latest, PRV06_Latest)

        cols06 = ['tms_run_id',
                  'tms_reporting_period',
                  'record_number',
                  'submitting_state',
                  'submitting_state as submtg_state_cd',
                  'upper(submitting_state_prov_id) as submitting_state_prov_id',
                  """case
                      when (prov_classification_type='2' or prov_classification_type='3') and
                      length(trim(prov_classification_code))<2 and length(trim(prov_classification_code))>0 and
                      nullif(trim(upper(prov_classification_code)),'') is not null then lpad(trim(upper(prov_classification_code)),2,'0')
                      when prov_classification_type='4' and
                      length(trim(prov_classification_code))<3 and length(trim(prov_classification_code))>0 and
                      nullif(trim(upper(prov_classification_code)),'') is not null then lpad(trim(upper(prov_classification_code)),3,'0')
                      else nullif(trim(upper(prov_classification_code)),'')
                   end as prov_classification_code""",
                  'prov_classification_type',
                  'prov_taxonomy_classification_eff_date',
                  'prov_taxonomy_classification_end_date']

        whr06 = 'prov_classification_type is not null and upper(prov_classification_code) is not null'

        self.copy_activerows('Prov06_Taxonomy_Latest1',
                             cols06,
                             whr06,
                             'Prov06_Taxonomy_Copy')

        # row count
        # self.prv.countrows(Prov06_Taxonomy_Copy, cnt_active, PRV06_Active)

        # screen for Taxonomy during the month
        keylist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_classification_type']

        self.screen_dates('Prov06_Taxonomy_Copy',
                          keylist,
                          'prov_taxonomy_classification_eff_date',
                          'prov_taxonomy_classification_end_date',
                          'Prov06_Taxonomy_Latest2')

        # row count
        # self.prv.countrows(Prov06_Taxonomy_Latest2, cnt_date, PRV06_Date)

        # remove duplicate records
        grplist = ['tms_run_id',
                   'submitting_state',
                   'submitting_state_prov_id',
                   'prov_classification_type',
                   'prov_classification_code']

        self.remove_duprecs('Prov06_Taxonomy_Latest2',
                            grplist,
                            'prov_taxonomy_classification_eff_date',
                            'prov_taxonomy_classification_end_date',
                            'prov_classification_type',
                            outtbl)

        # row count
        # self.prv.countrows(&outtbl, cnt_final, PRV06_Final)

    def hc_prvdr_sw_pos(self, max_keep):
        """
        Function to generate strings needed for creating sw_positions col
        """

        hc_prvdr_sw_pos = []
        cmma_cnct = ',\n\t\t\t'
        for i in list(range(1, 15 + 1)):
            if i <= max_keep:
                hc_prvdr_sw_pos.append(f"nvl(nppes.`Healthcare Provider Primary Taxonomy Switch_{i}`,' ')".format())
        return cmma_cnct.join(hc_prvdr_sw_pos)

    def hc_prvdr_cd_pos(self, max_keep):
        """
        Function to generate strings needed for creating cd_positions col
        """

        hc_prvdr_cd_pos = []
        cmma_cnct = ',\n\t\t\t'
        for i in list(range(1, 15 + 1)):
            if i <= max_keep:
                hc_prvdr_cd_pos.append(f"nvl(substring(nppes.`Healthcare Provider Taxonomy Code_{i}`,10,1),' ')".format())
        return cmma_cnct.join(hc_prvdr_cd_pos)

    def hc_prvdr_sw_pos_y(self, max_keep):
        """
        Function to generate strings needed for creating case when for prvdr_clsfctn_cd col
        """

        hc_prvdr_sw_pos_y = []
        new_line = '\n\t\t\t'
        for i in list(range(1, 15 + 1)):
            if i <= max_keep:
                hc_prvdr_sw_pos_y.append(f"when (taxo_switches = 1 and position('Y' in sw_positions)={i}) then nvl(hc_prvdr_txnmy_cd_{i},' ')".format())
        return new_line.join(hc_prvdr_sw_pos_y)

    def hc_prvdr_sw_pos_x(self, max_keep):
        """
        Function to generate strings needed for creating case when for prvdr_clsfctn_cd col
        """

        hc_prvdr_sw_pos_x = []
        new_line = '\n\t\t\t'
        for i in list(range(1, 15 + 1)):
            if i <= max_keep:
                hc_prvdr_sw_pos_x.append(f"when (taxo_switches = 0 and taxo_cnt = 1 and position('X' in cd_positions)={i}) then nvl(hc_prvdr_txnmy_cd_{i},' ')".format())
        return new_line.join(hc_prvdr_sw_pos_x)

    def prmry_NPPES_tax0b(self, max_keep):
        """
        Function to generate strings needed for creating sql for nppes_tax0b table
        """

        prmry_NPPES_tax0b = []
        new_line = '\n\n \t\tUNION ALL \n\t\t\t'
        for i in list(range(1, 15 + 1)):
            if i <= max_keep:
                prmry_NPPES_tax0b.append(f"""
                    (SELECT
                        prvdr_npi,
                        prvdr_id as prvdr_id_chr,
                        hc_prvdr_txnmy_cd_{i} as prvdr_clsfctn_cd
                    FROM
                        nppes_tax_flags
                    WHERE
                        taxo_switches > 1 and
                        nvl(hc_prvdr_txnmy_cd_{i},' ') <> ' ' and
                        hc_prvdr_prmry_txnmy_sw_{i}='Y')""".format())
        return new_line.join(prmry_NPPES_tax0b)


    def nppes_tax(self, id_intbl, tax_intbl, tax_outtbl):
        """
        Function to generate SQL query with multiple temporary views for nppes_tax.
        """

        # get NPPES taxonomy codes using NPI from PRV identifier segment
        z = f"""
            create or replace temporary view nppes_id1 as
            select
                submtg_state_cd,
                submtg_state_cd as submitting_state,
                tmsis_run_id as tms_run_id,
                submtg_state_prvdr_id as submitting_state_prov_id,
                prvdr_id
            from
                {id_intbl}
            where
                prvdr_id_type_cd='2'
            group by
                submtg_state_cd,
                tmsis_run_id,
                submtg_state_prvdr_id,
                prvdr_id
            order by
                prvdr_id
            """
        self.prv.append(type(self).__name__, z)

        # create a table with fewer columns for the initial record pull from NPPES table

        z = f"""
            create or replace temporary view nppes_id2 as
            select
                prvdr_id
            from
                nppes_id1
            group by
                prvdr_id
            order by
                prvdr_id
            """
        self.prv.append(type(self).__name__, z)

        # link on NPI in NPPES set flags to identify primary taxonomy codes
        # that should be included in the TAF classification segment
        max_keep = 15

        z = f"""
            create or replace temporary view nppes_tax_flags_temp as
            select
                nppes.NPI AS prvdr_npi,
                nppes.`Healthcare Provider Taxonomy Code_1` AS hc_prvdr_txnmy_cd_1,
                nppes.`Healthcare Provider Primary Taxonomy Switch_1` AS hc_prvdr_prmry_txnmy_sw_1,
                nppes.`Healthcare Provider Taxonomy Code_2` AS hc_prvdr_txnmy_cd_2,
                nppes.`Healthcare Provider Primary Taxonomy Switch_2` AS hc_prvdr_prmry_txnmy_sw_2,
                nppes.`Healthcare Provider Taxonomy Code_3` AS hc_prvdr_txnmy_cd_3,
                nppes.`Healthcare Provider Primary Taxonomy Switch_3` AS hc_prvdr_prmry_txnmy_sw_3,
                nppes.`Healthcare Provider Taxonomy Code_4` AS hc_prvdr_txnmy_cd_4,
                nppes.`Healthcare Provider Primary Taxonomy Switch_4` AS hc_prvdr_prmry_txnmy_sw_4,
                nppes.`Healthcare Provider Taxonomy Code_5` AS hc_prvdr_txnmy_cd_5,
                nppes.`Healthcare Provider Primary Taxonomy Switch_5` AS hc_prvdr_prmry_txnmy_sw_5,
                nppes.`Healthcare Provider Taxonomy Code_6` AS hc_prvdr_txnmy_cd_6,
                nppes.`Healthcare Provider Primary Taxonomy Switch_6` AS hc_prvdr_prmry_txnmy_sw_6,
                nppes.`Healthcare Provider Taxonomy Code_7` AS hc_prvdr_txnmy_cd_7,
                nppes.`Healthcare Provider Primary Taxonomy Switch_7` AS hc_prvdr_prmry_txnmy_sw_7,
                nppes.`Healthcare Provider Taxonomy Code_8` AS hc_prvdr_txnmy_cd_8,
                nppes.`Healthcare Provider Primary Taxonomy Switch_8` AS hc_prvdr_prmry_txnmy_sw_8,
                nppes.`Healthcare Provider Taxonomy Code_9` AS hc_prvdr_txnmy_cd_9,
                nppes.`Healthcare Provider Primary Taxonomy Switch_9` AS hc_prvdr_prmry_txnmy_sw_9,
                nppes.`Healthcare Provider Taxonomy Code_10` AS hc_prvdr_txnmy_cd_10,
                nppes.`Healthcare Provider Primary Taxonomy Switch_10` AS hc_prvdr_prmry_txnmy_sw_10,
                nppes.`Healthcare Provider Taxonomy Code_11` AS hc_prvdr_txnmy_cd_11,
                nppes.`Healthcare Provider Primary Taxonomy Switch_11` AS hc_prvdr_prmry_txnmy_sw_11,
                nppes.`Healthcare Provider Taxonomy Code_12` AS hc_prvdr_txnmy_cd_12,
                nppes.`Healthcare Provider Primary Taxonomy Switch_12` AS hc_prvdr_prmry_txnmy_sw_12,
                nppes.`Healthcare Provider Taxonomy Code_13` AS hc_prvdr_txnmy_cd_13,
                nppes.`Healthcare Provider Primary Taxonomy Switch_13` AS hc_prvdr_prmry_txnmy_sw_13,
                nppes.`Healthcare Provider Taxonomy Code_14` AS hc_prvdr_txnmy_cd_14,
                nppes.`Healthcare Provider Primary Taxonomy Switch_14` AS hc_prvdr_prmry_txnmy_sw_14,
                nppes.`Healthcare Provider Taxonomy Code_15` AS hc_prvdr_txnmy_cd_15,
                nppes.`Healthcare Provider Primary Taxonomy Switch_15` AS hc_prvdr_prmry_txnmy_sw_15,
                t2.prvdr_id,
                CONCAT({ self.hc_prvdr_sw_pos(max_keep) }) as sw_positions,
                CONCAT({ self.hc_prvdr_cd_pos(max_keep) }) as cd_positions
            from
                nppes_id2 t2
            left join
                taf_python.npidata nppes
            on
                t2.prvdr_id=nppes.NPI
            """
        self.prv.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view nppes_tax_flags as
            select
                *,
                (length(sw_positions) - coalesce(length(regexp_replace(sw_positions, 'Y', '')), 0)) as taxo_switches,
                (length(cd_positions) - coalesce(length(regexp_replace(cd_positions, 'X', '')), 0)) as taxo_cnt
            from
                nppes_tax_flags_temp
            """
        self.prv.append(type(self).__name__, z)

        z = f"""
                create or replace temporary view nppes_tax0 as
                select
                    distinct prvdr_npi,
                    prvdr_id as prvdr_id_chr,
                    case
                        { self.hc_prvdr_sw_pos_y(max_keep) }
                        { self.hc_prvdr_sw_pos_x(max_keep) }
                        else null
                    end as prvdr_clsfctn_cd
                FROM
                    nppes_tax_flags
                WHERE
                    taxo_switches = 1 or
                    (taxo_switches = 0 and taxo_cnt = 1)
        """
        self.prv.append(type(self).__name__, z)

        z = f"""
                create or replace temporary view nppes_tax0b as
                { self.prmry_NPPES_tax0b(max_keep) }
        """
        self.prv.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view nppes_tax1 as
            (select
                prvdr_npi,
                prvdr_id_chr,
                prvdr_clsfctn_cd
            from
                nppes_tax0)

            union

            (select
                prvdr_npi,
                prvdr_id_chr,
                prvdr_clsfctn_cd
            from
                nppes_tax0b)

            order by
                prvdr_npi
            """
        self.prv.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view nppes_tax_final as
            select
                distinct i.tms_run_id,
                i.submtg_state_cd,
                i.submitting_state,
                i.submitting_state_prov_id,
                'N' as prvdr_clsfctn_type_cd,
                n.prvdr_clsfctn_cd
            from
                nppes_id1 i
            left join
                NPPES_tax1 n
            on
                i.prvdr_id=n.prvdr_id_chr
            where
                n.prvdr_npi is not null
            order by
                5
            """
        self.prv.append(type(self).__name__, z)

        z = f"""
            create or replace temporary view {tax_outtbl} as
            (select
                tms_run_id,
                submtg_state_cd,
                submitting_state,
                submitting_state_prov_id,
                prvdr_clsfctn_type_cd,
                prvdr_clsfctn_cd
            from
                {tax_intbl})

            union

            (select
                tms_run_id,
                submtg_state_cd,
                submitting_state,
                submitting_state_prov_id,
                prvdr_clsfctn_type_cd,
                prvdr_clsfctn_cd
            from
                nppes_tax_final
            order by
                { ','.join(self.srtlist) })
            """
        self.prv.append(type(self).__name__, z)

    def create(self):
        """
        Create the PRV06 taxonomy segment.
        """

        self.process_06_taxonomy('Prov02_Main',
                                 'Prov06_Taxonomies')

        # create the separate linked child table
        # add code to validate and recode source variables (when needed), use SAS variable names, add linking variables, and sort records

        self.recode_lookup('Prov06_Taxonomies',
                           self.srtlist,
                           'prv_formats_sm',
                           'CLSSCDV',
                           'prov_classification_type',
                           'PRVDR_CLSFCTN_TYPE_CD',
                           'Prov06_Taxonomies_TYP',
                           'C',
                           1)

        # validate a portion of the provider classification codes

        z = f"""
                create or replace temporary view Prov06_Taxonomies_CCD as
                select *,
                        case when PRVDR_CLSFCTN_TYPE_CD='1' then prov_classification_code end as PRVDR_CLSFCTN_CD_TAX,
                        case when PRVDR_CLSFCTN_TYPE_CD='2' then trim(prov_classification_code) end as PRVDR_CLSFCTN_CD_SPC,
                        case when PRVDR_CLSFCTN_TYPE_CD='3' then trim(prov_classification_code) end as PRVDR_CLSFCTN_CD_PTYP,
                        case when PRVDR_CLSFCTN_TYPE_CD='4' then trim(prov_classification_code) end as PRVDR_CLSFCTN_CD_CSRV
                from Prov06_Taxonomies_TYP
                where PRVDR_CLSFCTN_TYPE_CD is not null
                order by { ','.join(self.srtlist) }
            """
        self.prv.append(type(self).__name__, z)

        z = f"""
                create or replace temporary view Prov06_Taxonomies_CCD2 as
                select B.*,
                        case when B.PRVDR_CLSFCTN_TYPE_CD='1' then B.PRVDR_CLSFCTN_CD_TAX
                            when B.PRVDR_CLSFCTN_TYPE_CD='2' then SPC.label
                            when B.PRVDR_CLSFCTN_TYPE_CD='3' then TYP.label
                            when B.PRVDR_CLSFCTN_TYPE_CD='4' then SRV.label end as PRVDR_CLSFCTN_CD
                from Prov06_Taxonomies_CCD B
                        left join (select * from prv_formats_sm where fmtname='CLSPRSPC') SPC on B.PRVDR_CLSFCTN_CD_SPC=SPC.start
                        left join (select * from prv_formats_sm where fmtname='CLSPRTYP') TYP on B.PRVDR_CLSFCTN_CD_PTYP=TYP.start
                        left join (select * from prv_formats_sm where fmtname='CLSSRVCD') SRV on B.PRVDR_CLSFCTN_CD_CSRV=SRV.start
                order by { ','.join(self.srtlist) }
        """
        self.prv.append(type(self).__name__, z)

        self.nppes_tax('Prov05_Identifiers_CNST', 'Prov06_Taxonomies_CCD2', 'Prov06_Taxonomies_ALL')

        z = f"""
                create or replace temporary view Prov06_Taxonomies_seg as
                select {self.prv.DA_RUN_ID} as DA_RUN_ID,
                        cast (('{self.prv.VERSION}' || '-' || { self.prv.monyrout } || '-' || SUBMTG_STATE_CD || '-' || coalesce(submitting_state_prov_id, '*')) as varchar(50)) as PRV_LINK_KEY,
                        '{self.prv.TAF_FILE_DATE}' as PRV_FIL_DT,
                        '{self.prv.VERSION}' as PRV_VRSN,
                        tms_run_id as TMSIS_RUN_ID,
                        SUBMTG_STATE_CD,
                        submitting_state_prov_id as SUBMTG_STATE_PRVDR_ID,
                        PRVDR_CLSFCTN_TYPE_CD,
                        PRVDR_CLSFCTN_CD,
                        from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS,
                        cast(NULL as timestamp) as REC_UPDT_TS
                        from Prov06_Taxonomies_ALL
                        where PRVDR_CLSFCTN_TYPE_CD is not null and PRVDR_CLSFCTN_CD is not null
                order by TMSIS_RUN_ID, SUBMTG_STATE_CD, SUBMTG_STATE_PRVDR_ID
        """
        self.prv.append(type(self).__name__, z)

        # insert contents of temp table into final TAF file for segment 6

        # insert into &DA_SCHEMA..TAF_PRV_TAX
        # select *
        # from Prov06_Taxonomies_seg

        # create the fields to merge with the root/main file

        self.recode_lookup('Prov06_Taxonomies_ALL',
                           self.srtlist,
                           'prv_formats_sm',
                           'TAXTYP',
                           'PRVDR_CLSFCTN_CD',
                           'PRVDR_CLSFCTN_IND',
                           'Prov06_Taxonomies_IND',
                           'N')

        self.recode_lookup('Prov06_Taxonomies_IND',
                           self.srtlist,
                           'prv_formats_sm',
                           'SMECLASS',
                           'PRVDR_CLSFCTN_CD',
                           'PRVDR_CLSFCTN_SME',
                           'Prov06_Taxonomies_SME',
                           'N')

        self.recode_lookup('Prov06_Taxonomies_SME',
                           self.srtlist,
                           'prv_formats_sm',
                           'MHPRVTY',
                           'PRVDR_CLSFCTN_CD',
                           'PRVDR_CLSFCTN_MHT',
                           'Prov06_Taxonomies_MHT',
                           'N')

        z = f"""
                create or replace temporary view Prov06_Taxonomies_CNST as
                select { ','.join(self.srtlist) },
                        PRVDR_CLSFCTN_IND,
                        PRVDR_CLSFCTN_TYPE_CD,
                        case
                        when PRVDR_CLSFCTN_IND=1 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as MLT_SNGL_SPCLTY_GRP_IND,
                        case
                        when PRVDR_CLSFCTN_IND=2 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as ALPTHC_OSTPTHC_PHYSN_IND,
                        case
                        when PRVDR_CLSFCTN_IND=3 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as BHVRL_HLTH_SCL_SRVC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=4 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as CHRPRCTIC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=5 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as DNTL_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=6 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as DTRY_NTRTNL_SRVC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=7 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as EMER_MDCL_SRVC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=8 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as EYE_VSN_SRVC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=9 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as NRSNG_SRVC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=10 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as NRSNG_SRVC_RLTD_IND,
                        case
                        when PRVDR_CLSFCTN_IND=11 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as OTHR_INDVDL_SRVC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=12 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as PHRMCY_SRVC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=13 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as PA_ADVCD_PRCTC_NRSNG_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=14 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as POD_MDCN_SRGRY_SRVCS_IND,
                        case
                        when PRVDR_CLSFCTN_IND=15 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as RESP_DEV_REH_RESTOR_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=16 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as SPCH_LANG_HEARG_SRVC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=17 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as STDNT_HLTH_CARE_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=18 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as TT_OTHR_TCHNCL_SRVC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=19 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as AGNCY_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=20 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as AMB_HLTH_CARE_FAC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=21 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as HOSP_UNIT_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=22 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as HOSP_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=23 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as LAB_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=24 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as MCO_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=25 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as NRSNG_CSTDL_CARE_FAC_IND,
                        case
                        when PRVDR_CLSFCTN_IND=26 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as OTHR_NONINDVDL_SRVC_PRVDRS_IND,
                        case
                        when PRVDR_CLSFCTN_IND=27 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as RSDNTL_TRTMT_FAC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=28 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as RESP_CARE_FAC_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=29 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as SUPLR_PRVDR_IND,
                        case
                        when PRVDR_CLSFCTN_IND=30 then 1
                        when PRVDR_CLSFCTN_IND is null then null
                        else 0
                        end as TRNSPRTN_SRVCS_PRVDR_IND,
                        case
                        when (PRVDR_CLSFCTN_TYPE_CD='1' or PRVDR_CLSFCTN_TYPE_CD='N') and PRVDR_CLSFCTN_SME=1 then 1
                        when PRVDR_CLSFCTN_TYPE_CD='2' and PRVDR_CLSFCTN_SME=4 then 1
                        when PRVDR_CLSFCTN_TYPE_CD='4' and PRVDR_CLSFCTN_SME=7 then 1
                        when PRVDR_CLSFCTN_CD='.' or PRVDR_CLSFCTN_CD is null or PRVDR_CLSFCTN_TYPE_CD='.' or PRVDR_CLSFCTN_TYPE_CD is null then null
                        when PRVDR_CLSFCTN_TYPE_CD<>'1' and PRVDR_CLSFCTN_TYPE_CD<>'N' and PRVDR_CLSFCTN_TYPE_CD<>'2' and PRVDR_CLSFCTN_TYPE_CD<>'4' then null
                        else 0
                        end as SUD_SRVC_PRVDR_IND,
                        case
                        when (PRVDR_CLSFCTN_TYPE_CD='1' or PRVDR_CLSFCTN_TYPE_CD='N') and PRVDR_CLSFCTN_SME=2 then 1
                        when PRVDR_CLSFCTN_TYPE_CD='2' and PRVDR_CLSFCTN_SME=5 then 1
                        when PRVDR_CLSFCTN_TYPE_CD='3' and PRVDR_CLSFCTN_MHT=1 then 1
                        when PRVDR_CLSFCTN_TYPE_CD='4' and PRVDR_CLSFCTN_SME=8 then 1
                        when PRVDR_CLSFCTN_CD='.' or PRVDR_CLSFCTN_CD is null or PRVDR_CLSFCTN_TYPE_CD='.' or PRVDR_CLSFCTN_TYPE_CD is null then null
                        when PRVDR_CLSFCTN_TYPE_CD<>'1' and PRVDR_CLSFCTN_TYPE_CD<>'N' and PRVDR_CLSFCTN_TYPE_CD<>'2' and  PRVDR_CLSFCTN_TYPE_CD<>'3' and PRVDR_CLSFCTN_TYPE_CD<>'4' then null
                        else 0
                        end as MH_SRVC_PRVDR_IND,
                        case
                        when (PRVDR_CLSFCTN_TYPE_CD='1' or PRVDR_CLSFCTN_TYPE_CD='N') and PRVDR_CLSFCTN_SME=3 then 1
                        when PRVDR_CLSFCTN_TYPE_CD='2' and PRVDR_CLSFCTN_SME=6 then 1
                        when PRVDR_CLSFCTN_TYPE_CD='4' and PRVDR_CLSFCTN_SME=9 then 1
                        when PRVDR_CLSFCTN_CD='.' or PRVDR_CLSFCTN_CD is null or PRVDR_CLSFCTN_TYPE_CD='.' or PRVDR_CLSFCTN_TYPE_CD is null then null
                        when PRVDR_CLSFCTN_TYPE_CD<>'1' and PRVDR_CLSFCTN_TYPE_CD<>'N' and PRVDR_CLSFCTN_TYPE_CD<>'2' and PRVDR_CLSFCTN_TYPE_CD<>'4' then null
                        else 0
                        end as EMER_SRVCS_PRVDR_IND
                        from Prov06_Taxonomies_MHT
                        where PRVDR_CLSFCTN_TYPE_CD is not null and PRVDR_CLSFCTN_CD is not null
                order by { ','.join(self.srtlist) }
            """
        self.prv.append(type(self).__name__, z)

        z = f"""
                create or replace temporary view Prov06_Taxonomies_Mapped as
                select { ','.join(self.srtlist) },
                        max(MLT_SNGL_SPCLTY_GRP_IND) as MLT_SNGL_SPCLTY_GRP_IND,
                        max(ALPTHC_OSTPTHC_PHYSN_IND) as ALPTHC_OSTPTHC_PHYSN_IND,
                        max(BHVRL_HLTH_SCL_SRVC_PRVDR_IND) as BHVRL_HLTH_SCL_SRVC_PRVDR_IND,
                        max(CHRPRCTIC_PRVDR_IND) as CHRPRCTIC_PRVDR_IND,
                        max(DNTL_PRVDR_IND) as DNTL_PRVDR_IND,
                        max(DTRY_NTRTNL_SRVC_PRVDR_IND) as DTRY_NTRTNL_SRVC_PRVDR_IND,
                        max(EMER_MDCL_SRVC_PRVDR_IND) as EMER_MDCL_SRVC_PRVDR_IND,
                        max(EYE_VSN_SRVC_PRVDR_IND) as EYE_VSN_SRVC_PRVDR_IND,
                        max(NRSNG_SRVC_PRVDR_IND) as NRSNG_SRVC_PRVDR_IND,
                        max(NRSNG_SRVC_RLTD_IND) as NRSNG_SRVC_RLTD_IND,
                        max(OTHR_INDVDL_SRVC_PRVDR_IND) as OTHR_INDVDL_SRVC_PRVDR_IND,
                        max(PHRMCY_SRVC_PRVDR_IND) as PHRMCY_SRVC_PRVDR_IND,
                        max(PA_ADVCD_PRCTC_NRSNG_PRVDR_IND) as PA_ADVCD_PRCTC_NRSNG_PRVDR_IND,
                        max(POD_MDCN_SRGRY_SRVCS_IND) as POD_MDCN_SRGRY_SRVCS_IND,
                        max(RESP_DEV_REH_RESTOR_PRVDR_IND) as RESP_DEV_REH_RESTOR_PRVDR_IND,
                        max(SPCH_LANG_HEARG_SRVC_PRVDR_IND) as SPCH_LANG_HEARG_SRVC_PRVDR_IND,
                        max(STDNT_HLTH_CARE_PRVDR_IND) as STDNT_HLTH_CARE_PRVDR_IND,
                        max(TT_OTHR_TCHNCL_SRVC_PRVDR_IND) as TT_OTHR_TCHNCL_SRVC_PRVDR_IND,
                        max(AGNCY_PRVDR_IND) as AGNCY_PRVDR_IND,
                        max(AMB_HLTH_CARE_FAC_PRVDR_IND) as AMB_HLTH_CARE_FAC_PRVDR_IND,
                        max(HOSP_UNIT_PRVDR_IND) as HOSP_UNIT_PRVDR_IND,
                        max(HOSP_PRVDR_IND) as HOSP_PRVDR_IND,
                        max(LAB_PRVDR_IND) as LAB_PRVDR_IND,
                        max(MCO_PRVDR_IND) as MCO_PRVDR_IND,
                        max(NRSNG_CSTDL_CARE_FAC_IND) as NRSNG_CSTDL_CARE_FAC_IND,
                        max(OTHR_NONINDVDL_SRVC_PRVDRS_IND) as OTHR_NONINDVDL_SRVC_PRVDRS_IND,
                        max(RSDNTL_TRTMT_FAC_PRVDR_IND) as RSDNTL_TRTMT_FAC_PRVDR_IND,
                        max(RESP_CARE_FAC_PRVDR_IND) as RESP_CARE_FAC_PRVDR_IND,
                        max(SUPLR_PRVDR_IND) as SUPLR_PRVDR_IND,
                        max(TRNSPRTN_SRVCS_PRVDR_IND) as TRNSPRTN_SRVCS_PRVDR_IND,
                        max(SUD_SRVC_PRVDR_IND) as SUD_SRVC_PRVDR_IND,
                        max(MH_SRVC_PRVDR_IND) as MH_SRVC_PRVDR_IND,
                        max(EMER_SRVCS_PRVDR_IND) as EMER_SRVCS_PRVDR_IND
                        from Prov06_Taxonomies_CNST
                group by { ','.join(self.srtlist) }
            """
        self.prv.append(type(self).__name__, z)

    def build(self, runner: PRV_Runner):
        """
        Build the PRV06 taxonomy segment.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if runner.run_stats_only:
            runner.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        z = f"""
                INSERT INTO {runner.DA_SCHEMA}.taf_prv_tax
                SELECT
                    *
                FROM
                    Prov06_Taxonomies_seg
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
