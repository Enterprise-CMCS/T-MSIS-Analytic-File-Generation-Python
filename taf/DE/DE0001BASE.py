from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner
from taf.TAF_Closure import TAF_Closure


class DE0001BASE(DE):
    """
    Description:  Generate the annual DE segment 001: Base

    Notes:  This program first creates all the non-demographic columns for the base file, using
            the current year data. It then pulls in the demographic columns (for which we also look
            to the prior year if available) for current and prior year, if getprior=1. If getprior=1,
            join the current and prior year demographics and take the last best with current year
            first, then prior. It then joins the non-demographic data to the demographic data.
            Finally, it pulls in the days eligible counts and _SPLMTL flags.
            It then inserts into the permanent table.
    """

    def __init__(self, runner: DE_Runner):
        DE.__init__(self, runner)
        self.de = runner

    def create(self):
        """
        Create the segment.
        """

        self.create_temp()
        self.demographics(self.de.YEAR)
        self.create_base("base")
        # Drop temporary table from DE0002
        self.drop_table("numbers")
        # Drop temporary table from DE0006
        self.drop_table("numbers_two")

    def basecols(self):
        """
        Define base columns.
        """

        z = """
            ,SSN_NUM
            ,BIRTH_DT
            ,DEATH_DT
            ,DCSD_FLAG
            ,AGE_NUM
            ,AGE_GRP_FLAG
            ,SEX_CD
            ,MRTL_STUS_CD
            ,INCM_CD
            ,VET_IND
            ,CTZNSHP_IND
            ,CTZNSHP_VRFCTN_IND
            ,IMGRTN_STUS_CD
            ,IMGRTN_VRFCTN_IND
            ,IMGRTN_STUS_5_YR_BAR_END_DT
            ,OTHR_LANG_HOME_CD
            ,PRMRY_LANG_FLAG
            ,ENGLSH_PRFCNCY_CD
            ,HSEHLD_SIZE_CD
            ,PRGNCY_FLAG_01
            ,PRGNCY_FLAG_02
            ,PRGNCY_FLAG_03
            ,PRGNCY_FLAG_04
            ,PRGNCY_FLAG_05
            ,PRGNCY_FLAG_06
            ,PRGNCY_FLAG_07
            ,PRGNCY_FLAG_08
            ,PRGNCY_FLAG_09
            ,PRGNCY_FLAG_10
            ,PRGNCY_FLAG_11
            ,PRGNCY_FLAG_12
            ,PRGNCY_FLAG_EVR
            ,AMRCN_INDN_ALSK_NTV_IND
            ,ETHNCTY_CD
            ,RACE_ETHNCTY_FLAG
            ,RACE_ETHNCTY_EXP_FLAG
            ,ELGBL_ZIP_CD
            ,ELGBL_CNTY_CD
            ,ELGBL_STATE_CD
            ,ELGBLTY_GRP_CD_01
            ,ELGBLTY_GRP_CD_02
            ,ELGBLTY_GRP_CD_03
            ,ELGBLTY_GRP_CD_04
            ,ELGBLTY_GRP_CD_05
            ,ELGBLTY_GRP_CD_06
            ,ELGBLTY_GRP_CD_07
            ,ELGBLTY_GRP_CD_08
            ,ELGBLTY_GRP_CD_09
            ,ELGBLTY_GRP_CD_10
            ,ELGBLTY_GRP_CD_11
            ,ELGBLTY_GRP_CD_12
            ,ELGBLTY_GRP_CD_LTST
            ,MASBOE_CD_01
            ,MASBOE_CD_02
            ,MASBOE_CD_03
            ,MASBOE_CD_04
            ,MASBOE_CD_05
            ,MASBOE_CD_06
            ,MASBOE_CD_07
            ,MASBOE_CD_08
            ,MASBOE_CD_09
            ,MASBOE_CD_10
            ,MASBOE_CD_11
            ,MASBOE_CD_12
            ,MASBOE_CD_LTST
            ,CARE_LVL_STUS_CD
            ,DEAF_DSBL_FLAG_EVR
            ,BLND_DSBL_FLAG_EVR
            ,DFCLTY_CNCNTRTNG_DSBL_FLAG_EVR
            ,DFCLTY_WLKG_DSBL_FLAG_EVR
            ,DFCLTY_DRSNG_BTH_DSBL_FLAG_EVR
            ,DFCLTY_ERNDS_ALN_DSBL_FLAG_EVR
            ,OTHR_DSBL_FLAG_EVR
            ,MSIS_CASE_NUM
            ,MDCD_ENRLMT_DAYS_01
            ,MDCD_ENRLMT_DAYS_02
            ,MDCD_ENRLMT_DAYS_03
            ,MDCD_ENRLMT_DAYS_04
            ,MDCD_ENRLMT_DAYS_05
            ,MDCD_ENRLMT_DAYS_06
            ,MDCD_ENRLMT_DAYS_07
            ,MDCD_ENRLMT_DAYS_08
            ,MDCD_ENRLMT_DAYS_09
            ,MDCD_ENRLMT_DAYS_10
            ,MDCD_ENRLMT_DAYS_11
            ,MDCD_ENRLMT_DAYS_12
            ,MDCD_ENRLMT_DAYS_YR
            ,CHIP_ENRLMT_DAYS_01
            ,CHIP_ENRLMT_DAYS_02
            ,CHIP_ENRLMT_DAYS_03
            ,CHIP_ENRLMT_DAYS_04
            ,CHIP_ENRLMT_DAYS_05
            ,CHIP_ENRLMT_DAYS_06
            ,CHIP_ENRLMT_DAYS_07
            ,CHIP_ENRLMT_DAYS_08
            ,CHIP_ENRLMT_DAYS_09
            ,CHIP_ENRLMT_DAYS_10
            ,CHIP_ENRLMT_DAYS_11
            ,CHIP_ENRLMT_DAYS_12
            ,CHIP_ENRLMT_DAYS_YR
            ,CHIP_CD_01
            ,CHIP_CD_02
            ,CHIP_CD_03
            ,CHIP_CD_04
            ,CHIP_CD_05
            ,CHIP_CD_06
            ,CHIP_CD_07
            ,CHIP_CD_08
            ,CHIP_CD_09
            ,CHIP_CD_10
            ,CHIP_CD_11
            ,CHIP_CD_12
            ,CHIP_CD_LTST
            ,MDCR_BENE_ID
            ,MDCR_HICN_NUM
            ,STATE_SPEC_ELGBLTY_GRP_01
            ,STATE_SPEC_ELGBLTY_GRP_02
            ,STATE_SPEC_ELGBLTY_GRP_03
            ,STATE_SPEC_ELGBLTY_GRP_04
            ,STATE_SPEC_ELGBLTY_GRP_05
            ,STATE_SPEC_ELGBLTY_GRP_06
            ,STATE_SPEC_ELGBLTY_GRP_07
            ,STATE_SPEC_ELGBLTY_GRP_08
            ,STATE_SPEC_ELGBLTY_GRP_09
            ,STATE_SPEC_ELGBLTY_GRP_10
            ,STATE_SPEC_ELGBLTY_GRP_11
            ,STATE_SPEC_ELGBLTY_GRP_12
            ,STATE_SPEC_ELGBLTY_GRP_LTST
            ,DUAL_ELGBL_CD_01
            ,DUAL_ELGBL_CD_02
            ,DUAL_ELGBL_CD_03
            ,DUAL_ELGBL_CD_04
            ,DUAL_ELGBL_CD_05
            ,DUAL_ELGBL_CD_06
            ,DUAL_ELGBL_CD_07
            ,DUAL_ELGBL_CD_08
            ,DUAL_ELGBL_CD_09
            ,DUAL_ELGBL_CD_10
            ,DUAL_ELGBL_CD_11
            ,DUAL_ELGBL_CD_12
            ,DUAL_ELGBL_CD_LTST
            ,MC_PLAN_TYPE_CD_01
            ,MC_PLAN_TYPE_CD_02
            ,MC_PLAN_TYPE_CD_03
            ,MC_PLAN_TYPE_CD_04
            ,MC_PLAN_TYPE_CD_05
            ,MC_PLAN_TYPE_CD_06
            ,MC_PLAN_TYPE_CD_07
            ,MC_PLAN_TYPE_CD_08
            ,MC_PLAN_TYPE_CD_09
            ,MC_PLAN_TYPE_CD_10
            ,MC_PLAN_TYPE_CD_11
            ,MC_PLAN_TYPE_CD_12
            ,RSTRCTD_BNFTS_CD_01
            ,RSTRCTD_BNFTS_CD_02
            ,RSTRCTD_BNFTS_CD_03
            ,RSTRCTD_BNFTS_CD_04
            ,RSTRCTD_BNFTS_CD_05
            ,RSTRCTD_BNFTS_CD_06
            ,RSTRCTD_BNFTS_CD_07
            ,RSTRCTD_BNFTS_CD_08
            ,RSTRCTD_BNFTS_CD_09
            ,RSTRCTD_BNFTS_CD_10
            ,RSTRCTD_BNFTS_CD_11
            ,RSTRCTD_BNFTS_CD_12
            ,RSTRCTD_BNFTS_CD_LTST
            ,SSDI_IND
            ,SSI_IND
            ,SSI_STATE_SPLMT_STUS_CD
            ,SSI_STUS_CD
            ,BIRTH_CNCPTN_IND
            ,TANF_CASH_CD
            ,TPL_INSRNC_CVRG_IND
            ,TPL_OTHR_CVRG_IND
            ,EL_DTS_SPLMTL
            ,MNGD_CARE_SPLMTL
            ,HCBS_COND_SPLMTL
            ,LCKIN_SPLMTL
            ,LTSS_SPLMTL
            ,MFP_SPLMTL
            ,HH_SPO_SPLMTL
            ,OTHER_NEEDS_SPLMTL
            ,WAIVER_SPLMTL
            ,MISG_ENRLMT_TYPE_IND_01
            ,MISG_ENRLMT_TYPE_IND_02
            ,MISG_ENRLMT_TYPE_IND_03
            ,MISG_ENRLMT_TYPE_IND_04
            ,MISG_ENRLMT_TYPE_IND_05
            ,MISG_ENRLMT_TYPE_IND_06
            ,MISG_ENRLMT_TYPE_IND_07
            ,MISG_ENRLMT_TYPE_IND_08
            ,MISG_ENRLMT_TYPE_IND_09
            ,MISG_ENRLMT_TYPE_IND_10
            ,MISG_ENRLMT_TYPE_IND_11
            ,MISG_ENRLMT_TYPE_IND_12
            ,MISG_ELGBLTY_DATA_IND
            ,ELGBLTY_TRMNTN_RSN_CD_01
            ,ELGBLTY_TRMNTN_RSN_CD_02
            ,ELGBLTY_TRMNTN_RSN_CD_03
            ,ELGBLTY_TRMNTN_RSN_CD_04
            ,ELGBLTY_TRMNTN_RSN_CD_05
            ,ELGBLTY_TRMNTN_RSN_CD_06
            ,ELGBLTY_TRMNTN_RSN_CD_07
            ,ELGBLTY_TRMNTN_RSN_CD_08
            ,ELGBLTY_TRMNTN_RSN_CD_09
            ,ELGBLTY_TRMNTN_RSN_CD_10
            ,ELGBLTY_TRMNTN_RSN_CD_11
            ,ELGBLTY_TRMNTN_RSN_CD_12
            ,ELGBL_AFTR_EOY_IND
            ,FED_PVT_LVL
            ,ELGBLTY_EXTNSN_CD
            ,CNTNUS_ELGBLTY_CD
            ,INCM_STD_CD
            ,ELGBLTY_RDTRMNTN_DT
        """
        return z

    def create_temp(self):
        """
        Create the base segment, pulling in only non-demographic columns for which we DO NOT look to the prior year.
        Set all pregnancy flags to null.
        """

        tblname = "base_nondemo"
        s = f"""
            {DE.last_best(self, 'CTZNSHP_VRFCTN_IND')}
            {DE.last_best(self, 'IMGRTN_STUS_CD')}
            {DE.last_best(self, 'IMGRTN_VRFCTN_IND')}

            ,null :: smallint as PRGNCY_FLAG_01
            ,null :: smallint as PRGNCY_FLAG_02
            ,null :: smallint as PRGNCY_FLAG_03
            ,null :: smallint as PRGNCY_FLAG_04
            ,null :: smallint as PRGNCY_FLAG_05
            ,null :: smallint as PRGNCY_FLAG_06
            ,null :: smallint as PRGNCY_FLAG_07
            ,null :: smallint as PRGNCY_FLAG_08
            ,null :: smallint as PRGNCY_FLAG_09
            ,null :: smallint as PRGNCY_FLAG_10
            ,null :: smallint as PRGNCY_FLAG_11
            ,null :: smallint as PRGNCY_FLAG_12
            ,null :: smallint as PRGNCY_FLAG_EVR

            ,{TAF_Closure.monthly_array(self, incol='ELGBLTY_GRP_CD')}
            {DE.last_best(self, 'ELGBLTY_GRP_CD', outcol='ELGBLTY_GRP_CD_LTST')}
            ,{TAF_Closure.monthly_array(self, incol='MASBOE_CD')}
            {DE.last_best(self, incol='MASBOE_CD', outcol='MASBOE_CD_LTST')}
            {DE.last_best(self, incol='CARE_LVL_STUS_CD')}
            ,{DE.ever_year(self, incol='DEAF_DSBL_FLAG')}
            ,{DE.ever_year(self, incol='BLND_DSBL_FLAG')}
            ,{TAF_Closure.ever_year(incol='DFCLTY_CONC_DSBL_FLAG',outcol='DFCLTY_CNCNTRTNG_DSBL_FLAG_EVR')}
            ,{DE.ever_year(self, incol='DFCLTY_WLKG_DSBL_FLAG')}
            ,{TAF_Closure.ever_year(incol='DFCLTY_DRSNG_BATHG_DSBL_FLAG',outcol='DFCLTY_DRSNG_BTH_DSBL_FLAG_EVR')}
            ,{TAF_Closure.ever_year(incol='DFCLTY_ERRANDS_ALN_DSBL_FLAG',outcol='DFCLTY_ERNDS_ALN_DSBL_FLAG_EVR')}
            ,{DE.ever_year(self, incol='OTHR_DSBL_FLAG')}

            ,{TAF_Closure.monthly_array(self, incol='CHIP_CD')}
            {DE.last_best(self, incol='CHIP_CD', outcol='CHIP_CD_LTST')}

            ,{TAF_Closure.monthly_array(self, incol='STATE_SPEC_ELGBLTY_FCTR_TXT',outcol='STATE_SPEC_ELGBLTY_GRP')}
            {DE.last_best(self, incol='STATE_SPEC_ELGBLTY_FCTR_TXT',outcol='STATE_SPEC_ELGBLTY_GRP_LTST')}
            ,{TAF_Closure.monthly_array(self, incol='DUAL_ELGBL_CD')}
            {DE.last_best(self, incol='DUAL_ELGBL_CD',outcol='DUAL_ELGBL_CD_LTST')}

            {DE.mc_type_rank(self, smonth=1, emonth=2)}

            ,{TAF_Closure.monthly_array(self, incol='RSTRCTD_BNFTS_CD')}
            {DE.last_best(self, incol='RSTRCTD_BNFTS_CD',outcol='RSTRCTD_BNFTS_CD_LTST')}
            {DE.last_best(self, incol='SSDI_IND')}
            {DE.last_best(self, incol='SSI_IND')}
            {DE.last_best(self, incol='SSI_STATE_SPLMT_STUS_CD')}
            {DE.last_best(self, incol='SSI_STUS_CD')}
            {DE.last_best(self, incol='BIRTH_CNCPTN_IND')}
            {DE.last_best(self, incol='TANF_CASH_CD')}
            {DE.last_best(self, incol='TPL_INSRNC_CVRG_IND')}
            {DE.last_best(self, incol='TPL_OTHR_CVRG_IND')}
            {DE.misg_enrlm_type()}
            {DE.last_best(self, incol='AGE_NUM',outcol='AGE_NUM_TEMP')}
            {DE.last_best(self, incol='AGE_GRP_FLAG',outcol=' AGE_GRP_FLAG_TEMP')}
            {DE.last_best(self, incol='ELGBL_AFTR_EOM_IND',outcol='ELGBL_AFTR_EOM_TEMP')}
            {DE.nonmiss_month2(self, incol='msis_ident_num',outcol='BSF_RECORD',var_type=" ")}
            ,{TAF_Closure.monthly_array(self, incol='ELGBLTY_TRMNTN_RSN_CD')}
            {DE.last_best(self, incol='FED_PVT_LVL')}
            {DE.last_best(self, incol='ELGBLTY_EXTNSN_CD')}
            {DE.last_best(self, incol='CNTNUS_ELGBLTY_CD')}
            {DE.last_best(self, incol='INCM_STD_CD')}
            {DE.last_best(self, incol='ELGBLTY_RDTRMNTN_DT')}
            """

        s2 = f"""{DE.mc_type_rank(self, smonth=3, emonth=4)}"""
        s3 = f"""{DE.mc_type_rank(self, smonth=5, emonth=6)}"""
        s4 = f"""{DE.mc_type_rank(self, smonth=7, emonth=8)}"""
        s5 = f"""{DE.mc_type_rank(self, smonth=9, emonth=10)}"""
        s6 = f"""{DE.mc_type_rank(self, smonth=11, emonth=12)}"""

        self.create_temp_table(tblname, self.de.YEAR, subcols=s,
                               subcols2=s2, subcols3=s3, subcols4=s4,
                               subcols5=s5, subcols6=s6)

    def demographics(self, runyear: int):
        """
        Pull in the demographic columns for which we will look to the prior year if data are
        available - if so, pull in the same columns for the prior year and then join to get last/best
        values from current and prior year.
        """

        from taf.TAF_Closure import TAF_Closure

        # Must array all address elements to take the value that aligns with
        # latest non-missing home address 1 (in outer loop)

        DE.create_temp_table(
            self,
            tblname="base_demo",
            inyear=self.de.YEAR,
            subcols=f"""
                {DE.last_best(self, incol='SSN_NUM')}
                {DE.last_best(self, incol='BIRTH_DT')}
                {DE.last_best(self, incol='DEATH_DT')}
                {DE.last_best(self, incol='DCSD_FLAG')}

                {DE.last_best(self, incol='SEX_CD')}
                {DE.last_best(self, incol='MRTL_STUS_CD')}
                {DE.last_best(self, incol='INCM_CD')}
                {DE.last_best(self, incol='VET_IND')}
                {DE.last_best(self, incol='CTZNSHP_IND')}
                {DE.last_best(self, incol='IMGRTN_STUS_5_YR_BAR_END_DT')}
                {DE.last_best(self, incol='OTHR_LANG_HOME_CD')}
                {DE.last_best(self, incol='PRMRY_LANG_FLAG')}
                {DE.last_best(self, incol='ENGLSH_PRFCNCY_CD')}
                {DE.last_best(self, incol='HSEHLD_SIZE_CD')}
                {DE.last_best(self, incol='AMRCN_INDN_ALSK_NTV_IND')}
                {DE.last_best(self, incol='ETHNCTY_CD')}
                {DE.last_best(self, incol='RACE_ETHNCTY_FLAG')}
                {DE.last_best(self, incol='RACE_ETHNCTY_EXP_FLAG')}

                ,{TAF_Closure.monthly_array(self, 'ELGBL_LINE_1_ADR_HOME')}
                ,{TAF_Closure.monthly_array(self, 'ELGBL_LINE_1_ADR_MAIL')}
                ,{TAF_Closure.monthly_array(self, 'ELGBL_ZIP_CD_HOME')}
                ,{TAF_Closure.monthly_array(self, 'ELGBL_CNTY_CD_HOME')}
                ,{TAF_Closure.monthly_array(self, 'ELGBL_STATE_CD_HOME')}
                ,{TAF_Closure.monthly_array(self, 'ELGBL_ZIP_CD_MAIL')}
                ,{TAF_Closure.monthly_array(self, 'ELGBL_CNTY_CD_MAIL')}
                ,{TAF_Closure.monthly_array(self, 'ELGBL_STATE_CD_MAIL')}
                {DE.nonmiss_month2(self, 'ELGBL_LINE_1_ADR_HOME',var_type=" ")}
                {DE.nonmiss_month2(self, 'ELGBL_LINE_1_ADR_MAIL',var_type=" ")}

                {DE.last_best(self,incol='MSIS_CASE_NUM')}
                {DE.last_best(self,incol='MDCR_BENE_ID')}
                {DE.last_best(self,incol='MDCR_HICN_NUM')}
                """,
            outercols=f"""{DE.assign_nonmiss_month(self, 'ELGBL_LINE_1_ADR', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_LINE_1_ADR_HOME', monthval2='ELGBL_LINE_1_ADR_MAIL_MN', incol2='ELGBL_LINE_1_ADR_MAIL')}
                    {DE.assign_nonmiss_month(self, 'ELGBL_ZIP_CD', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_ZIP_CD_HOME',monthval2='ELGBL_LINE_1_ADR_MAIL_MN', incol2='ELGBL_ZIP_CD_MAIL')}
                    {DE.assign_nonmiss_month(self, 'ELGBL_CNTY_CD', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_CNTY_CD_HOME',monthval2='ELGBL_LINE_1_ADR_MAIL_MN', incol2='ELGBL_CNTY_CD_MAIL')}
                    {DE.assign_nonmiss_month(self, 'ELGBL_STATE_CD', 'ELGBL_LINE_1_ADR_HOME_MN', 'ELGBL_STATE_CD_HOME',monthval2='ELGBL_LINE_1_ADR_MAIL_MN', incol2='ELGBL_STATE_CD_MAIL')}
                    """
        )
    pass

    def create_hist_demo(self, tblname, inyear):
        """
        Create the base demo table.
        """

        z = f"""create or replace temporary view {tblname}_{inyear} as
                select
                    c.*
                    ,d.ELGBL_LINE_1_ADR
                from (
                    select   b.submtg_state_cd
                            ,b.msis_ident_num
                            ,b.de_fil_dt
                            ,b.da_run_id
                            ,b.de_link_key
                            ,ssn_num
                            ,birth_dt
                            ,death_dt
                            ,dcsd_flag
                            ,age_num
                            ,age_grp_flag
                            ,sex_cd
                            ,mrtl_stus_cd
                            ,incm_cd
                            ,vet_ind
                            ,ctznshp_ind
                            ,imgrtn_stus_5_yr_bar_end_dt
                            ,othr_lang_home_cd
                            ,prmry_lang_flag
                            ,englsh_prfcncy_cd
                            ,hsehld_size_cd
                            ,amrcn_indn_alsk_ntv_ind
                            ,ethncty_cd
                            ,race_ethncty_flag
                            ,race_ethncty_exp_flag
                            ,elgbl_zip_cd
                            ,elgbl_cnty_cd
                            ,elgbl_state_cd
                            ,msis_case_num
                            ,mdcr_bene_id
                            ,mdcr_hicn_num
                    from
                        max_run_id_de_{inyear} a
                    inner join
                        {self.de.DA_SCHEMA}.taf_ann_de_base b
                    on
                        a.submtg_state_cd = b.submtg_state_cd and
                    a.da_run_id = b.da_run_id
                    where
                        b.misg_elgblty_data_ind = 0
                    order by
                        submtg_state_cd,
                    msis_ident_num
                    ) as c
                    left join
                    {self.de.DA_SCHEMA}.taf_ann_de_cntct_dtls as d
                on
                    c.de_link_key = d.de_link_key
            """
        self.de.append(type(self).__name__, z)

    def gen_bsf_last_dt(self, tot_month, in_year):
        """
        Create BSF last date.
        """

        gen_bsf_last_dt = []
        new_line = '\n\t\t\t'
        for i in list(range(1, 12 + 1)):
            if i <= tot_month:
                if i <= 9:
                    gen_bsf_last_dt.append(f"when BSF_RECORD = '0{i}' then last_day('{in_year}-0{i}-01')".format())
                else:
                    gen_bsf_last_dt.append(f"when BSF_RECORD = '{i}' then last_day('{in_year}-{i}-01')".format())
        return new_line.join(gen_bsf_last_dt)

    def age_date_calculate(self, inyear):
        """
        Calculate age date.
        """

        z = f"""case { self.gen_bsf_last_dt(tot_month=12, in_year=inyear) }
                end as BSF_LAST_DT,
                case when BSF_RECORD = '12' then ELGBL_AFTR_EOM_TEMP
                else 0
                end as ELGBL_AFTR_EOY_IND
            """
        return z

    def create_base(self, tblname):
        """
        Create the base segment, pulling in only non-demographic columns for which we DO NOT look to the prior year.
        Set all pregnancy flags to null.
        """

        if self.de.GETPRIOR == 1:
            cnt = 0
            # for pyear in range(1, self.de.PYEARS + 1):
            for pyear in self.de.PYEARS:
                self.create_hist_demo(tblname="base_demo", inyear=pyear)

                # Now join the above tables together to use prior years if current year is missing, keeping demographics only.
                # For address information, identify year pulled for latest non-null value of ELGBL_LINE_1_ADR.
                # Use that year to then take value for all cols

            z = f"""create or replace temporary view base_demo_{self.de.YEAR}_out1 as
                select
                     c.msis_ident_num
                    ,c.submtg_state_cd

                    {DE.last_best(self, 'SSN_NUM',prior=1)}
                    {DE.last_best(self, 'BIRTH_DT',prior=1)}
                    {DE.last_best(self, 'DEATH_DT',prior=1)}
                    {DE.last_best(self, 'DCSD_FLAG',prior=1)}

                    {DE.last_best(self, 'SEX_CD',prior=1)}
                    {DE.last_best(self, 'MRTL_STUS_CD',prior=1)}
                    {DE.last_best(self, 'INCM_CD',prior=1)}
                    {DE.last_best(self, 'VET_IND',prior=1)}
                    {DE.last_best(self, 'CTZNSHP_IND',prior=1)}
                    {DE.last_best(self, 'IMGRTN_STUS_5_YR_BAR_END_DT',prior=1)}
                    {DE.last_best(self, 'OTHR_LANG_HOME_CD',prior=1)}
                    {DE.last_best(self, 'PRMRY_LANG_FLAG',prior=1)}
                    {DE.last_best(self, 'ENGLSH_PRFCNCY_CD',prior=1)}
                    {DE.last_best(self, 'HSEHLD_SIZE_CD',prior=1)}

                    {DE.last_best(self, 'AMRCN_INDN_ALSK_NTV_IND',prior=1)}
                    {DE.last_best(self, 'ETHNCTY_CD',prior=1)}
                    {DE.last_best(self, 'RACE_ETHNCTY_FLAG',prior=1)}
                    {DE.last_best(self, 'RACE_ETHNCTY_EXP_FLAG',prior=1)}

                    ,case when c.ELGBL_LINE_1_ADR is not null then {self.de.YEAR}"""+' '

            # for pyear in range(1, self.de.PYEARS + 1):
            for pyear in self.de.PYEARS:
                cnt += 1
                z += f"""when p{cnt}.ELGBL_LINE_1_ADR is not null then {pyear}"""+' '

            z += f"""else null
                      end as yearpull

                    {DE.last_best(self, 'MSIS_CASE_NUM',prior=1)}
                    {DE.last_best(self, 'MDCR_BENE_ID',prior=1)}
                    {DE.last_best(self, 'MDCR_HICN_NUM',prior=1)}

                    from base_demo_{self.de.YEAR} c"""+' '
            cnt = 0
            for pyear in self.de.PYEARS:
                cnt += 1
                # for pyear in range(1, self.de.PYEARS + 1):
                z += f"""
                    left join
                    base_demo_{pyear} p{cnt}

                    on c.submtg_state_cd = p{cnt}.submtg_state_cd and
                        c.msis_ident_num = p{cnt}.msis_ident_num"""

            # Now if we do NOT have prior year data, simply rename base_demo_YR to base_demo_out
            self.de.append(type(self).__name__, z)

            z = f"""create or replace temporary view base_demo_{self.de.YEAR}_out as
                    select one.*
                    {DE.address_same_year(self,'ELGBL_ZIP_CD')}
                    {DE.address_same_year(self,'ELGBL_CNTY_CD')}
                    {DE.address_same_year(self,'ELGBL_STATE_CD')}
                    from base_demo_{self.de.YEAR} c
                        left join base_demo_{self.de.YEAR}_out1 one
                        on c.submtg_state_cd = one.submtg_state_cd and
                            c.msis_ident_num = one.msis_ident_num
                """+' '
            cnt = 0
            for pyear in self.de.PYEARS:
                cnt += 1
                z += f"""left join
                    base_demo_{pyear} p{cnt}

                on c.submtg_state_cd = p{cnt}.submtg_state_cd and
                    c.msis_ident_num = p{cnt}.msis_ident_num
                """
            self.de.append(type(self).__name__, z)

        if self.de.GETPRIOR == 0:
            z = f"""alter view base_demo_{self.de.YEAR} rename to base_demo_{self.de.YEAR}_out"""
            self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view base_{self.de.YEAR}_temp as
                select a.*,
                    b.SSN_NUM as SSN_NUM_TEMP,
                    b.BIRTH_DT,
                    b.DEATH_DT,
                    b.DCSD_FLAG,
                    {self.age_date_calculate(inyear=self.de.YEAR)},

                    b.SEX_CD,
                    b.MRTL_STUS_CD,
                    b.INCM_CD,
                    b.VET_IND,
                    b.CTZNSHP_IND,
                    b.IMGRTN_STUS_5_YR_BAR_END_DT,
                    b.OTHR_LANG_HOME_CD,
                    b.PRMRY_LANG_FLAG,
                    b.ENGLSH_PRFCNCY_CD,
                    b.HSEHLD_SIZE_CD,

                    b.AMRCN_INDN_ALSK_NTV_IND,
                    b.ETHNCTY_CD,
                    b.RACE_ETHNCTY_FLAG,
                    b.RACE_ETHNCTY_EXP_FLAG,

                    b.ELGBL_ZIP_CD,
                    b.ELGBL_CNTY_CD,
                    b.ELGBL_STATE_CD,

                    b.MSIS_CASE_NUM,
                    b.MDCR_BENE_ID,
                    b.MDCR_HICN_NUM

                from base_nondemo_{self.de.YEAR} a
                        inner join
                        base_demo_{self.de.YEAR}_out b

                on a.submtg_state_cd = b.submtg_state_cd and
                    a.msis_ident_num = b.msis_ident_num
            """
        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view base_{self.de.YEAR}_temp1 as
                select *,
                    case when AGE_NUM_TEMP is not null then AGE_NUM_TEMP
                        when BIRTH_DT is not null then
                        case when DEATH_DT is not null then floor(datediff(DEATH_DT, BIRTH_DT) /365.25)
                            when BSF_RECORD is not null then floor(datediff(BSF_LAST_DT, BIRTH_DT) /365.25)
                        end
                    else null
                    end as AGE_NUM_RAW
                from base_{self.de.YEAR}_temp
            """
        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view base_{self.de.YEAR}_temp2 as
                select *,
                    case when AGE_NUM_RAW < -1 then null
                        when AGE_NUM_RAW > 125 then 125
                    else AGE_NUM_RAW
                    end as AGE_NUM
                from base_{self.de.YEAR}_temp1
            """
        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view base_{self.de.YEAR} as
                select *,
                    case when AGE_NUM between -1 and 0 then 1
                        when AGE_NUM between 1 and 5 then 2
                        when AGE_NUM between 6 and 14 then 3
                        when AGE_NUM between 15 and 18 then 4
                        when AGE_NUM between 19 and 20 then 5
                        when AGE_NUM between 21 and 44 then 6
                        when AGE_NUM between 45 and 64 then 7
                        when AGE_NUM between 65 and 74 then 8
                        when AGE_NUM between 75 and 84 then 9
                        when AGE_NUM between 85 and 125 then 10
                    else null
                    end as AGE_GRP_FLAG
                from base_{self.de.YEAR}_temp2
            """
        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view enrolled_days_{self.de.YEAR} as
                select coalesce(a.msis_ident_num,b.msis_ident_num) as msis_ident_num
                    ,coalesce(a.submtg_state_cd,b.submtg_state_cd) as submtg_state_cd """
        for mm in range(1, 13):
            m = str(mm).zfill(2)
            z += f""",a.MDCD_ENRLMT_DAYS_{m}
                     ,b.CHIP_ENRLMT_DAYS_{m}
                """
        z += """,a.MDCD_ENRLMT_DAYS_YR
                ,b.CHIP_ENRLMT_DAYS_YR
                ,1 as EL_DTS_SPLMTL
        from MDCD_days_out a
            full outer join
            CHIP_days_out b

        on a.msis_ident_num = b.msis_ident_num and
            a.submtg_state_cd = b.submtg_state_cd"""

        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view base_{self.de.YEAR}_final0 as
            select a.*"""

        for m in range(1, 13):
            m = str(m).zfill(2)
            z += f"""
                ,coalesce(b.MDCD_ENRLMT_DAYS_{m},0) as MDCD_ENRLMT_DAYS_{m}
                ,coalesce(b.CHIP_ENRLMT_DAYS_{m},0) as CHIP_ENRLMT_DAYS_{m}
                """

        z += f""",coalesce(b.MDCD_ENRLMT_DAYS_YR,0) as MDCD_ENRLMT_DAYS_YR
                 ,coalesce(b.CHIP_ENRLMT_DAYS_YR,0) as CHIP_ENRLMT_DAYS_YR
                 ,coalesce(b.EL_DTS_SPLMTL,0) as EL_DTS_SPLMTL

                ,c.MNGD_CARE_SPLMTL

                ,d.WAIVER_SPLMTL

                ,e.MFP_SPLMTL

                ,f.HH_SPO_SPLMTL

                ,g.HCBS_COND_SPLMTL
                ,g.LCKIN_SPLMTL
                ,g.LTSS_SPLMTL
                ,g.OTHER_NEEDS_SPLMTL

            from base_{self.de.YEAR} a
                left join
                enrolled_days_{self.de.YEAR} b

                on a.submtg_state_cd = b.submtg_state_cd and
                a.msis_ident_num = b.msis_ident_num

                inner join
                MNGD_CARE_SPLMTL_{self.de.YEAR} c

                on a.submtg_state_cd = c.submtg_state_cd and
                a.msis_ident_num = c.msis_ident_num

                inner join
                WAIVER_SPLMTL_{self.de.YEAR} d

                on a.submtg_state_cd = d.submtg_state_cd and
                a.msis_ident_num = d.msis_ident_num

                inner join
                MFP_SPLMTL_{self.de.YEAR} e

                on a.submtg_state_cd = e.submtg_state_cd and
                a.msis_ident_num = e.msis_ident_num

                inner join
                HH_SPO_SPLMTL_{self.de.YEAR} f

                on a.submtg_state_cd = f.submtg_state_cd and
                a.msis_ident_num = f.msis_ident_num

                inner join
                DIS_NEED_SPLMTLS_{self.de.YEAR} g

                on a.submtg_state_cd = g.submtg_state_cd and
                a.msis_ident_num = g.msis_ident_num
            """
        self.de.append(type(self).__name__, z)

        # Create a table of all unique state/MSIS IDs from claims, to join back to Base and create dummy records for
        # all benes with a claim and not in Base
        z = f"""create or replace temporary view claims_ids as
                select distinct submtg_state_cd
                                ,msis_ident_num

                from ({DE.unique_claims_ids(self, cltype='IP')}

                    union
                    {DE.unique_claims_ids(self, cltype='LT')}

                    union
                    {DE.unique_claims_ids(self, cltype='OT')}

                    union
                    {DE.unique_claims_ids(self, cltype='RX')}
                )
            """
        self.de.append(type(self).__name__, z)

        z = f"""create or replace temporary view base_{self.de.YEAR}_final as

                select a.*
                    ,coalesce(a.submtg_state_cd,b.submtg_state_cd) as submtg_state_cd_comb
                    ,coalesce(a.msis_ident_num,b.msis_ident_num) as msis_ident_num_comb
                    ,case when a.submtg_state_cd is null then '000000000'
                            else SSN_NUM_TEMP
                            end as SSN_NUM

                    ,case when a.submtg_state_cd is null
                            then 1 else 0
                            end as MISG_ELGBLTY_DATA_IND


                from base_{self.de.YEAR}_final0 a
                    full join
                    claims_ids b

                on a.submtg_state_cd = b.submtg_state_cd and
                a.msis_ident_num = b.msis_ident_num
            """
        self.de.append(type(self).__name__, z)

        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if self.de.run_stats_only:
            self.de.logger.info(f"** {self.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        else:
            z = f"""insert into {self.de.DA_SCHEMA}.taf_ann_de_{tblname}
                (DE_LINK_KEY, DE_FIL_DT, ANN_DE_VRSN, MSIS_IDENT_NUM {self.basecols()}{DE.table_id_cols_sfx(self, extra_cols=[], as_select=True)})
                select
                    cast ({self.de.DA_RUN_ID} || '-' || '{self.de.YEAR}' || '-' || '{self.de.VERSION}' || '-' ||
                    SUBMTG_STATE_CD_comb || '-' || MSIS_IDENT_NUM_comb as varchar(40)) as DE_LINK_KEY
                    ,'{self.de.YEAR}' as DE_FIL_DT
                    ,'{self.de.VERSION}' as ANN_DE_VRSN
                    ,MSIS_IDENT_NUM_comb
                    {self.basecols()}
                    ,from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS
                    ,cast(NULL as timestamp) as REC_UPDT_TS
                    ,{self.de.DA_RUN_ID} as DA_RUN_ID
                    ,SUBMTG_STATE_CD_comb

                from base_{self.de.YEAR}_final
                """

            self.de.append(type(self).__name__, z)


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
