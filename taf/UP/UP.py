from taf.UP.UP_Runner import UP_Runner
from taf.TAF import TAF


class UP(TAF):
    """
    Annual Use and Payment (UP) TAF: The annual UP TAF contain annual summaries of service use 
    and expenditure measures for all Medicaid and CHIP beneficiaries who had at least one service 
    or capitated/supplemental payment during the calendar year.  Each annual UP TAF is comprised of 
    two files:  a Base file and a Type of Program (TOP) file.  The files can be linked together using 
    unique keys that are constructed based on various data elements.  These annual TAF UP files are 
    created from the annual DE TAF base file (selected data elements) and the monthly TAF IP, LT, OT 
    and RX claims files.  The base file includes selected claims-based data elements for four groups of 
    beneficiaries:  Medicaid non-crossover beneficiaries, S-CHIP non-crossover beneficiaries, Medicaid 
    crossover beneficiaries and S-CHIP crossover beneficiaries.
    """
     
    #def __init__(self, up: UP_Runner):
        #self.up = up

    def __init__(self, runner: UP_Runner):
        """
        TODO:  Update docstring
        """
         
        self.up = runner
        self.year = self.up.reporting_period.year

        self.pyear = self.year - 1
        self.pyear2 = self.year - 2
        self.fyear = self.year + 1

        self.fltypes = ["IP", "LT", "OT", "RX"]
        self.inds1 = ["MDCD", "SCHIP"]
        self.inds2 = ["XOVR", "NON_XOVR"]

        # list of hcbs values corresponding to values of 1-7 to loop over to create indicators
        self.hcbsvals = [
            "1915I",
            "1915J",
            "1915K",
            "1915C",
            "1115",
            "OTHR_ACUTE_CARE",
            "OTHR_LT_CARE",
        ]

        # list of chip_cd values corresponding to values of 1-3 to loop over to create indicators
        #self.chipmos = ["nonchip_mdcd", "mchip", "schip"]

        self.basecols = ["AGE_NUM"
                            ,"GNDR_CD"
                            ,"RACE_ETHNCTY_EXP_FLAG"
                            ,"ELGBLTY_GRP_CD_LTST"
                            ,"MASBOE_CD_LTST"
                            ,"ELGBLTY_NONCHIP_MDCD_MOS" 
                            ,"ELGBLTY_MCHIP_MOS"
                            ,"ELGBLTY_SCHIP_MOS"
                            ,"CHIP_CD_LTST"
                            ,"DUAL_ELGBL_EVR"
                            ,"DUAL_ELGBL_CD_LTST"
                            ,"RCPNT_IND"
                            ,"MISG_ELGBLTY_FLAG"
                            ,"DLVRY_IND"
                            ,"SECT_1115A_DEMO_IND_ANY"
                            ,"HCBS_1915I_CLM_FLAG"
                            ,"HCBS_1915J_CLM_FLAG"
                            ,"HCBS_1915K_CLM_FLAG"
                            ,"HCBS_1915C_CLM_FLAG"
                            ,"HCBS_1115_CLM_FLAG"
                            ,"HCBS_OTHR_ACUTE_CARE_CLM_FLAG"
                            ,"HCBS_OTHR_LT_CARE_CLM_FLAG"
                            ,"IP_MH_DX_IND_ANY"
                            ,"IP_MH_TXNMY_IND_ANY"
                            ,"IP_FFS_MH_CLM"
                            ,"IP_MC_MH_CLM"
                            ,"IP_FFS_MH_PD"
                            ,"IP_SUD_DX_IND_ANY"
                            ,"IP_SUD_TXNMY_IND_ANY"
                            ,"IP_FFS_SUD_CLM"
                            ,"IP_MC_SUD_CLM"
                            ,"IP_FFS_SUD_PD"
                            ,"LT_MH_DX_IND_ANY"
                            ,"LT_MH_TXNMY_IND_ANY"
                            ,"LT_FFS_MH_CLM"
                            ,"LT_MC_MH_CLM"
                            ,"LT_FFS_MH_PD"
                            ,"LT_SUD_DX_IND_ANY"
                            ,"LT_SUD_TXNMY_IND_ANY"
                            ,"LT_FFS_SUD_CLM"
                            ,"LT_MC_SUD_CLM"
                            ,"LT_FFS_SUD_PD"
                            ,"OT_MH_DX_IND_ANY"
                            ,"OT_MH_TXNMY_IND_ANY"
                            ,"OT_FFS_MH_CLM"
                            ,"OT_MC_MH_CLM"
                            ,"OT_FFS_MH_PD"
                            ,"OT_SUD_DX_IND_ANY"
                            ,"OT_SUD_TXNMY_IND_ANY"
                            ,"OT_FFS_SUD_CLM"
                            ,"OT_MC_SUD_CLM"
                            ,"OT_FFS_SUD_PD"
                            ,"MDCD_RCPNT_NON_XOVR_FFS_FLAG"
                            ,"MDCD_NON_XOVR_FFS_IP_STAYS"
                            ,"MDCD_NON_XOVR_FFS_IP_DAYS"
                            ,"MDCD_NON_XOVR_FFS_LT_DAYS"
                            ,"MDCD_NON_XOVR_FFS_OT_CLM"
                            ,"MDCD_NON_XOVR_FFS_RX_CLM"
                            ,"MDCD_RCPNT_NON_XOVR_MC_FLAG"
                            ,"MDCD_NON_XOVR_MC_IP_STAYS"
                            ,"MDCD_NON_XOVR_MC_IP_DAYS"
                            ,"MDCD_NON_XOVR_MC_LT_DAYS" 
                            ,"MDCD_NON_XOVR_MC_OT_CLM"
                            ,"MDCD_NON_XOVR_MC_RX_CLM"
                            ,"TOT_MDCD_NON_XOVR_PD"
                            ,"MDCD_NON_XOVR_PD"
                            ,"MDCD_NON_XOVR_FFS_EQUIV_AMT"
                            ,"TOT_MDCD_NON_XOVR_FFS_IP_PD"
                            ,"TOT_MDCD_NON_XOVR_FFS_LT_PD"
                            ,"TOT_MDCD_NON_XOVR_FFS_OT_PD"
                            ,"TOT_MDCD_NON_XOVR_FFS_RX_PD"
                            ,"MDCD_NON_XOVR_MC_CMPRHNSV_CLM"
                            ,"MDCD_NON_XOVR_MC_PCCM_CLM"
                            ,"MDCD_NON_XOVR_MC_PVT_INS_CLM"
                            ,"MDCD_NON_XOVR_MC_PHP_CLM"
                            ,"MDCD_NON_XOVR_MC_CMPRHNSV_PD"
                            ,"MDCD_NON_XOVR_MC_PCCM_PD"
                            ,"MDCD_NON_XOVR_MC_PVT_INS_PD"
                            ,"MDCD_NON_XOVR_MC_PHP_PD"
                            ,"MDCD_NON_XOVR_SPLMTL_CLM"
                            ,"TOT_MDCD_NON_XOVR_SPLMTL_PD"
                            ,"SCHIP_RCPNT_NON_XOVR_FFS_FLAG"
                            ,"SCHIP_NON_XOVR_FFS_IP_STAYS"
                            ,"SCHIP_NON_XOVR_FFS_IP_DAYS"
                            ,"SCHIP_NON_XOVR_FFS_LT_DAYS"
                            ,"SCHIP_NON_XOVR_FFS_OT_CLM"
                            ,"SCHIP_NON_XOVR_FFS_RX_CLM"
                            ,"SCHIP_RCPNT_NON_XOVR_MC_FLAG"
                            ,"SCHIP_NON_XOVR_MC_IP_STAYS"
                            ,"SCHIP_NON_XOVR_MC_IP_DAYS"
                            ,"SCHIP_NON_XOVR_MC_LT_DAYS"
                            ,"SCHIP_NON_XOVR_MC_OT_CLM"
                            ,"SCHIP_NON_XOVR_MC_RX_CLM"
                            ,"TOT_SCHIP_NON_XOVR_PD"
                            ,"SCHIP_NON_XOVR_PD"
                            ,"SCHIP_NON_XOVR_FFS_EQUIV_AMT"
                            ,"TOT_SCHIP_NON_XOVR_FFS_IP_PD"
                            ,"TOT_SCHIP_NON_XOVR_FFS_LT_PD"
                            ,"TOT_SCHIP_NON_XOVR_FFS_OT_PD"
                            ,"TOT_SCHIP_NON_XOVR_FFS_RX_PD"
                            ,"SCHIP_NON_XOVR_MC_CMPRHNSV_CLM"
                            ,"SCHIP_NON_XOVR_MC_PCCM_CLM"
                            ,"SCHIP_NON_XOVR_MC_PVT_INS_CLM"
                            ,"SCHIP_NON_XOVR_MC_PHP_CLM"
                            ,"SCHIP_NON_XOVR_MC_CMPRHNSV_PD"
                            ,"SCHIP_NON_XOVR_MC_PCCM_PD"
                            ,"SCHIP_NON_XOVR_MC_PVT_INS_PD"
                            ,"SCHIP_NON_XOVR_MC_PHP_PD"
                            ,"SCHIP_NON_XOVR_SPLMTL_CLM"
                            ,"TOT_SCHIP_NON_XOVR_SPLMTL_PD"
                            ,"MDCD_RCPNT_XOVR_FFS_FLAG"
                            ,"MDCD_XOVR_FFS_IP_STAYS"
                            ,"MDCD_XOVR_FFS_IP_DAYS"
                            ,"MDCD_XOVR_FFS_LT_DAYS"
                            ,"MDCD_XOVR_FFS_OT_CLM"
                            ,"MDCD_XOVR_FFS_RX_CLM"
                            ,"MDCD_RCPNT_XOVR_MC_FLAG"
                            ,"MDCD_XOVR_MC_IP_STAYS"
                            ,"MDCD_XOVR_MC_IP_DAYS"
                            ,"MDCD_XOVR_MC_LT_DAYS"
                            ,"MDCD_XOVR_MC_OT_CLM"
                            ,"MDCD_XOVR_MC_RX_CLM"
                            ,"TOT_MDCD_XOVR_PD"
                            ,"MDCD_XOVR_PD"
                            ,"MDCD_XOVR_FFS_EQUIV_AMT"
                            ,"TOT_MDCD_XOVR_FFS_IP_PD"
                            ,"TOT_MDCD_XOVR_FFS_LT_PD"
                            ,"TOT_MDCD_XOVR_FFS_OT_PD"
                            ,"TOT_MDCD_XOVR_FFS_RX_PD"
                            ,"MDCD_XOVR_MC_CMPRHNSV_CLM"
                            ,"MDCD_XOVR_MC_PCCM_CLM"
                            ,"MDCD_XOVR_MC_PVT_INS_CLM"
                            ,"MDCD_XOVR_MC_PHP_CLM"
                            ,"MDCD_XOVR_MC_CMPRHNSV_PD"
                            ,"MDCD_XOVR_MC_PCCM_PD"
                            ,"MDCD_XOVR_MC_PVT_INS_PD"
                            ,"MDCD_XOVR_MC_PHP_PD"
                            ,"SCHIP_RCPNT_XOVR_FFS_FLAG"
                            ,"SCHIP_XOVR_FFS_IP_STAYS"
                            ,"SCHIP_XOVR_FFS_IP_DAYS"
                            ,"SCHIP_XOVR_FFS_LT_DAYS"
                            ,"SCHIP_XOVR_FFS_OT_CLM"
                            ,"SCHIP_XOVR_FFS_RX_CLM"
                            ,"SCHIP_RCPNT_XOVR_MC_FLAG"
                            ,"SCHIP_XOVR_MC_IP_STAYS"
                            ,"SCHIP_XOVR_MC_IP_DAYS"
                            ,"SCHIP_XOVR_MC_LT_DAYS"
                            ,"SCHIP_XOVR_MC_OT_CLM"
                            ,"SCHIP_XOVR_MC_RX_CLM"
                            ,"TOT_SCHIP_XOVR_PD"
                            ,"SCHIP_XOVR_PD"
                            ,"SCHIP_XOVR_FFS_EQUIV_AMT"
                            ,"TOT_SCHIP_XOVR_FFS_IP_PD"
                            ,"TOT_SCHIP_XOVR_FFS_LT_PD"
                            ,"TOT_SCHIP_XOVR_FFS_OT_PD"
                            ,"TOT_SCHIP_XOVR_FFS_RX_PD"
                            ,"SCHIP_XOVR_MC_CMPRHNSV_CLM"
                            ,"SCHIP_XOVR_MC_PCCM_CLM"
                            ,"SCHIP_XOVR_MC_PVT_INS_CLM"
                            ,"SCHIP_XOVR_MC_PHP_CLM"
                            ,"SCHIP_XOVR_MC_CMPRHNSV_PD"
                            ,"SCHIP_XOVR_MC_PCCM_PD"
                            ,"SCHIP_XOVR_MC_PVT_INS_PD"
                            ,"SCHIP_XOVR_MC_PHP_PD"
                            ,"MDCD_NON_XOVR_HH_CLM"
                            ,"MDCD_NON_XOVR_MDCR_CLM"
                            ,"MDCD_NON_XOVR_OTHR_CLM"
                            ,"MDCD_NON_XOVR_HH_PD"
                            ,"MDCD_NON_XOVR_MDCR_PD"
                            ,"MDCD_NON_XOVR_OTHR_PD"
                            ,"MDCD_XOVR_HH_CLM"
                            ,"MDCD_XOVR_MDCR_CLM"
                            ,"MDCD_XOVR_OTHR_CLM"
                            ,"MDCD_XOVR_HH_PD"
                            ,"MDCD_XOVR_MDCR_PD"
                            ,"MDCD_XOVR_OTHR_PD"
                            ,"SCHIP_NON_XOVR_HH_CLM"
                            ,"SCHIP_NON_XOVR_MDCR_CLM"
                            ,"SCHIP_NON_XOVR_OTHR_CLM"
                            ,"SCHIP_NON_XOVR_HH_PD"
                            ,"SCHIP_NON_XOVR_MDCR_PD"
                            ,"SCHIP_NON_XOVR_OTHR_PD"
                            ,"SCHIP_XOVR_HH_CLM"
                            ,"SCHIP_XOVR_MDCR_CLM"
                            ,"SCHIP_XOVR_OTHR_CLM"
                            ,"SCHIP_XOVR_HH_PD"
                            ,"SCHIP_XOVR_MDCR_PD"
                            ,"SCHIP_XOVR_OTHR_PD"
                            ]

        # Create tables with the max da_run_id for all file types for each month for the given state/year
        # When inyear is not specified, current year is run
        # For IP only, must also look to prior and following years (last three of prior and first three of following)
        # Insert into a metadata table to be able to link back to this run
    def create(self):
        """
        TODO:  Update docstring
        """
         
        UP.max_run_id(self, file="DE", tbl="taf_ann_de_base", inyear=self.year)
        UP.max_run_id(self, file="IP", inyear=self.year)
        UP.max_run_id(self, file="IP", inyear=self.pyear)
        UP.max_run_id(self, file="IP", inyear=self.fyear)
        UP.max_run_id(self, file="LT", inyear=self.year)
        UP.max_run_id(self, file="OT", inyear=self.year)
        UP.max_run_id(self, file="RX", inyear=self.year)

        for ipyear in range(self.pyear, int(self.fyear) + 1):
            UP.pullclaims(
                self,
                "IP",
                hcols="ip_mh_dx_ind ip_sud_dx_ind ip_mh_txnmy_ind ip_sud_txnmy_ind admsn_dt dschrg_dt ptnt_stus_cd blg_prvdr_num blg_prvdr_npi_num admtg_dgns_cd admtg_dgns_cd_ind dgns_1_cd dgns_2_cd dgns_3_cd dgns_4_cd dgns_5_cd dgns_6_cd dgns_7_cd dgns_8_cd dgns_9_cd dgns_10_cd dgns_11_cd dgns_12_cd dgns_1_cd_ind dgns_2_cd_ind dgns_3_cd_ind dgns_4_cd_ind dgns_5_cd_ind dgns_6_cd_ind dgns_7_cd_ind dgns_8_cd_ind dgns_9_cd_ind dgns_10_cd_ind dgns_11_cd_ind dgns_12_cd_ind prcdr_1_cd prcdr_2_cd prcdr_3_cd prcdr_4_cd prcdr_5_cd prcdr_6_cd",
                lcols="rev_cd",
                inyear=ipyear,
            )

        UP.pullclaims(
            self,
            "LT",
            hcols="lt_mh_dx_ind lt_sud_dx_ind lt_mh_txnmy_ind lt_sud_txnmy_ind srvc_bgnng_dt admsn_dt srvc_endg_dt dschrg_dt",
        )
        UP.pullclaims(
            self,
            "OT",
            hcols="ot_mh_dx_ind ot_sud_dx_ind ot_mh_txnmy_ind ot_sud_txnmy_ind dgns_1_cd dgns_2_cd dgns_1_cd_ind dgns_2_cd_ind",
            lcols="hcbs_srvc_cd prcdr_cd rev_cd",
        )
        UP.pullclaims(self, "RX")

    def max_run_id(self, file="", tbl="", inyear=""):
        """
        TODO:  Update docstring
        """
         
        if file.casefold() != "de":
            node = file + "H"
        else:
            node = "BSE"

        if not tbl:
            _tbl = tbl
        else:
            _tbl = "taf" + "_" + file + "h"

        if not inyear:
            inyear = self.year

        # For NON state-specific runs (where job_parms_text does not include submtg_state_cd in)
        # pull highest da_run_id by time

        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear}_nat AS

            SELECT {file}_fil_dt
                ,max(da_run_id) AS da_run_id
            FROM (
                SELECT substring(job_parms_txt, 1, 4) || substring(job_parms_txt, 6, 2) AS {file}_fil_dt
                    ,da_run_id
                FROM {self.up.DA_SCHEMA_DC}.job_cntl_parms
                WHERE upper(substring(fil_type, 2)) = "{file}"
                    AND sucsfl_ind = 1
                    AND substring(job_parms_txt, 1, 4) = "{inyear}"
        """

        if inyear == self.pyear:
            z += f"""
                    AND substring(job_parms_txt, 6, 2) IN (
                            '10'
                            ,'11'
                            ,'12'
                            )
            """

        if inyear == self.fyear:
            z += f"""
                    AND substring(job_parms_txt, 6, 2) IN (
                        '01'
                        ,'02'
                        ,'03'
                        )
            """

        z += f"""
                    AND charindex('submtg_state_cd in', regexp_replace(job_parms_txt, '\\s+', ' ')) = 0
                )

            GROUP BY {file}_fil_dt
        """
        self.up.append(type(self).__name__, z)

        # For state-specific runs (where job_parms_text includes submtg_state_cd in)
        # pull highest da_run_id by time and state;

        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear}_ss AS

            SELECT {file}_fil_dt
                ,submtg_state_cd
                ,max(da_run_id) AS da_run_id
            FROM (
                SELECT substring(job_parms_txt, 1, 4) || substring(job_parms_txt, 6, 2) AS {file}_fil_dt
                    ,regexp_extract(substring(job_parms_txt, 10), '([0-9]{2})') AS submtg_state_cd
                    ,da_run_id
                FROM {self.up.DA_SCHEMA_DC}.job_cntl_parms
                WHERE upper(substring(fil_type, 2)) = "{file}"
                    AND sucsfl_ind = 1
                    AND substring(job_parms_txt, 1, 4) = "{inyear}"
        """

        if inyear == self.pyear:
            z += f"""
                 AND substring(job_parms_txt, 6, 2) IN (
                            '10'
                            ,'11'
                            ,'12'
                            )
            """

        if inyear == self.fyear:
            z += f"""
                 AND substring(job_parms_txt, 6, 2) IN (
                        '01'
                        ,'02'
                        ,'03'
                        )
            """

        z += f"""
                    AND charindex('submtg_state_cd in', regexp_replace(job_parms_txt, '\\s+', ' ')) > 0
                )

            GROUP BY {file}_fil_dt
                ,submtg_state_cd
        """
        self.up.append(type(self).__name__, z)

        # Now join the national and state lists by month - take the national run ID if higher than
        # the state-specific, otherwise take the state-specific
        # Must ALSO stack with the national IDs so they are not lost
        # In outer query, get a list of unique IDs to pull

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
                ) c
        """
        self.up.append(type(self).__name__, z)

        # Now join to EFTS data to get table of month/state/run IDs to use for data pull
        # Note must then take the highest da_run_id by state/month (if any state-specific runs
        # were identified as being later than a national run)
        # Note for DE only, strip off month from fil_dt

        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW max_run_id_{file}_{inyear} AS

            SELECT
        """

        if file.casefold() == "de":
            z += f"""
                 substring(a.{file}_fil_dt, 1, 4) AS {file}_fil_dt
            """
        else:
            z += f"""
                a.{file}_fil_dt
            """

        z += f"""
                ,b.submtg_state_cd
                ,max(b.da_run_id) AS da_run_id
                ,max(b.fil_cret_dt) AS fil_cret_dt
            FROM job_cntl_parms_both_{file}_{inyear} a
            INNER JOIN (
                SELECT da_run_id
                    ,incldd_state_cd AS submtg_state_cd
                    ,fil_cret_dt
                FROM {self.up.DA_SCHEMA_DC}.efts_fil_meta
                WHERE incldd_state_cd != 'Missing'
                ) b ON a.da_run_id = b.da_run_id
        """

        if UP_Runner.ST_FILTER(self).count("ALL"):
            z += f"""WHERE {UP_Runner.ST_FILTER(self)}
            """
        z += f"""
            GROUP BY a.{file}_fil_dt
                ,b.submtg_state_cd
        """
        self.up.append(type(self).__name__, z)

        # Insert into metadata table so we keep track of all monthly DA_RUN_IDs (both DE and claims)
        # that go into each annual UP file

        z = f"""
            INSERT INTO {self.up.DA_SCHEMA_DC}.TAF_ANN_INP_SRC
            SELECT 
                 {self.up.DA_RUN_ID} AS ANN_DA_RUN_ID
                ,'aup' as ann_fil_type
                ,SUBMTG_STATE_CD
                ,lower('{file}') as src_fil_type
                ,{file}_FIL_DT as src_fil_dt
                ,DA_RUN_ID AS SRC_DA_RUN_ID
                ,fil_cret_dt as src_fil_creat_dt
            FROM max_run_id_{file}_{inyear}
        """
        self.up.append(type(self).__name__, z)

    @staticmethod
    def is_leap_year(inyear):
        """
        Determine whether a year is a leap year
        """
         
        return inyear % 4 == 0 and (inyear % 100 != 0 or inyear % 400 == 0)

    def pullclaims(self, file: str, hcols="", lcols="", inyear=None):
        """
        Pull in all the monthly claims files, subsetting to only desired claim types,
        non-null MSIS ID, and MSIS ID not beginning with '&'. Keep only needed cols.
        
        Also create MDCD, SCHIP, NON_XOVR and XOVR indicators to more easily pull in
        these four combinations of records when
        counting/summing.
        
        For NON_XOVR/XOVR, if the input xovr_ind is null, look to tot_mdcr_ddctbl_amt
        and tot_mdcr_coinsrnc_amt - if EITHER is > 0, set OXVR=1
        """
         
        if (inyear == None):
            inyear = self.year

        # distkey(&file._link_key)
        # sortkey(&file._link_key)
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW {file}h_{inyear} AS
            SELECT *
            FROM (
                SELECT a.da_run_id
                    ,a.submtg_state_cd
                    ,msis_ident_num
                    ,{file}_link_key
                    ,clm_type_cd
                    ,tot_mdcd_pd_amt
                    ,sect_1115a_demo_ind
                    ,xovr_ind
                    ,pgm_type_cd
                    ,CASE
                        WHEN clm_type_cd IN (
                                '1'
                                ,'2'
                                ,'3'
                                ,'5'
                                )
                            THEN 1
                        ELSE 0
                        END AS MDCD
                    ,CASE
                     WHEN clm_type_cd IN (
                                '1'
                                ,'2'
                                ,'3'
                                ,'5'
                                )
                            THEN 0
                        ELSE 1
                        END AS SCHIP
                    ,CASE
                        WHEN xovr_ind = '1'
                            OR (
                                xovr_ind IS NULL
                                AND (
                                    tot_mdcr_ddctbl_amt > 0
                                    OR tot_mdcr_coinsrnc_amt > 0
                                    )
                                )
                            THEN 1
                        ELSE 0
                        END AS XOVR
                    ,CASE
                        WHEN xovr_ind = '1'
                            OR (
                                xovr_ind IS NULL
                                AND (
                                    tot_mdcr_ddctbl_amt > 0
                                    OR tot_mdcr_coinsrnc_amt > 0
                                    )
                                )
                            THEN 0
                        ELSE 1
                        END AS NON_XOVR
                    ,CASE
                        WHEN clm_type_cd IN (
                                '1'
                                ,'A'
                                )
                            THEN 1
                        ELSE 0
                        END AS FFS
        """

        if hcols:
            z += f""","""
            z += f"""
                 {",".join(hcols.split())}
            """

        z += f"""
             FROM

             max_run_id_{file}_{inyear} a
             INNER JOIN {self.up.DA_SCHEMA}.taf_{file}h b ON a.submtg_state_cd = b.submtg_state_cd
                 AND a.{file}_fil_dt = b.{file}_fil_dt
                 AND a.da_run_id = b.da_run_id
             WHERE clm_type_cd IN (
                     '1'
                     ,'2'
                     ,'3'
                     ,'5'
                     ,'A'
                     ,'B'
                     ,'C'
                     ,'E'
                     )
                AND msis_ident_num IS NOT NULL
                AND substring(msis_ident_num, 1, 1) != '&'
            )
        """
        self.up.append(type(self).__name__, z)

        # join to line level file to get needed cols
        # drop denied lines

        # distkey(&file._link_key)
        # sortkey(&file._link_key)
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW {file}l0_{inyear} AS

            SELECT a.da_run_id
                ,a.submtg_state_cd
                ,b.{file}_link_key
                ,b.tos_cd
                ,b.mdcd_pd_amt
                ,b.mdcd_ffs_equiv_amt
        """

        if file.casefold() in ("ip", "lt"):
            z += f"""
                ,b.srvc_bgnng_dt AS srvc_bgnng_dt_ln
                ,b.srvc_endg_dt AS srvc_endg_dt_ln
            """

        if lcols:
            z += f""","""
            z += f"""
                 {",".join(lcols.split())}
            """

        z += f"""
             FROM

             max_run_id_{file}_{inyear} a
             INNER JOIN {self.up.DA_SCHEMA}.taf_{file}l b ON a.submtg_state_cd = b.submtg_state_cd
                 AND a.{file}_fil_dt = b.{file}_fil_dt
                 AND a.da_run_id = b.da_run_id
             WHERE cll_stus_cd NOT IN (
                     '026'
                     ,'26'
                     ,'087'
                     ,'87'
                     ,'542'
                     ,'585'
                     ,'654'
                     )
                 OR cll_stus_cd IS NULL
        """
        self.up.append(type(self).__name__, z)

        # distkey(&file._link_key)
        # sortkey(&file._link_key)
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW {file}l_{inyear} AS

            SELECT a.*
                ,b.tos_cd
                ,b.mdcd_pd_amt
                ,b.mdcd_ffs_equiv_amt
        """

        if file.casefold() in ("ip", "lt"):
            z += f"""
                ,b.srvc_bgnng_dt_ln
                ,b.srvc_endg_dt_ln
            """

        if lcols:
            z += f""","""
            z += f"""
                 {",".join(lcols.split())}
            """

        z += f"""
            FROM {file}h_{inyear} a
            LEFT JOIN {file}l0_{inyear} b ON a.{file}_link_key = b.{file}_link_key
                AND a.da_run_id = b.da_run_id
        """
        self.up.append(type(self).__name__, z)


    def union_base_hdr(self, file):
        """
        select statement to union the four file types, called once per file type,
        from the header-level rollup (created in 001_up_base_hdr)
        """
         
        z = f"""
            SELECT submtg_state_cd
                ,msis_ident_num
                ,sect_1115a_demo_ind_any
                ,ANY_FFS
                ,ANY_MC
        """

        for ind1 in self.inds1:
            for ind2 in self.inds2:
                z += f"""
                     ,{ind1}_rcpnt_{ind2}_ffs_flag
                     ,{ind1}_rcpnt_{ind2}_mc_flag
                     ,tot_{ind1}_{ind2}_pd
                """

                if ind2.casefold() == "non_xovr":
                    z += f"""
                         ,{ind1}_{ind2}_splmtl_clm
                         ,tot_{ind1}_{ind2}_splmtl_pd
                    """

        # loop over the four file types
        # if the file type is NOT the given type,
        # set element to 0/null, otherwise pull in actual column
        for file2 in self.fltypes:
            if file2.casefold() != "rx":
                if file.casefold() != file2.casefold():
                    z += f"""
                        ,0 as {file2}_mh_dx_ind_any
                        ,0 as {file2}_sud_dx_ind_any
                        ,0 as {file2}_mh_txnmy_ind_any
                        ,0 as {file2}_sud_txnmy_ind_any

                        ,0 as {file2}_ffs_mh_clm
                        ,0 as {file2}_mc_mh_clm
                        ,0 as {file2}_ffs_sud_clm
                        ,0 as {file2}_mc_sud_clm

                        ,NULL as {file2}_ffs_mh_pd
                        ,NULL as {file2}_ffs_sud_pd
                    """
                elif file.casefold() == file2.casefold():
                    z += f"""
                        ,{file2}_mh_dx_ind_any
                        ,{file2}_sud_dx_ind_any
                        ,{file2}_mh_txnmy_ind_any
                        ,{file2}_sud_txnmy_ind_any

                        ,{file2}_ffs_mh_clm
                        ,{file2}_mc_mh_clm
                        ,{file2}_ffs_sud_clm
                        ,{file2}_mc_sud_clm

                        ,{file2}_ffs_mh_pd
                        ,{file2}_ffs_sud_pd
                    """

            for ind1 in self.inds1:
                for ind2 in self.inds2:
                    if file.casefold() != file2.casefold():
                        z += f""",NULL as tot_{ind1}_{ind2}_ffs_{file2}_pd"""

                    if file.casefold() == file2.casefold():
                        z += f""",tot_{ind1}_{ind2}_ffs_{file2}_pd"""

                    # only count claims for OT and RX
                    # IP and LT will be counted when rolling up to visits/days
                    if file2.casefold() in ("ot", "rx"):
                        if file.casefold() != file2.casefold():
                            z += f"""
                                ,0 as {ind1}_{ind2}_ffs_{file2}_clm
                                ,0 as {ind1}_{ind2}_mc_{file2}_clm
                            """
                        elif file.casefold() == file2.casefold():
                            z += f"""
                                ,{ind1}_{ind2}_ffs_{file2}_clm
                                ,{ind1}_{ind2}_mc_{file2}_clm
                            """

        z += f"""
             FROM {file}h_bene_base_{self.year}
        """

        return z

    def commoncols_base_line(self) -> str:
        """
        Macro commoncols_base_line: list of columns to be read in from all four file types from line-level
        rollup to header (created in 003_up_base_line) when unioning all four file types 
        """
         
        cols = ["submtg_state_cd", "msis_ident_num"]

        for h in range(1, 8):
            cols.append(f"""hcbs_{h}_clm_flag""")

        # For four combinations of claims (MDCD non-xover, SCHIP non-xover, MDCD xovr and SCHIP xovr,
        # get the same counts and totals. Loop over INDS1 (MDCD SCHIP) and INDS2 (NON_XOVR XOVR) to assign
        # the four pairs of records

        for ind1 in self.inds1:
            for ind2 in self.inds2:
                cols.append(f"""{ind1}_{ind2}_ANY_MC_CMPRHNSV""")
                cols.append(f"""{ind1}_{ind2}_ANY_MC_PCCM""")
                cols.append(f"""{ind1}_{ind2}_ANY_MC_PVT_INS""")
                cols.append(f"""{ind1}_{ind2}_ANY_MC_PHP""")
                cols.append(f"""{ind1}_{ind2}_PD""")
                cols.append(f"""{ind1}_{ind2}_FFS_EQUIV_AMT""")
                cols.append(f"""{ind1}_{ind2}_MC_CMPRHNSV_PD""")
                cols.append(f"""{ind1}_{ind2}_MC_PCCM_PD""")
                cols.append(f"""{ind1}_{ind2}_MC_PVT_INS_PD""")
                cols.append(f"""{ind1}_{ind2}_MC_PHP_PD""")
                cols.append(f"""{ind1}_{ind2}_MDCR_CLM""")
                cols.append(f"""{ind1}_{ind2}_MDCR_PD""")
                cols.append(f"""{ind1}_{ind2}_OTHR_CLM""")
                cols.append(f"""{ind1}_{ind2}_OTHR_PD""")
                cols.append(f"""{ind1}_{ind2}_HH_CLM""")
                cols.append(f"""{ind1}_{ind2}_HH_PD""")

        return ",".join(cols)

    def table_id_cols(self) -> str:
        """
        Macro table_id_cols to add the 6 cols that are the same across all tables into the final insert select
        statement (DA_RUN_ID, DE_LINK_KEY, DE_FIL_DT, ANN_DE_VRSN, SUBMTG_STATE_CD, MSIS_IDENT_NUM)
        """
        
        return f"""
               {self.up.DA_RUN_ID} AS DA_RUN_ID
                   ,cast(('{self.up.DA_RUN_ID}' || '-' || '{self.year}' || '-' || '{self.up.version}' || '-' || SUBMTG_STATE_CD || '-' || MSIS_IDENT_NUM) AS VARCHAR(40)) AS UP_LINK_KEY
                   ,'{self.year}' AS UP_FIL_DT
                   ,'{self.up.version}' AS ANN_UP_VRSN
                   ,SUBMTG_STATE_CD
                   ,MSIS_IDENT_NUM
        """

    def join_del_lists(self, file: str, diag_cols, prcdr_cols) -> str:
        """
        TODO:  Update docstring
        """

        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW {file}_deliv_{self.year} as

            select submtg_state_cd
                ,msis_ident_num
                ,max(maternal) as maternal_{file}
                ,max(newborn) as newborn_{file}

            from (
                select *
        """

        # Create a final set of indicators that looks at ALL the above cols */
        z += f"""
                   ,case when rev_maternal=1
        """
        for diag in diag_cols:
            z += f"""
                    OR {diag}_maternal = 1
            """

        for prcdr in prcdr_cols:
            z += f"""
                    OR {prcdr}_maternal = 1
            """

        z += f"""
                    then 1 else 0
                    end as maternal
        """

        z += f"""
                    ,case when rev_newborn=1
        """
        for diag in diag_cols:
            z += f"""
                     OR {diag}_newborn = 1
            """

        for prcdr in prcdr_cols:
            z += f"""
                     OR {prcdr}_newborn = 1
            """

        z += f"""
                    then 1 else 0
                    end as newborn
        """

        z += f"""
             from (
                 select submtg_state_cd
                     ,msis_ident_num
        """

        acc = 0
        for diag in diag_cols:
            acc += 1

            z += f"""
                 ,d{acc}.newborn as {diag}_newborn
                 ,d{acc}.maternal as {diag}_maternal
            """

        acc = 0
        for prcdr in prcdr_cols:
            acc += 1

            z += f"""
                 ,p{acc}.newborn as {prcdr}_newborn
                 ,p{acc}.maternal as {prcdr}_maternal
            """

        z += f"""
             ,r.newborn as rev_newborn
             ,r.maternal as rev_maternal
        """

        z += f"""
             from {file}l_{self.year} a
        """

        acc = 0
        for diag in diag_cols:
            acc += 1
            z += f"""
                        left join
                        dgns_cd_lookup d{acc}

                        on a.{diag} = d{acc}.dgns_cd and
                           a.{diag}_ind = d{acc}.dgns_cd_ind
            """

        acc = 0
        for prcdr in prcdr_cols:
            acc += 1
            z += f"""
                        left join
                        prcdr_cd_lookup p{acc}

                        on a.{prcdr} = p{acc}.prcdr_cd
            """

        z += f"""
                 left join rev_cd_lookup r
                     on a.rev_cd = r.rev_cd
                )
            )

            group by submtg_state_cd
                ,msis_ident_num
        """
        return z


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
