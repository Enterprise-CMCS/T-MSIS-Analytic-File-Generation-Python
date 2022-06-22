from taf.UP.UP_Runner import UP_Runner
from taf.TAF import TAF


# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class UP(TAF):

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, up: UP_Runner):
        self.up = up
        self.year = self.up.reporting_period.year

        self.pyear = self.year - 1
        self.pyear2 = self.year - 2
        self.fyear = self.year + 1

        self.fltypes = ["IP", "LT", "OT", "RX"]
        self.inds1 = ["MDCD", "SCHIP"]
        self.inds2 = ["XOVER", "NON_XOVR"]

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
        self.chipmos = ["nonchip_mdcd", "mchip", "schip"]

        # Create tables with the max da_run_id for all file types for each month for the given state/year
        # When inyear is not specified, current year is run
        # For IP only, must also look to prior and following years (last three of prior and first three of following)
        # Insert into a metadata table to be able to link back to this run

        UP.max_run_id(self, file="DE", tbl="taf_ann_de_base", inyear=self.year)
        UP.max_run_id(self, file="IP", inyear=self.year)
        UP.max_run_id(self, file="IP", inyear=self.pyear)
        UP.max_run_id(self, file="IP", inyear=self.fyear)
        UP.max_run_id(self, file="LT", inyear=self.year)
        UP.max_run_id(self, file="OT", inyear=self.year)
        UP.max_run_id(self, file="RX", inyear=self.year)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def max_run_id(self, file="", tbl="", inyear=""):

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
                FROM {self.up.DA_SCHEMA}.job_cntl_parms
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
                    ,regexp_substr(substring(job_parms_txt, 10), '([0-9]{2})') AS submtg_state_cd
                    ,da_run_id
                FROM {self.up.DA_SCHEMA}.job_cntl_parms
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
            b.submtg_state_cd
                ,max(b.da_run_id) AS da_run_id
            FROM job_cntl_parms_both_{file}_{inyear}a
            INNER JOIN (
                SELECT da_run_id
                    ,incldd_state_cd AS submtg_state_cd
                FROM {self.up.DA_SCHEMA}.efts_fil_meta
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
            INSERT INTO {self.up.DA_SCHEMA}.TAF_ANN_UP_INP_SRC
            SELECT {self.up.DA_RUN_ID} AS ANN_DA_RUN_ID
                ,SUBMTG_STATE_CD
                ,{file}_FIL_DT
                ,DA_RUN_ID AS SRC_DA_RUN_ID
            FROM max_run_id_{file}_{inyear}
        """
        self.up.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    # determine whether a year is a leap year
    #
    # ---------------------------------------------------------------------------------
    @staticmethod
    def is_leap_year(inyear):
        return inyear % 4 == 0 and (inyear % 100 != 0 or inyear % 400 == 0)

    # ---------------------------------------------------------------------------------
    # pull in all the monthly claims files, subsetting to only desired claim types,
    # non-null MSIS ID, and MSIS ID not beginning with '&'. Keep only needed cols.
    #
    # Also create MDCD, SCHIP, NON_XOVR and XOVR indicators to more easily pull in
    # these four combinations of records when
    # counting/summing.
    #
    # For NON_XOVR/XOVR, if the input xovr_ind is null, look to tot_mdcr_ddctbl_amt
    # and tot_mdcr_coinsrnc_amt - if EITHER is > 0, set OXVR=1
    # ---------------------------------------------------------------------------------
    def pullclaims(self, file: str, hcols, lcols, inyear):

        # distkey(&file._link_key)
        # sortkey(&file._link_key)
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW {file}h_{inyear} AS

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
                    WHEN MDCD = 0
                        THEN 1
                    ELSE 0
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
                    WHEN XOVR = 0
                        THEN 1
                    ELSE 0
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
                 {",".join(hcols)}
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
        """
        self.up.append(type(self).__name__, z)

        # join to line level file to get needed cols
        # drop denied lines

        # distkey(&file._link_key)
        # sortkey(&file._link_key)
        z = f"""
            CREATE TEMP TABLE {file}l0_{inyear} AS

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
                 {",".join(hcols)}
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
                 {",".join(hcols)}
            """

        z += f"""
            FROM {file}h_{inyear}a
            LEFT JOIN {file}l0_{inyear} b ON a.{file}_link_key = b.{file}_link_key
                AND a.da_run_id = b.da_run_id
        """
        self.up.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    # select statement to union the four file types, called once per file type,
    # from the header-level rollup (created in 001_up_base_hdr)
    #
    # ---------------------------------------------------------------------------------
    def union_base_hdr(self, file):

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

                        ,typeof(NULL) as {file2}_ffs_mh_pd
                        ,typeof(NULL) as {file2}_ffs_sud_pd
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
                    z += f"""typeof(NULL) as tot_{ind1}_{ind2}_ffs_{file2}_pd"""

                if file.casefold() == file2.casefold():
                    z += f"""tot_{ind1}_{ind2}_ffs_{file2}_pd"""

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
        self.up.append(type(self).__name__, z)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def commoncols_base_line(self) -> str:

        cols = ["submtg_state_cd", "msis_ident_num"]

        for h in range(1, 8):
            cols.append(f"""hcbs_{h}_clm_flag""")

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

        return ",".join(cols)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def table_id_cols(self) -> str:
        return f"""
               {self.up.DA_RUN_ID} AS DA_RUN_ID
                   ,cast(('{self.up.DA_RUN_ID}' || '-' || '{self.year}' || '-' || {self.up.version} || '-' || SUBMTG_STATE_CD || '-' || MSIS_IDENT_NUM) AS VARCHAR(40)) AS UP_LINK_KEY
                   ,'{self.year}' AS UP_FIL_DT
                   ,'{self.up.version}' AS ANN_UP_VRSN
                   ,SUBMTG_STATE_CD
                   ,MSIS_IDENT_NUM
        """

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def join_del_lists(self, file: str, diag_cols, prcdr_cols) -> str:
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW {file}_deliv_{self.year} as

            select submtg_state_cd
                ,msis_ident_num
                ,max(maternal) as maternal_{file}
                ,max(newborn) as newborn_{file}

            from (
                    select submtg_state_cd
                        ,msis_ident_num
        """

        acc = 0
        for diag in diag_cols:
            acc += 1

            z += f"""
                 ,d{acc}newborn as {diag}_newborn
                 ,d{acc}maternal as {diag}_maternal
            """

        acc = 0
        for prcdr in prcdr_cols:
            acc += 1

            z += f"""
                 ,p{acc}newborn as {prcdr}_newborn
                 ,p{acc}maternal as {prcdr}_maternal
            """

        z += f"""
             ,r.newborn as rev_newborn
             ,r.maternal as rev_maternal
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
             end as maternal
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

                        on a.{diag} = d{acc}dgns_cd and
                           a.{diag}_ind = d{acc}dgns_cd_ind
            """

        acc = 0
        for prcdr in prcdr_cols:
            acc += 1
            z += f"""
                        left join
                        prcdr_cd_lookup p{acc}

                        on a.{prcdr} = p{acc}prcdr_cd
            """

        z += f"""
             left join rev_cd_lookup r
                 on a.rev_cd = r.rev_cd
            )

            group by submtg_state_cd
                ,msis_ident_num
        """
        return z
