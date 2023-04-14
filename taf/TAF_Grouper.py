from taf.TAF import TAF
from taf.TAF_Metadata import TAF_Metadata
from taf.TAF_Closure import TAF_Closure
from taf.TAF_Runner import TAF_Runner


class TAF_Grouper:
    """
    Contains helper functions to facilitate TAF analysis.
    """
     
    otmh9f = [
        "290",
        "291",
        "292",
        "293",
        "294",
        "295",
        "296",
        "297",
        "298",
        "299",
        "300",
        "301",
        "302",
        "306",
        "307",
        "308",
        "309",
        "310",
        "311",
        "312",
        "313",
        "314",
        "315",
        "316",
        "317",
        "318",
        "319",
        ]

    otmh0f = [
        "F01",
        "F02",
        "F03",
        "F04",
        "F05",
        "F06",
        "F07",
        "F08",
        "F09",
        "F20",
        "F21",
        "F22",
        "F23",
        "F24",
        "F25",
        "F26",
        "F27",
        "F28",
        "F29",
        "F30",
        "F31",
        "F32",
        "F33",
        "F34",
        "F35",
        "F36",
        "F37",
        "F38",
        "F39",
        "F40",
        "F41",
        "F42",
        "F43",
        "F44",
        "F45",
        "F46",
        "F47",
        "F48",
        "F49",
        "F50",
        "F51",
        "F52",
        "F53",
        "F54",
        "F55",
        "F56",
        "F57",
        "F58",
        "F59",
        "F60",
        "F61",
        "F62",
        "F63",
        "F64",
        "F65",
        "F66",
        "F67",
        "F68",
        "F69",
        "F70",
        "F71",
        "F72",
        "F73",
        "F74",
        "F75",
        "F76",
        "F77",
        "F78",
        "F79",
        "F80",
        "F81",
        "F82",
        "F83",
        "F84",
        "F85",
        "F86",
        "F87",
        "F88",
        "F89",
        "F90",
        "F91",
        "F92",
        "F93",
        "F94",
        "F95",
        "F96",
        "F97",
        "F98",
        "F99",
        ]

    otsu9f = ["303", "304", "305"]

    otsu0f = ["F10", "F11", "F12", "F13", "F14", "F15", "F16", "F17", "F18", "F19"]

    otmtax = [
        "101Y00000X",
        "101YM0800X",
        "101YP1600X",
        "101YP2500X",
        "101YS0200X",
        "102L00000X",
        "102X00000X",
        "103G00000X",
        "103GC0700X",
        "103K00000X",
        "103T00000X",
        "103TA0700X",
        "103TC0700X",
        "103TC2200X",
        "103TB0200X",
        "103TC1900X",
        "103TE1000X",
        "103TE1100X",
        "103TF0000X",
        "103TF0200X",
        "103TP2701X",
        "103TH0004X",
        "103TH0100X",
        "103TM1700X",
        "103TM1800X",
        "103TP0016X",
        "103TP0814X",
        "103TP2700X",
        "103TR0400X",
        "103TS0200X",
        "103TW0100X",
        "104100000X",
        "1041C0700X",
        "1041S0200X",
        "106E00000X",
        "106H00000X",
        "106S00000X",
        "163WP0808X",
        "163WP0809X",
        "163WP0807X",
        "167G00000X",
        "1835P1300X",
        "2080P0006X",
        "2080P0008X",
        "2084B0040X",
        "2084P0804X",
        "2084F0202X",
        "2084P0805X",
        "2084P0005X",
        "2084P0015X",
        "2084P0800X",
        "225XM0800X",
        "251S00000X",
        "252Y00000X",
        "261QM0801X",
        "261QM0850X",
        "261QM0855X",
        "273R00000X",
        "283Q00000X",
        "320600000X",
        "320900000X",
        "3104A0630X",
        "3104A0625X",
        "310500000X",
        "311500000X",
        "315P00000X",
        "320800000X",
        "320900000X",
        "322D00000X",
        "323P00000X",
        "363LP0808X",
        "364SP0807X",
        "364SP0808X",
        "364SP0809X",
        "364SP0810X",
        "364SP0811X",
        "364SP0812X",
        "364SP0813X",
        "385HR2055X",
        "385HR2060X",
        ]

    otstax = [
        "101YA0400X",
        "103TA0400X",
        "163WA0400X",
        "207LA0401X",
        "207QA0401X",
        "207RA0401X",
        "2084A0401X",
        "2084P0802X",
        "261QM2800X",
        "261QR0405X",
        "276400000X",
        "324500000X",
        "3245S0500X",
        "2083A0300X",
        ]

    def __init__(self, runner: TAF_Runner):
         
        self.runner = runner
        self.rep_yr = runner.reporting_period.year
        self.rep_mo = runner.reporting_period.month

    def mdc(self, MDC: bool, filetyp: str):
        """
        Major diagnostic indicator
        """
         
        select = []
        if MDC:

            select.append(
                f",coalesce(m14.XREF_VAL, m13.XREF_VAL, m12.XREF_VAL, NULL) as MAJOR_DIAGNOSTIC_CATEGORY"
            )

        return "\n".join(select)

    def iap(self, IAP: bool, filetyp: str):
        """
        Helper function to return the IAP_CONDITION_IND as part of a SQL query.  
        """
         
        select = []
        if IAP:

            select.append(f"NULL as IAP_CONDITION_IND")

        return "\n".join(select)

    def phc(self, PHC: bool, filetyp: str):
        """
        Code primary hierarchial conditions.  These variables are mutually exclusive and it takes the first populated value
        """
         
        select = []
        if PHC:
            # these columns are mutex and take on the first populated value
            select.append(
                f",coalesce(h12.XREF_VAL, h13.XREF_VAL, h14.XREF_VAL, h16.XREF_VAL, NULL) as PRIMARY_HIERARCHICAL_CONDITION"
            )

        return "\n".join(select)

    def icd(self, MH_SUD: bool, filetyp: str):
        """
        dgns_cd_ind: 1-> ICD-9, 2-> ICD-10
        """
         
        select = []
        if MH_SUD:

            select.append(
                f",case when (DGNS_1_CD_IND = '1') and (substring (DGNS_1_TEMP, 1, 3) in {tuple(TAF_Grouper.otmh9f)}) then 1 else 0 end as {filetyp}M91"
            )
            select.append(
                f",case when (DGNS_1_CD_IND = '1') and (substring (DGNS_1_TEMP, 1, 3) in {tuple(TAF_Grouper.otsu9f)}) then 1 else 0 end as {filetyp}S91"
            )

            select.append(
                f",case when (DGNS_2_CD_IND = '1') and (substring (DGNS_2_TEMP, 1, 3) in {tuple(TAF_Grouper.otmh9f)}) then 1 else 0 end as {filetyp}M92"
            )
            select.append(
                f",case when (DGNS_2_CD_IND = '1') and (substring (DGNS_2_TEMP, 1, 3) in {tuple(TAF_Grouper.otsu9f)}) then 1 else 0 end as {filetyp}S92"
            )

            select.append(
                f",case when (DGNS_1_CD_IND = '2') and (substring (DGNS_1_TEMP, 1, 3) in {tuple(TAF_Grouper.otmh0f)}) then 1 else 0 end as {filetyp}M01"
            )
            select.append(
                f",case when (DGNS_1_CD_IND = '2') and (substring (DGNS_1_TEMP, 1, 3) in {tuple(TAF_Grouper.otsu0f)}) then 1 else 0 end as {filetyp}S01"
            )

            select.append(
                f",case when (DGNS_2_CD_IND = '2') and (substring (DGNS_2_TEMP, 1, 3) in {tuple(TAF_Grouper.otmh0f)}) then 1 else 0 end as {filetyp}M02"
            )
            select.append(
                f",case when (DGNS_2_CD_IND = '2') and (substring (DGNS_2_TEMP, 1, 3) in {tuple(TAF_Grouper.otsu0f)}) then 1 else 0 end as {filetyp}S02"
            )

        return "\n".join(select)

    def icd_inner(self, MH_SUD: bool, filetyp: str):
        """
        dgns_cd_ind: 1-> ICD-9, 2-> ICD-10
        """

        select = []
        if MH_SUD:

            select.append(",")
            select.append(
                TAF_Closure.var_set_fills(
                    "DGNS_1_CD",
                    cond1="0",
                    cond2="8",
                    cond3="9",
                    cond4="#",
                    new="DGNS_1_TEMP",
                )
            )
            select.append(",")
            select.append(
                TAF_Closure.var_set_fills(
                    "DGNS_2_CD",
                    cond1="0",
                    cond2="8",
                    cond3="9",
                    cond4="#",
                    new="DGNS_2_TEMP",
                )
            )

            return "\n".join(select)

    def select_taxonomy(self, TAXONOMY: bool, filetyp: str):
        """
        Create case-when SQL statements to select taxonomy.  
        """
         
        select = []
        if TAXONOMY:

            select.append(
                f",case when TEMP_TAXONMY is NULL and BLG_PRVDR_NPPES_TXNMY_CD is NULL then null when TEMP_TAXONMY in {tuple(TAF_Grouper.otmtax)} or BLG_PRVDR_NPPES_TXNMY_CD in {tuple(TAF_Grouper.otmtax)} then 1 else 0 end as {filetyp}_MH_TAXONOMY_IND_HDR"
            )
            select.append(
                f",case when TEMP_TAXONMY is NULL and BLG_PRVDR_NPPES_TXNMY_CD is NULL then null when TEMP_TAXONMY in {tuple(TAF_Grouper.otstax)} or BLG_PRVDR_NPPES_TXNMY_CD in {tuple(TAF_Grouper.otstax)} then 1 else 0 end as {filetyp}_SUD_TAXONOMY_IND_HDR"
            )

        return "\n".join(select)

    def select_taxonomy_inner(self, TAXONOMY: bool, filetyp: str):
        """
        Helper function to select taxonomy.  
        """
         
        select = []
        if TAXONOMY:

            select.append(",")
            select.append(
                TAF_Closure.var_set_taxo(
                    "BLG_PRVDR_TXNMY_CD",
                    cond1="8888888888",
                    cond2="9999999999",
                    cond3="000000000X",
                    cond4="999999999X",
                    cond5="NONE",
                    cond6="XXXXXXXXXX",
                    cond7="NO TAXONOMY",
                    new="TEMP_TAXONMY",
                )
            )
            select.append(",")
            select.append(
                TAF_Closure.var_set_taxo(
                    "SELECTED_TXNMY_CD",
                    cond1="8888888888",
                    cond2="9999999999",
                    cond3="000000000X",
                    cond4="999999999X",
                    cond5="NONE",
                    cond6="XXXXXXXXXX",
                    cond7="NO TAXONOMY",
                    new="BLG_PRVDR_NPPES_TXNMY_CD",
                )
            )

        return "\n".join(select)

    def join_MDC(self, MDC: str):
        """
        Join MDC tables.
        """
         
        join = []
        if MDC:

            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF m12 on m12.LKP_VAL=a.drg_cd and {self.rep_yr} <= 2012 and m12.FRMT_NAME_TXT = 'MDC12FM'"
            )
            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF m13 on m13.LKP_VAL=a.drg_cd and {self.rep_yr}  = 2013 and m13.FRMT_NAME_TXT = 'MDC13FM'"
            )
            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF m14 on m14.LKP_VAL=a.drg_cd and {self.rep_yr} >= 2014 and m14.FRMT_NAME_TXT = 'MDC14FM'"
            )

        return "\n".join(join)

    def join_IAP(self, IAP: str):
        """
        Join IAP tables.  
        """
         
        join = []
        if IAP:

            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF i93 on i93.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=3 and a.DGNS_1_CD_IND='1' and i93.FRMT_NAME_TXT = 'IAP93F'"
            )
            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF i94 on i94.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=4 and a.DGNS_1_CD_IND='1' and i94.FRMT_NAME_TXT = 'IAP94F'"
            )
            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF i95 on i95.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=5 and a.DGNS_1_CD_IND='1' and i95.FRMT_NAME_TXT = 'IAP95F'"
            )
            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF i04 on i04.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=4 and a.DGNS_1_CD_IND='2' and i04.FRMT_NAME_TXT = 'IAP04F'"
            )
            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF i05 on i05.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=5 and a.DGNS_1_CD_IND='2' and i05.FRMT_NAME_TXT = 'IAP05F'"
            )
            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF i06 on i06.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=6 and a.DGNS_1_CD_IND='2' and i06.FRMT_NAME_TXT = 'IAP06F'"
            )
            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF i07 on i07.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=7 and a.DGNS_1_CD_IND='2' and i07.FRMT_NAME_TXT = 'IAP07F'"
            )

        return "\n".join(join)

    def join_PHC(self, PHC: str):
        """
        Join PCC tables.
        """
         
        join = []
        if PHC:

            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF h12 on h12.LKP_VAL=a.dgns_1_cd and {self.rep_yr} <= 2012 and h12.FRMT_NAME_TXT = 'HCC12FM'"
            )
            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF h13 on h13.LKP_VAL=a.dgns_1_cd and {self.rep_yr}  = 2013 and h13.FRMT_NAME_TXT = 'HCC13FM'"
            )

            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF h14 on h14.LKP_VAL=a.dgns_1_cd and ({self.rep_yr}   = 2014  or ({self.rep_yr} = 2015 and {self.rep_mo} < 10)) and h14.FRMT_NAME_TXT = 'HCC14FM'"
            )
            join.append(
                f"left join {self.runner.DA_SCHEMA_DC}.FRMT_NAME_XREF h16 on h16.LKP_VAL=a.dgns_1_cd and (({self.rep_yr} >= 2016) or ({self.rep_yr} = 2015 and {self.rep_mo} >=10)) and h16.FRMT_NAME_TXT = 'HCC16FM'"
            )

        return "\n".join(join)

    def mh_sud(self, MH_SUD: bool, filetyp: str):
        """
        Create case-when SQL statements for mh_sud.  
        """
         
        select = []
        if MH_SUD:

            select.append(
                f""",
                case when ((DGNS_1_CD_IND < '1' or DGNS_1_CD_IND > '2' or DGNS_1_CD_IND is NULL or DGNS_1_TEMP is NULL) and
                           (DGNS_2_CD_IND < '1' or DGNS_2_CD_IND > '2' or DGNS_2_CD_IND is NULL or DGNS_2_TEMP is NULL)) then null
                    when ({filetyp}M91+{filetyp}M92+{filetyp}M01+{filetyp}M02 > 0) then 1
                    when ({filetyp}M91+{filetyp}M92 = 0 or {filetyp}M01+{filetyp}M02 = 0) then 0
                end as {filetyp}_MH_DX_IND"""
            )

            select.append(
                f""",
                case when ((DGNS_1_CD_IND < '1' or DGNS_1_CD_IND > '2' or DGNS_1_CD_IND is NULL or DGNS_1_TEMP is NULL) and
                           (DGNS_2_CD_IND < '1' or DGNS_2_CD_IND > '2' or DGNS_2_CD_IND is NULL or DGNS_2_TEMP is NULL)) then null
                    when ({filetyp}S91+{filetyp}S92+{filetyp}S01+{filetyp}S02 > 0) then 1
                    when ({filetyp}S91+{filetyp}S92 = 0 or {filetyp}S01+{filetyp}S02 = 0) then 0
                end as {filetyp}_SUD_DX_IND"""
            )

        return "\n".join(select)

    def taxonomy(self, TAXONOMY: bool, filetyp: str):
        """
        Create case-when statements for taxonomy.  
        """
         
        select = []
        if TAXONOMY:

            select.append(
                f""",
                case when a.{filetyp}_MH_TAXONOMY_IND_HDR is null
                    and l.{filetyp}_MH_TAXONOMY_IND_LINE is null then null
                    when (coalesce(a.{filetyp}_MH_TAXONOMY_IND_HDR,0) + coalesce(l.{filetyp}_MH_TAXONOMY_IND_LINE,0))=2 then 1
                    when a.{filetyp}_MH_TAXONOMY_IND_HDR=1 then 2
                    when l.{filetyp}_MH_TAXONOMY_IND_LINE=1 then 3 else 0 end as {filetyp}_MH_TAXONOMY_IND"""
            )

            select.append(
                f""",
                case when a.{filetyp}_SUD_TAXONOMY_IND_HDR is null
                    and l.{filetyp}_SUD_TAXONOMY_IND_LINE is null then null
                    when (coalesce(a.{filetyp}_SUD_TAXONOMY_IND_HDR,0) + coalesce(l.{filetyp}_SUD_TAXONOMY_IND_LINE,0))=2 then 1
                    when a.{filetyp}_SUD_TAXONOMY_IND_HDR=1 then 2
                    when l.{filetyp}_SUD_TAXONOMY_IND_LINE=1 then 3 else 0 end as {filetyp}_SUD_TAXONOMY_IND"""
            )

        return "\n".join(select)

    def join_taxonomy(self, TAXONOMY: bool, filetyp: str):
        """
        Join taxonomy tables.  
        """
         
        join = []
        if TAXONOMY:

            join.append(
                f"""
                left join {filetyp}_TAXONOMY l
                on
                    l.NEW_SUBMTG_STATE_CD_LINE = a.NEW_SUBMTG_STATE_CD
                and l.ORGNL_CLM_NUM_LINE = a.ORGNL_CLM_NUM
                and l.ADJSTMT_CLM_NUM_LINE = a.ADJSTMT_CLM_NUM
                and l.ADJDCTN_DT_LINE = a.ADJDCTN_DT
                and l.LINE_ADJSTMT_IND = a.ADJSTMT_IND"""
            )

        return "\n".join(join)

    def select_condition_category(self, MDC: str, IAP: str, PHC: str):
        """
        Helper function to generate SQL code for selecting condition category.  
        """
         
        select = []
        if MDC:

            # code Major Diagnostic Indicator
            select.append(
                ",coalesce(m14.XREF_VAL,m13.XREF_VAL,m12.XREF_VAL,null) as MAJOR_DIAGNOSTIC_CATEGORY"
            )

        if IAP:

            # code IAP_CONDITION_IND
            select.append(",NULL as IAP_CONDITION_IND")

        if PHC:

            # code Primary Hierarchical Condition
            # these variables are mutually exclusive and it takes the first populated value
            select.append(
                ",coalesce(h12.XREF_VAL,h13.XREF_VAL,h14.XREF_VAL,h16.XREF_VAL,null) as PRIMARY_HIERARCHICAL_CONDITION"
            )

        return "\n".join(select)

    def AWS_Assign_Grouper_Data_Conv(
        self,
        filetyp,
        clm_tbl,
        line_tbl,
        analysis_date,
        MDC=True,
        IAP=True,
        PHC=True,
        MH_SUD=True,
        TAXONOMY=True,
        ):
  
        z = f"""
            create or replace temporary view {clm_tbl}_STEP1 as
            select
                a.*
            """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ccs_dx.dflt_ccsr_ctgry_ot as dgns_1_ccsr_dflt_ctgry_cd
            """
        else:
            z += f"""
                 ,ccs_dx.dflt_ccsr_ctgry_{filetyp} as dgns_1_ccsr_dflt_ctgry_cd
            """

        z += f"""
                { self.select_condition_category(MDC, IAP, PHC) }
                { self.icd(MH_SUD, filetyp) }
                { self.select_taxonomy(TAXONOMY, filetyp) }

            from (
                select
                    b.*
                    { self.icd_inner(MH_SUD, filetyp) }
                    { self.select_taxonomy_inner(TAXONOMY, filetyp) }
                from
                    {clm_tbl} b
                    left join nppes_npi nppes on nppes.prvdr_npi=b.BLG_PRVDR_NPI_NUM
            ) as a

            left join ccs_dx ccs_dx on ccs_dx.icd_10_cm_cd=a.DGNS_1_CD

            { self.join_MDC(MDC) }
            { self.join_IAP(IAP) }
            { self.join_PHC(PHC) }

        """
        self.runner.append(filetyp, z)

        if TAXONOMY:

            z = f"""
                create or replace temporary view {filetyp}_TAXONOMY as

                select
                    NEW_SUBMTG_STATE_CD_LINE,
                    ORGNL_CLM_NUM_LINE,
                    ADJSTMT_CLM_NUM_LINE,
                    ADJDCTN_DT_LINE,
                    LINE_ADJSTMT_IND

                    ,max(case when TEMP_TAXONMY_LINE is null
            """

            if filetyp.casefold() == "othr_toc":
                z += f"""
                     and SRVCNG_PRVDR_NPPES_TXNMY_CD is null
                """

            z += f"""
                    then null when TEMP_TAXONMY_LINE in { tuple(TAF_Grouper.otmtax) }
            """

            if filetyp.casefold() == "othr_toc":
                z += f"""
                     or SRVCNG_PRVDR_NPPES_TXNMY_CD in { tuple(TAF_Grouper.otmtax) }
                """

            z += f"""then 1 else 0 end) as {filetyp}_MH_TAXONOMY_IND_LINE
                    ,max(case when TEMP_TAXONMY_LINE is null
                 """

            if filetyp.casefold() == "othr_toc":
                z += f"""
                     and SRVCNG_PRVDR_NPPES_TXNMY_CD is null
                """

            z += f"""
                 then null when TEMP_TAXONMY_LINE in { tuple(TAF_Grouper.otstax) }
                 """
            if filetyp.casefold() == "othr_toc":
                z += f"""
                     or SRVCNG_PRVDR_NPPES_TXNMY_CD in { tuple(TAF_Grouper.otstax) }
                """

            z += f"""
                 then 1 else 0 end) as {filetyp}_SUD_TAXONOMY_IND_LINE

                from (
                    select
                        *
                        , { TAF_Closure.var_set_taxo(
                            'srvcng_prvdr_txnmy_cd',
                            cond1='8888888888',
                            cond2='9999999999',
                            cond3='000000000X',
                            cond4='999999999X',
                            cond5='NONE',
                            cond6='XXXXXXXXXX',
                            cond7='NO TAXONOMY',
                            new='TEMP_TAXONMY_LINE') }

                    from
                        {line_tbl}) line

                    group by
                        NEW_SUBMTG_STATE_CD_LINE,
                        ORGNL_CLM_NUM_LINE,
                        ADJSTMT_CLM_NUM_LINE,
                        ADJDCTN_DT_LINE,
                        LINE_ADJSTMT_IND
            """
            self.runner.append(filetyp, z)

        z = f"""
            create or replace temporary view {clm_tbl}_GROUPER as

            select
                a.*
                { self.mh_sud(MH_SUD, filetyp) }
                { self.taxonomy(TAXONOMY, filetyp) }

            from
                {clm_tbl}_STEP1 a

            { self.join_taxonomy(TAXONOMY, filetyp) }

        """
        self.runner.append(filetyp, z)

    def fetch_nppes(self, filetyp: str):
        """
        Helper function that generates SQL transforming national provider identifier
        registry data from CMS.
        """

        z = f"""
            create or replace temporary view taxo_switches as
            select NPI AS prvdr_npi,
        """

        for i in range(1, 15):
            z += f"""concat(nvl(`Healthcare Provider Primary Taxonomy Switch_{i}`,' '),
            """

        z += f"""
                            nvl(`Healthcare Provider Primary Taxonomy Switch_15`,' ')))))))))))))))
                        as sw_positions
        """

        # ------------------------------------------------------------------------------
        #   taxo switches
        # ------------------------------------------------------------------------------

        z += ", length("

        for i in range(1, 15):
            z += f"""concat(nvl(`Healthcare Provider Primary Taxonomy Switch_{i}`,' '),"""
        z += f"""nvl(`Healthcare Provider Primary Taxonomy Switch_15`,' ')))))))))))))))"""

        z += ") - coalesce(length(regexp_replace("

        for i in range(1, 15):
            z += f"""concat(nvl(`Healthcare Provider Primary Taxonomy Switch_{i}`,' '),"""
        z += f"""nvl(`Healthcare Provider Primary Taxonomy Switch_15`,' ')))))))))))))))"""

        z += ", 'Y', '')), 0) as taxo_switches,"
        # ------------------------------------------------------------------------------

        # ------------------------------------------------------------------------------
        #   taxopos
        # ------------------------------------------------------------------------------
        taxopos = f"""
            case when `Healthcare Provider Taxonomy Code_{{0}}` is not null and
                    `Healthcare Provider Taxonomy Code_{{0}}` <> ' ' then 'X'
            """
        # ------------------------------------------------------------------------------
        for i in range(1, 16):
            z += f"""
                {taxopos.format(i)} else ' ' end as taxopos{i},
            """
        # ------------------------------------------------------------------------------

        for i in range(1, 15):
            z += f"""
                 concat({taxopos.format(i)} else ' ' end,
            """

        z += f"""
                    {taxopos.format(15)} else ' ' end))))))))))))))
                    as cd_positions

        """

        # ------------------------------------------------------------------------------
        #   taxo_cds
        # ------------------------------------------------------------------------------

        z += ", length("

        for i in range(1, 15):
            z += f"""concat(nvl({taxopos.format(i)} else ' ' end,' '),"""
        z += f"""nvl({taxopos.format(15)} else ' ' end, ' ')))))))))))))))"""

        z += ") - coalesce(length(regexp_replace("

        for i in range(1, 15):
            z += f"""concat(nvl({taxopos.format(i)} else ' ' end, ' '),"""
        z += f"""nvl({taxopos.format(15)} else ' ' end, ' ')))))))))))))))"""

        z += ", 'X', '')), 0) as taxo_cds,"

        # ------------------------------------------------------------------------------

        for i in range(1, 16):
            z += f"""
                    case when `Healthcare Provider Taxonomy Code_{i}` is not null and
                            `Healthcare Provider Taxonomy Code_{i}` <> ' ' then {i} else null end as taxo{i}
            """
            if i < 15:
                z += ","

        z += f"""
            from taf_python.npidata
        """
        self.runner.append(filetyp, z)

        z = f"""
            create or replace temporary view nppes_npi_step2 as
            select a.NPI as prvdr_npi
                ,taxo_switches
                ,sw_positions
                ,taxo_cds
                ,cd_positions
                ,position('Y' in sw_positions) as primary_switch_position
                ,case when taxo_switches = 1 then
                            slice(array(
        """

        for i in range(1, 15):
            z += f"""nvl(`Healthcare Provider Taxonomy Code_{i}`,' '),
        """

        z += f"""
                                            nvl(`Healthcare Provider Taxonomy Code_15`,' ')),position('Y' in sw_positions),1)
                        when taxo_switches = 0 and taxo_cds = 1 then
                            slice(array(
        """

        for i in range(1, 15):
            z += f"""nvl(`Healthcare Provider Taxonomy Code_{i}`,' '),
        """

        z += f"""
                                            nvl(`Healthcare Provider Taxonomy Code_15`,' ')),position('X' in cd_positions),1)
                        when taxo_switches = 0 and taxo_cds > 1 then null
                        when taxo_switches > 1 then
                            slice(array(
        """
        for i in range(1, 15):
            z += f"""nvl(`Healthcare Provider Taxonomy Code_{i}`,' '),
        """

        z += f"""
             nvl(`Healthcare Provider Taxonomy Code_15`,' ')),least(
        """

        for j in range(1, 14):
            z += f"""
                 taxo{j},
            """

        z += f"""
                 taxo15)-1,1)
                        else null
                    end as selected_txnmy_cdx
        """

        for i in range(1, 16):
            z += f"""
                 ,`Healthcare Provider Taxonomy Code_{i}`
            """

        for i in range(1, 16):
            z += f"""
                 ,taxo{i}
            """

        z += f"""
            from taf_python.npidata a
                inner join
                taxo_switches b
            on	   a.NPI=b.prvdr_npi
            where  b.taxo_cds>0
        """
        self.runner.append(filetyp, z)

        z = f"""
            create or replace temporary view nppes_npi as
                select
                     a.prvdr_npi
                    ,a.selected_txnmy_cd

                    ,case when a.selected_txnmy_cd in ('{ "','".join(TAF_Metadata.vs_ICF_Taxo) }')
                            then 1 else 0 end as prvdr_txnmy_icf

                    ,case when a.selected_txnmy_cd in ('{ "','".join(TAF_Metadata.vs_NF_Taxo) }')
                            then 1 else 0 end as prvdr_txnmy_nf

                    ,case when a.selected_txnmy_cd in ('{ "','".join(TAF_Metadata.vs_Othr_Res_Taxo) }')
                            then 1 else 0 end as prvdr_txnmy_othr_res

                    ,case when a.selected_txnmy_cd in ('{ "','".join(TAF_Metadata.vs_IP_Taxo) }')
                            then 1 else 0 end as prvdr_txnmy_IP
                from (
                    select
                         cast(prvdr_npi as varchar(10)) as prvdr_npi

                        ,case when selected_txnmy_cdx is null then null
                            else substring(array_join(selected_txnmy_cdx,''),1,10)
                            end as selected_txnmy_cd

                from nppes_npi_step2
                ) a
        """
        self.runner.append(filetyp, z)

    def fetch_ccs(self, filetyp: str):
        """
        Helper function that generates case-when SQLs statements to fetch ccs.
        The Clinical Classifications Software Refined (CCSR) aggregates diagnoses
        into clinically meaningful categoires and includes the assignment of a
        default category for both inpatient and outpatient data.
        """

        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW ccs_proc AS
            SELECT explode(split(regexp_replace(`Code Range`, "\'", ""), "-")) AS cd_rng
                ,CCS
                ,CASE
                    WHEN CCS IN ('{ "','".join(TAF_Metadata.vs_Lab_CCS_Cat) }')
                        THEN 'Lab       '
                    WHEN CCS IN ('{ "','".join(TAF_Metadata.vs_Rad_CCS_Cat) }')
                        THEN 'Rad       '
                    WHEN CCS IN ('{ "','".join(TAF_Metadata.vs_DME_CCS_Cat) }')
                        THEN 'DME       '
                    WHEN CCS IN ('{ "','".join(TAF_Metadata.vs_transp_CCS_Cat) }')
                        THEN 'Transprt  '
                    ELSE NULL
                    END AS code_cat
            FROM hcup.ccs_sp_mapping
        """
        self.runner.append(filetyp, z)

        # the assignment of default ccsr categories to tmsis file types
        # is consistent with the sas macro definiton
        # https://github.com/CMSgov/T-MSIS-Analytic-File-Generation-Code/blob/c854e63a3bf692fd3751f65bb2cc22bfc87c24a1/AWS_Shared_Macros.sas#L1521
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW ccs_dx AS
            SELECT
            `ICD-10-CM Code` AS icd_10_cm_cd,
            max(
                case
                when `Inpatient Default CCSR (Y/N/X)` = 'Y' then `CCSR Category`
                else NULL
                end
            ) as dflt_ccsr_ctgry_ip,
            max(
                case
                when `Inpatient Default CCSR (Y/N/X)` = 'Y' then `CCSR Category`
                else NULL
                end
            ) as dflt_ccsr_ctgry_lt,
            max(
                case
                when `Outpatient Default CCSR (Y/N/X)` = 'Y' then `CCSR Category`
                else NULL
                end
            ) as dflt_ccsr_ctgry_ot
            FROM
            hcup.ccsr_dx_mapping
            GROUP BY
            `ICD-10-CM Code`
        """
        self.runner.append(filetyp, z)

    def fasc_code(self, filetyp: str):
        """
        Federally assigned service category
        """
         
        # claim header
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW {filetyp}_header_0 AS
            SELECT submtg_state_cd
                ,msis_ident_num
                ,da_run_id
            """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                ,ot_fil_dt as fil_dt
                ,ot_link_key
                ,ot_vrsn as vrsn
                ,'ot' as file_type
            """
        else:
            z += f"""
                ,{filetyp}_fil_dt as fil_dt
                ,{filetyp}_link_key
                ,{filetyp}_vrsn as vrsn
                ,"{filetyp}" as file_type
            """

        z += f"""
                ,clm_type_cd
                ,srvc_trkng_type_cd
                ,srvc_trkng_pymt_amt
                ,blg_prvdr_txnmy_cd
        """

        if filetyp.casefold() == "ip":
            z += f"""
                 ,mdcd_dsh_pd_amt
                 ,CASE
                     WHEN (
                             mdcd_dsh_pd_amt IS NOT NULL
                             AND mdcd_dsh_pd_amt != 0
                             )
                         THEN 1
                     ELSE 0
                     END AS non_msng_dsh_pd
            """

        z += f"""
             ,tot_mdcd_pd_amt
             ,blg_prvdr_npi_num
        """

        if filetyp.casefold() == "rx":
            z += f"""
                 ,cmpnd_drug_ind
            """

        if filetyp.casefold() != "rx":
            z += f"""
                 ,dgns_1_cd
                 ,dgns_2_cd
                 ,bill_type_cd
                 ,CASE
                     WHEN length(bill_type_cd) = 3
                         AND substring(bill_type_cd, 1, 1) != '0'
                         THEN '0' || substring(bill_type_cd, 1, 3)
                     WHEN length(bill_type_cd) = 4
                         AND bill_type_cd NOT IN ('0000')
                         THEN bill_type_cd
                     ELSE NULL
                     END AS bill_type_cd_upd
                 ,CASE
                     WHEN dgns_1_cd IS NULL
                         THEN 1
                     ELSE 0
                     END AS dgns_1_cd_null
            """

        if filetyp.casefold() == "othr_toc":
            z += f"""
                 ,srvc_plc_cd
            """

        z += f"""
             ,num_cll
             ,CASE
                 WHEN clm_type_cd IN (
                         '1'
                         ,'A'
                         ,'U'
                         )
                     THEN '1_FFS'
                 WHEN clm_type_cd IN (
                         '2'
                         ,'B'
                         ,'V'
                         )
                     THEN '2_CAP'
                 WHEN clm_type_cd IN (
                         '3'
                         ,'C'
                         ,'W'
                         )
                     THEN '3_ENC'
                 WHEN clm_type_cd IN (
                         '4'
                         ,'D'
                         ,'X'
                         )
                     THEN '4_SRVC_TRKG'
                 WHEN clm_type_cd IN (
                         '5'
                         ,'E'
                         ,'Y'
                         )
                     THEN '5_SUPP'
                 ELSE NULL
                 END AS clm_type_grp_ctgry
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 FROM oth
            """
        else:
            z += f"""
                FROM {filetyp}h
            """
        self.runner.append(filetyp, z)

        # service line
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW {filetyp}_LNE AS
            SELECT submtg_state_cd
                ,msis_ident_num
                ,da_run_id
                ,line_num
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                ,ot_fil_dt as fil_dt
                ,ot_link_key
                ,ot_vrsn as vrsn
            """
        else:
            z += f"""
                ,{filetyp}_fil_dt as fil_dt
                ,{filetyp}_link_key
                ,{filetyp}_vrsn as vrsn
            """

        z += f"""
                ,mdcd_pd_amt
                ,xix_srvc_ctgry_cd
                ,tos_cd
                ,xxi_srvc_ctgry_cd
                ,bnft_type_cd
        """

        if filetyp.casefold() != "rx":
            z += f"""
                 ,srvcng_prvdr_txnmy_cd
                 ,rev_cd
                 ,min(CASE
                         WHEN rev_cd IS NULL
                             THEN 1
                         ELSE 0
                         END) OVER (
                     PARTITION BY submtg_state_cd
            """

            if (filetyp.casefold() == 'othr_toc'):
                z += f"""
                     ,ot_link_key
                """
            else:
                z += f"""
                     ,{filetyp}_link_key
                """

            z += f"""
                     ) AS all_null_rev_cd
                 ,max(CASE
                         WHEN rev_cd IN (
                                 '0510'
                                ,'0511'
                                ,'0512'
                                ,'0513'
                                ,'0514'
                                ,'0515'
                                ,'0516'
                                ,'0517'
                                ,'0518'
                                ,'0519'
                                ,'0520'
                                ,'0521'
                                ,'0522'
                                ,'0523'
                                ,'0524'
                                ,'0525'
                                ,'0526'
                                ,'0527'
                                ,'0528'
                                ,'0529'
                                )
                             THEN 1
                         ELSE 0
                         END) OVER (
                     PARTITION BY submtg_state_cd
            """

            if (filetyp.casefold() == 'othr_toc'):
                z += f"""
                     ,ot_link_key
                """
            else:
                z += f"""
                     ,{filetyp}_link_key
                """

            z += f"""
                     ) AS ever_clinic_rev
                 ,max(CASE
                         WHEN rev_cd IN (
                                 '0650'
                                ,'0651'
                                ,'0652'
                                ,'0653'
                                ,'0654'
                                ,'0655'
                                ,'0656'
                                ,'0657'
                                ,'0658'
                                ,'0659'
                                ,'0115'
                                ,'0125'
                                ,'0135'
                                ,'0145'
                                 )
                             THEN 1
                         ELSE 0
                         END) OVER (
                     PARTITION BY submtg_state_cd
            """

            if (filetyp.casefold() == 'othr_toc'):
                z += f"""
                     ,ot_link_key
                """
            else:
                z += f"""
                     ,{filetyp}_link_key
                """

            z += f"""
                     ) AS ever_hospice_rev
                 ,min(CASE
                         WHEN rev_cd IN { tuple(TAF_Metadata.vs_HH_Rev_cd) }
                             THEN 1
                         WHEN rev_cd IS NOT NULL
                             THEN 0
                         ELSE NULL
                         END) OVER (
                     PARTITION BY submtg_state_cd
            """

            if (filetyp.casefold() == 'othr_toc'):
                z += f"""
                     ,ot_link_key
                """
            else:
                z += f"""
                     ,{filetyp}_link_key
                """

            z += f"""
                     ) AS only_hh_rev
            """

        if filetyp.casefold() == "othr_toc":
            z += f"""
                 ,hcbs_txnmy
                 ,prcdr_cd
                 ,hcpcs_rate
                 ,srvcng_prvdr_num
                 ,srvcng_prvdr_npi_num
                 ,min(CASE
                         WHEN prcdr_cd IS NULL
                             THEN 1
                         ELSE 0
                         END) OVER (
                     PARTITION BY submtg_state_cd
                     ,ot_link_key
                     ) AS all_null_prcdr_cd
                 ,min(CASE
                         WHEN hcpcs_rate IS NULL
                             THEN 1
                         ELSE 0
                         END) OVER (
                     PARTITION BY submtg_state_cd
                     ,ot_link_key
                     ) AS all_null_hcpcs_cd
                 ,max(CASE
                         WHEN prcdr_cd IS NULL
                             AND hcpcs_rate IS NULL
                             THEN 1
                         ELSE 0
                         END) OVER (
                     PARTITION BY submtg_state_cd
                     ,ot_link_key
                     ) AS ever_null_prcdr_hcpcs_cd
                 ,max(CASE
                         WHEN prcdr_cd IS NOT NULL
                             OR hcpcs_rate IS NOT NULL
                             THEN 1
                         ELSE 0
                        END) OVER (
                     PARTITION BY submtg_state_cd
                     ,ot_link_key
                     ) AS ever_valid_prcdr_hcpcs_cd
                 ,min(CASE
                         WHEN prcdr_cd IN { tuple(TAF_Metadata.vs_HH_Proc_cd) }
                             OR hcpcs_rate IN { tuple(TAF_Metadata.vs_HH_Proc_cd) }
                             THEN 1
                         WHEN prcdr_cd IS NULL
                             AND hcpcs_rate IS NULL
                             THEN NULL
                         ELSE 0
                         END) OVER (
                     PARTITION BY submtg_state_cd
                     ,ot_link_key
                     ) AS only_hh_procs
            """

        if filetyp.casefold() == "rx":
            z += f"""
            ,ndc_cd
            ,min(CASE
                WHEN ndc_cd IS NULL
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
            ,{filetyp}_link_key
            ) AS all_null_ndc_cd
            ,max(CASE
                WHEN (
                        (
                            length(ndc_cd) = 10
                            OR length(ndc_cd) = 11
                            )
                        AND ndc_cd not rlike '([^0-9])'
                        )
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
            ,{filetyp}_link_key
            ) AS ever_valid_ndc
            """

        z += f"""
        ,max(CASE
                WHEN bnft_type_cd IN ('039')
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_icf_bnft_typ
        ,max(CASE
                WHEN tos_cd IN (
                        '123'
                        ,'131'
                        ,'135'
                        )
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_dsh_drg_ehr_tos
        ,max(CASE
                WHEN tos_cd IN (
                        '119'
                        ,'120'
                        ,'121'
                        ,'122'
                        )
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_cap_pymt_tos
        ,max(CASE
                WHEN tos_cd IN (
                        '119'
                        ,'120'
                        ,'121'
                        ,'122'
                        ,'138'
                        ,'139'
                        ,'140'
                        ,'141'
                        ,'142'
                        ,'143'
                        ,'144'
                        )
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_cap_tos
        ,max(CASE
                WHEN tos_cd IN (
                        '119'
                        ,'122'
                        )
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_php_tos
        ,max(CASE
                WHEN tos_cd IN ('123')
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_dsh_tos
        ,max(CASE
                WHEN tos_cd IN ('131')
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_drg_rbt_tos
        ,max(CASE
                WHEN tos_cd IN (
                        '036'
                        ,'018'
                        )
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_dme_hhs_tos
        ,max(CASE
                WHEN xix_srvc_ctgry_cd IN (
                        '001B'
                        ,'002B'
                        )
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_dsh_xix_srvc_ctgry
        ,max(CASE
                WHEN xix_srvc_ctgry_cd IN (
                        '07A1'
                        ,'07A2'
                        ,'07A3'
                        ,'07A4'
                        ,'07A5'
                        ,'07A6'
                        )
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_othr_fin_xix_srvc_ctgry
        ,max(CASE
                WHEN CLL_STUS_CD IN (
                        '542'
                        ,'585'
                        ,'654'
                        )
                    THEN 1
                ELSE 0
                END) OVER (
            PARTITION BY submtg_state_cd
        """

        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 ,ot_link_key
            """
        else:
            z += f"""
                 ,{filetyp}_link_key
            """

        z += f"""
            ) AS ever_denied_line
        """
        if (filetyp.casefold() == 'othr_toc'):
            z += f"""
                 FROM OTL
            """
        else:
            z += f"""
                 FROM {filetyp}L
            """
        self.runner.append(filetyp, z)

        # combine claim header and line
        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW {filetyp}_COMBINED AS
            SELECT
                b.*
                ,case when { TAF_Closure.misslogic("msis_ident_num","len(trim(msis_ident_num))") } then 1 else 0 end as inval_msis_id
                ,case when cmc_php =0  and
                                other_pmpm =0 and
                                dsh_flag =0  and
                                clm_type_cd is null and
                                ever_cap_pymt_tos=1
                    then 1 else 0 end as cap_tos_null_toc

                ,case when cmc_php =0  and
                                other_pmpm =0 and
                                dsh_flag =0  and
                                clm_type_cd in ('4','D','X')
                    then 1 else 0 end as srvc_trkg

                ,case when cmc_php =0  and
                                other_pmpm =0 and
                                dsh_flag =0  and
                                clm_type_cd in ('5','E','Y') and
                            ( ( { TAF_Closure.misslogic("msis_ident_num","len(trim(msis_ident_num))") } )
        """

        if filetyp.casefold() in ("lt", "ip"):
            z += f"""
                 or (bill_type_cd_upd is null and
                     dgns_1_cd is null)
            """
        elif filetyp.casefold() == "othr_toc":
            z += f"""
                 or (all_null_rev_cd =1 and
                  all_null_prcdr_cd=1 and
                  all_null_hcpcs_cd =1)
            """
        elif filetyp.casefold() == "rx":
            z += f"""
                 or (all_null_ndc_cd =1)
            """

        z += f"""
                        ) then 1 else 0 end as supp_clms

                        ,row_number() over (partition by submtg_state_cd,
             """

        if filetyp.casefold() == "othr_toc":
            z += f"""
                                                        ot_link_key
                                order by submtg_state_cd,ot_link_key)
            """
        else:
            z += f"""
                                                        {filetyp}_link_key
                                order by submtg_state_cd,{filetyp}_link_key)
            """

        z += f"""
                                as rec_cnt
            from (select
                    a.*
        """

        if filetyp.casefold() != "rx":

            z += f"""
                ,case when cmc_php =0  and
                other_pmpm =0 and
                ((clm_type_cd in ('4','D','X','5','E','Y') and
                (srvc_trkng_type_cd in ('02') or
                    ever_dsh_tos=1))
                or

                (clm_type_cd in ('4','X','5','Y') and
                    ever_dsh_xix_srvc_ctgry=1)
            """
            
            if filetyp.casefold() == "ip":
                z += f"""
                    or
                    (clm_type_cd in ('1') and
                    (mdcd_dsh_pd_amt is not null or mdcd_dsh_pd_amt !=0 ) and
                    ever_dsh_xix_srvc_ctgry=1
                    )
                """

            z += f"""
                )
                then 1 else 0 end as dsh_flag
            """

        if filetyp.casefold() == "rx":
            z += f"""
                ,0 as dsh_flag
            """

        z += f"""
                from ( select
                        h.*
                        ,l.line_num
                        ,l.mdcd_pd_amt
                        ,l.xix_srvc_ctgry_cd
                        ,l.tos_cd
                        ,l.xxi_srvc_ctgry_cd
                        ,l.bnft_type_cd
                        ,l.ever_icf_bnft_typ
        """

        if filetyp.casefold() != "rx":
            z += f"""
                        ,l.rev_cd
                        ,l.all_null_rev_cd
                        ,case when bill_type_cd_upd is null then 1 else 0 end as upd_bill_type_cd_null
                        ,substring(bill_type_cd_upd,2,1) as bill_typ_byte2
                        ,substring(bill_type_cd_upd,3,1) as bill_typ_byte3
                        ,l.ever_clinic_rev
                        ,l.ever_hospice_rev
                        ,l.only_hh_rev
                        ,l.srvcng_prvdr_txnmy_cd
            """
        if filetyp.casefold() == "othr_toc":
            z += f"""
                        ,case when h.srvc_plc_cd is null then 1 else 0 end as srvc_plc_cd_null
                        ,l.hcbs_txnmy
                        ,l.prcdr_cd
                        ,l.hcpcs_rate
                        ,l.all_null_prcdr_cd
                        ,l.all_null_hcpcs_cd
                        ,l.ever_null_prcdr_hcpcs_cd
                        ,l.ever_valid_prcdr_hcpcs_cd
                        ,l.srvcng_prvdr_num
                        ,l.srvcng_prvdr_npi_num
                        ,l.only_hh_procs
            """
        if filetyp.casefold() == "rx":
            z += f"""
                        ,l.ndc_cd
                        ,l.all_null_ndc_cd
                        ,l.ever_valid_ndc
            """

        z += f"""
                        ,l.ever_dsh_drg_ehr_tos
                        ,l.ever_cap_tos
                        ,l.ever_cap_pymt_tos
                        ,l.ever_php_tos
                        ,l.ever_dsh_tos
                        ,l.ever_drg_rbt_tos
                        ,l.ever_dme_hhs_tos
                        ,l.ever_dsh_xix_srvc_ctgry
                        ,l.ever_othr_fin_xix_srvc_ctgry
                        ,l.ever_denied_line
        """

        # cmc php and pmpm
        z += f"""
                        ,case when (srvc_trkng_type_cd not in ('01') or srvc_trkng_type_cd is null) and
        """

        if filetyp.casefold() == "othr_toc":
            z += f"""
                                    (
                                        ( clm_type_cd in ('2','B','V') and
                                        (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
                                        ) OR
                                        ( clm_type_cd in ('4','D','X','5','E','Y') and
                                        (ever_cap_tos=1)
                                        )
                                        ) and
            """
        elif filetyp.casefold() in ("lt", "ip"):
            z += f"""
                                        ( clm_type_cd in ('2','B','V') and
                                        (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
                                        ) and
                                    (bill_type_cd_upd is null and dgns_1_cd is null ) and
            """
        elif filetyp.casefold() == "rx":
            z += f"""
                                        ( clm_type_cd in ('2','B','V') and
                                        (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
                                        ) and
                                        (cmpnd_drug_ind is null and all_null_ndc_cd=1 ) and
            """

        z += f"""
                                    (ever_php_tos=1)
                                then 1 else 0 end as cmc_php


                            ,case when (srvc_trkng_type_cd not in ('01') or srvc_trkng_type_cd is null) and
        """

        if filetyp.casefold() == "othr_toc":
            z += f"""
                                        (
                                        ( clm_type_cd in ('2','B','V') and
                                            (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
                                            ) OR
                                            ( clm_type_cd in ('4','D','X','5','E','Y') and
                                            (ever_cap_tos=1)
                                            )
                                        ) and
            """

        if filetyp.casefold() in ("lt", "ip"):
            z += f"""
                                        ( ( clm_type_cd in ('2','B','V') and
                                            (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
                                        ) and
                                        (bill_type_cd_upd is null and dgns_1_cd is null )
                                        ) and
            """
        elif filetyp.casefold() == "rx":
            z += f"""
                                        ( ( clm_type_cd in ('2','B','V') and
                                        (ever_dsh_drg_ehr_tos=0 or ever_dsh_drg_ehr_tos is null)
                                        ) and
                                        (cmpnd_drug_ind is null and all_null_ndc_cd=1 )
                                        ) and
            """

        z += f"""
                                        (ever_php_tos=0 or ever_php_tos is null)

                                    then 1 else 0 end as other_pmpm

                from {filetyp}_header_0 h
                        left join
                        {filetyp}_lne l

                on h.submtg_state_cd=l.submtg_state_cd and
        """

        if filetyp.casefold() == 'othr_toc':
            z += f"""
                 h.ot_link_key=l.ot_link_key
            """
        else:
            z += f"""
                 h.{filetyp}_link_key=l.{filetyp}_link_key
            """

        z += f"""
             ) a ) b
        """
        self.runner.append(filetyp, z)

        # create columns for non financial claims
        z = f"""
            create or replace temporary view {filetyp}_LNE_FLAG_TOS_CAT as
            select *
        """

        if filetyp.casefold() == "othr_toc":
            z += f"""
                    ,max(dental_lne_clms) over (partition by submtg_state_cd,ot_link_key) as dental_clms
                    ,sum(dental_lne_clms) over (partition by submtg_state_cd,ot_link_key) as dental_lne_cnts

                    ,max(trnsprt_lne_clms) over (partition by submtg_state_cd,ot_link_key) as trnsprt_clms
                    ,sum(trnsprt_lne_clms) over (partition by submtg_state_cd,ot_link_key) as trnsprt_lne_cnts

                    ,max(othr_hcbs_lne_clms) over (partition by submtg_state_cd,ot_link_key) as othr_hcbs_clms
                    ,sum(othr_hcbs_lne_clms) over (partition by submtg_state_cd,ot_link_key) as othr_hcbs_lne_cnts

                    ,min(case when Lab_lne_clms=1 then 1
                            when prcdr_cd is null and hcpcs_rate is null then null
                            else 0 end) over (partition by submtg_state_cd,ot_link_key) as Lab_clms

                    ,sum(Lab_lne_clms) over (partition by submtg_state_cd,ot_link_key) as Lab_lne_cnts

                    ,min(case when Rad_lne_clms=1 then 1
                            when prcdr_cd is null and hcpcs_rate is null then null
                            else 0 end) over (partition by submtg_state_cd,ot_link_key) as Rad_clms

                    ,sum(Rad_lne_clms) over (partition by submtg_state_cd,ot_link_key) as Rad_lne_cnts

                    ,max(DME_lne_clms) over (partition by submtg_state_cd,ot_link_key) as DME_clms
                    ,sum(DME_lne_clms) over (partition by submtg_state_cd,ot_link_key) as DME_lne_cnts
            """

        if filetyp.casefold() == "rx":
            z += f"""
                    ,max(DME_lne_clms) over (partition by submtg_state_cd,{filetyp}_link_key) as DME_clms
                    ,sum(DME_lne_clms) over (partition by submtg_state_cd,{filetyp}_link_key) as DME_lne_cnts
            """

        z += f"""
            from (select
                            a.*
        """

        if filetyp.casefold() == "lt":
            z += f"""
                            ,b.prvdr_txnmy_icf
                            ,b.prvdr_txnmy_nf
                            ,b.prvdr_txnmy_othr_res
            """
        if filetyp.casefold() == "othr_toc":
            z += f"""
                            ,trim(b.code_cat) as prcdr_ccs_cat
                            ,trim(c.code_cat) as hcpcs_ccs_cat

                            ,case when  not_fin_clm=1 and
                                        (all_null_rev_cd=0 or
                                        (bill_type_cd_upd is not null and srvc_plc_cd is null))
                                then 1 else 0 end as inst_clms

                            ,case when not_fin_clm=1 and
                                        (
                                        ((all_null_rev_cd=1 or all_null_rev_cd is null) and /**headers with no line included*/
                                        bill_type_cd_upd is not null and
                                        srvc_plc_cd is not null) or

                                        ((all_null_rev_cd=1 or all_null_rev_cd is null) and
                                        bill_type_cd_upd is null and
                                        srvc_plc_cd is not null) or

                                        ((all_null_rev_cd=1 or all_null_rev_cd is null) and
                                        bill_type_cd_upd is null and
                                        srvc_plc_cd is null and
                                        (ever_null_prcdr_hcpcs_cd =0) /**ALL prcdr code OR HCPCS is non-null*/

                                        )
                                        )
                                then 1 else 0 end as prof_clms

                            ,case when not_fin_clm=1 and
                                        (
                                        ((all_null_rev_cd=1 or all_null_rev_cd is null) and /**headers with no line included*/
                                        bill_type_cd_upd is not null and
                                        srvc_plc_cd is not null) or

                                        ((all_null_rev_cd=1 or all_null_rev_cd is null) and
                                        bill_type_cd_upd is null and
                                        srvc_plc_cd is not null) or

                                        ((all_null_rev_cd=1 or all_null_rev_cd is null) and
                                        bill_type_cd_upd is null and
                                        srvc_plc_cd is null and
                                        (ever_valid_prcdr_hcpcs_cd =1 ) /**At least one non-null HCPCS or PRCDR code **/
                                        )
                                        )
                                then 1 else 0 end as prof_clms_2

                            ,case when not_fin_clm=1 and
                                        ((length(prcdr_cd)=5 and substring(prcdr_cd,1,1)='D') or
                                            (length(hcpcs_rate)=5 and substring(hcpcs_rate,1,1)='D')
                                        )
                                then 1 else 0 end as dental_lne_clms

                            ,case when not_fin_clm=1 and
                                        (trim(b.code_cat) ='Transprt' or
                                        trim(c.code_cat) ='Transprt')
                                then 1 else 0 end as trnsprt_lne_clms

                            ,case when not_fin_clm=1 and
                                    (substring(rev_cd,1,3) in ('066','310') or
                                            prcdr_cd in {tuple(TAF_Metadata.vs_Othr_HCBS_Proc_cd)} or
                                            hcpcs_rate in {tuple(TAF_Metadata.vs_Othr_HCBS_Proc_cd)} or
                                            (prcdr_cd in ('T2025') and srvc_plc_cd in ('12') ) or
                                            (hcpcs_rate in ('T2025') and srvc_plc_cd in ('12') ) or
                                            bnft_type_cd in ('045') or
                                            hcbs_txnmy in {tuple(TAF_Metadata.vs_Othr_HCBS_Taxo)}
                                    )
                                    then 1 else 0 end as othr_hcbs_lne_clms


                            ,case when not_fin_clm=1 and
                                        (trim(b.code_cat) ='Lab' or
                                        trim(c.code_cat) ='Lab' )
                                then 1 else 0 end as Lab_lne_clms

                            ,case when not_fin_clm=1 and
                                        (trim(b.code_cat) ='Rad' or
                                        trim(c.code_cat) ='Rad' )
                                then 1 else 0 end as Rad_lne_clms


                            ,case when not_fin_clm=1 and
                                    (trim(b.code_cat) ='DME' or
                                        trim(c.code_cat) ='DME' )
                                then 1 else 0 end as DME_lne_clms
            """

        if filetyp.casefold() in ("lt", "ip"):
            z += f"""
                            ,case when not_fin_clm=1 and
                                    ( (substring(bill_type_cd_upd,2,1) in ('1', '4') and
                                        substring(bill_type_cd_upd,3,1) in ('1','2')
                                        ) or b.prvdr_txnmy_ip=1
                                        )
                                    then 1 else 0 end as inp_clms

                            ,case when not_fin_clm=1 and
                                        (b.prvdr_txnmy_icf=1 or
                                        (substring(bill_type_cd_upd,2,1) in ('6') and
                                        substring(bill_type_cd_upd,3,1) in ('5','6') )
                                        )
                                then 1 else 0 end as ic_clms

                            ,case when  not_fin_clm=1 and
                                    ( b.prvdr_txnmy_nf=1 or
                                    (substring(bill_type_cd_upd,2,1) in ('2')) or
                                    (substring(bill_type_cd_upd,2,2) in ('18') )
                                    )
                                then 1 else 0 end as nf_clms

                            ,case when  not_fin_clm=1 and
                                    ( b.prvdr_txnmy_othr_res=1 or
                                    (substring(bill_type_cd_upd,2,2) in ('86') )
                                    )
                                then 1 else 0 end as othr_res_clms
            """

        if filetyp.casefold() in ("othr_toc", "lt", "ip"):
            z += f"""
                            ,case when not_fin_clm=1 and
                                        ((substring(bill_type_cd_upd,2,1) in ('1') and
                                        substring(bill_type_cd_upd,3,1) in ('3','4')) or

                                        (substring(bill_type_cd_upd,2,1) in ('8') and
                                        substring(bill_type_cd_upd,3,1) in ('3','4','5','9'))
                                        )
                                    then 1 else 0 end as op_hosp_clms

                            ,case when not_fin_clm=1 and
                                        (substring(bill_type_cd_upd,2,1) in ('7') or
                                        ever_clinic_rev =1)
                                then 1 else 0 end as clinic_clms

                            ,case when not_fin_clm=1 and
                                    (substring(bill_type_cd_upd,2,2) in ('81','82') or
                                            ever_hospice_rev=1)
                                    then 1 else 0 end as hospice_clms

                            ,case when not_fin_clm=1 and
                                        (substring(bill_type_cd_upd,1,3) in ('032','033','034') or
                                        only_hh_rev =1
            """

            if filetyp.casefold() == "othr_toc":
                z += f"""
                     or  only_hh_procs=1
                """
            z += f"""
                                        )
                                    then 1 else 0 end as HH_clms
            """
        elif filetyp.casefold() == "rx":
            z += f"""
                            ,case when not_fin_clm=1 and
                                    (ever_valid_ndc =1 or ever_valid_ndc is null or
                                    (ever_valid_ndc =0 and
                                        ever_dme_hhs_tos =0) )
                                then 1 else 0 end as rx_clms


                            ,case when not_fin_clm=1 and
                                        (ever_valid_ndc =0 and
                                        ever_dme_hhs_tos =1)
                                then 1 else 0 end as DME_lne_clms
            """

        z += f"""
                    from (select *,
                                    case when cmc_php=0 and other_pmpm=0 and
                                            dsh_flag=0 and srvc_trkg=0 and
                                            supp_clms=0 and cap_tos_null_toc=0
                                        then 1 else 0 end as not_fin_clm
                            from {filetyp}_combined
                            )a
        """

        if filetyp.casefold() in ("lt", "ip"):
            z += f"""
                        left join nppes_npi b
                        on a.blg_prvdr_npi_num=b.prvdr_npi
            """
        elif filetyp.casefold() == "othr_toc":
            z += f"""
                        left join ccs_proc b
                        on a.prcdr_cd=b.cd_rng

                        left join ccs_proc c
                        on a.hcpcs_rate=c.cd_rng
            """

        z += f"""
                    ) s1
        """
        self.runner.append(filetyp, z)

        # roll up to claim header

        # ---------------------------------------------------------------------
        # select the first claim line from each set of claims
        # that is header level file
        # not rolling up because some line columns are needed for qa tab
        # ---------------------------------------------------------------------
        z = f"""
             create or replace temporary view {filetyp}_HDR_ROLLED_0 as
             select b.*
                     ,inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms +
                     dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms +
                     clinic_clms + hospice_clms + all_othr_inst_clms +

                     lab_clms + rad_clms + hh_clms + dme_clms +
                     all_othr_prof_clms as tot_num_srvc_flag
                     ,case when (inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms +
                                 dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms +
                                 clinic_clms + hospice_clms + all_othr_inst_clms +
                                 lab_clms + rad_clms + hh_clms + dme_clms +
                                 all_othr_prof_clms ) =0

                             then 0
                             when (inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms +
                                 dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms +
                                 clinic_clms + hospice_clms + all_othr_inst_clms +
                                 lab_clms + rad_clms + hh_clms + dme_clms +
                                 all_othr_prof_clms) = 1
                             then 1
                             when (inp_clms + rx_clms + ic_clms + nf_clms + othr_res_clms +
                                 dental_clms + trnsprt_clms + othr_hcbs_clms + op_hosp_clms +
                                 clinic_clms + hospice_clms + all_othr_inst_clms +
                                 lab_clms + rad_clms + hh_clms + dme_clms +
                                 all_othr_prof_clms ) > 1
                             then 2 else null end as num_srvc_flag_grp

                     ,case when inst_clms=0 and
                             prof_clms=0
                         then 1 else 0 end as not_inst_prof

             from ( select
                     a.*
                     ,cmc_php + other_pmpm + dsh_flag + other_fin
                     as tot_num_fin_flag
        """

        if filetyp.casefold() != "rx":
            z += f"""
                 ,0 as rx_clms
            """

        if filetyp.casefold() != "othr_toc":
            z += f"""
                     ,0 as inst_clms
                     ,0 as prof_clms

                     ,0 as dental_clms
                     ,0 dental_lne_cnts

                     ,0 as trnsprt_clms
                     ,0 as trnsprt_lne_cnts

                     ,0 as othr_hcbs_clms
                     ,0 as othr_hcbs_lne_cnts

                     ,0 as Lab_clms
                     ,0 as Lab_lne_cnts

                     ,0 as Rad_clms
                     ,0 as Rad_lne_cnts

                     ,0 as all_othr_inst_clms
                     ,0 as all_othr_prof_clms
            """

        if filetyp.casefold() in ("othr_toc", "rx"):
            z += f"""
                     ,0 as nf_clms /**Only in IP/LT**/
                     ,0 as inp_clms
                     ,0 as ic_clms
                     ,0 as othr_res_clms
            """

        if filetyp.casefold() == "rx":
            z += f"""
                     /**Only in OT/IP/LT**/
                     ,0 as op_hosp_clms
                     ,0 as clinic_clms
                     ,0 as hospice_clms
                     ,0 as hh_clms
            """

        if filetyp.casefold() in ("lt", "ip"):
            z += f"""
                     /**Only in OT/RX**/
                     ,0 as dme_clms
                     ,0 as DME_lne_cnts
            """

        z += f"""
             from (select
                     *
                     ,case when (srvc_trkg=1 or supp_clms=1 or cap_tos_null_toc=1)
                             then 1 else 0 end as other_fin
        """

        if filetyp.casefold() == "othr_toc":
            z += f"""
                     ,case when  not_fin_clm=1 and
                                 inst_clms=1 and
                                 dental_clms=0 and
                                 trnsprt_clms=0 and
                                 othr_hcbs_clms=0 and
                                 op_hosp_clms=0 and
                                 clinic_clms=0 and
                                 hospice_clms=0
                             then 1 else 0 end as all_othr_inst_clms

                     ,case when  not_fin_clm=1 and
                                 prof_clms=1 and
                                 dental_clms=0 and
                                 trnsprt_clms=0 and
                                 othr_hcbs_clms=0 and
                                 Lab_clms=0 and
                                 Rad_clms=0 and
                                 hh_clms=0 and
                                 DME_clms=0
                             then 1 else 0 end as all_othr_prof_clms
            """

        z += f"""
             from {filetyp}_lne_flag_tos_cat
             where rec_cnt=1 ) a ) b
        """
        self.runner.append(filetyp, z)

        z = f"""
            CREATE OR REPLACE TEMPORARY VIEW {filetyp}_HDR_ROLLED AS
            SELECT *
                ,CASE
                    WHEN cmc_php = 1
                        THEN '11'
                    WHEN other_pmpm = 1
                        THEN '12'
                    WHEN dsh_flag = 1
                        THEN '13'
                    WHEN other_fin = 1
                        THEN '14'
                    WHEN inp_clms = 1
                        THEN '21'
                    WHEN rx_clms = 1
                        THEN '41'
                    WHEN nf_clms = 1
                        THEN '22'
                    WHEN ic_clms = 1
                        THEN '23'
                    WHEN othr_res_clms = 1
                        THEN '24'
                    WHEN hospice_clms = 1
                        THEN '25'
                    WHEN rad_clms = 1
                        THEN '31'
                    WHEN lab_clms = 1
                        THEN '32'
                    WHEN hh_clms = 1
                        THEN '33'
                    WHEN trnsprt_clms = 1
                        THEN '34'
                    WHEN dental_clms = 1
                        THEN '35'
                    WHEN op_hosp_clms = 1
                        THEN '26'
                    WHEN clinic_clms = 1
                        THEN '27'
                    WHEN othr_hcbs_clms = 1
                        THEN '36'
                    WHEN dme_clms = 1
                        THEN '37'
                    WHEN all_othr_inst_clms = 1
                        THEN '28'
                    WHEN all_othr_prof_clms = 1
                        THEN '38'
                    END AS fed_srvc_ctgry_cd
            FROM {filetyp}_HDR_ROLLED_0
        """
        self.runner.append(filetyp, z)


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
