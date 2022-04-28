from taf.TAF_Closure import TAF_Closure, TAF_Closure
from taf.TAF_Runner import TAF_Runner


class TAF_Grouper():

    otmh9f = [
        '290', '291', '292', '293', '294', '295', '296', '297', '298', '299', '300', '301', '302',
        '306', '307', '308', '309', '310', '311', '312', '313', '314', '315', '316', '317', '318', '319']

    otmh0f = [
        'F01', 'F02', 'F03', 'F04', 'F05', 'F06', 'F07', 'F08', 'F09',
        'F20', 'F21', 'F22', 'F23', 'F24', 'F25', 'F26', 'F27', 'F28', 'F29',
        'F30', 'F31', 'F32', 'F33', 'F34', 'F35', 'F36', 'F37', 'F38', 'F39',
        'F40', 'F41', 'F42', 'F43', 'F44', 'F45', 'F46', 'F47', 'F48', 'F49',
        'F50', 'F51', 'F52', 'F53', 'F54', 'F55', 'F56', 'F57', 'F58', 'F59',
        'F60', 'F61', 'F62', 'F63', 'F64', 'F65', 'F66', 'F67', 'F68', 'F69',
        'F70', 'F71', 'F72', 'F73', 'F74', 'F75', 'F76', 'F77', 'F78', 'F79',
        'F80', 'F81', 'F82', 'F83', 'F84', 'F85', 'F86', 'F87', 'F88', 'F89',
        'F90', 'F91', 'F92', 'F93', 'F94', 'F95', 'F96', 'F97', 'F98', 'F99']

    otsu9f = ['303', '304', '305']

    otsu0f = ['F10', 'F11', 'F12', 'F13', 'F14', 'F15', 'F16', 'F17', 'F18', 'F19']

    otmtax = [
        '101Y00000X', '101YM0800X', '101YP1600X', '101YP2500X', '101YS0200X', '102L00000X',
        '102X00000X', '103G00000X', '103GC0700X', '103K00000X', '103T00000X', '103TA0700X',
        '103TC0700X', '103TC2200X', '103TB0200X', '103TC1900X', '103TE1000X', '103TE1100X',
        '103TF0000X', '103TF0200X', '103TP2701X', '103TH0004X', '103TH0100X', '103TM1700X',
        '103TM1800X', '103TP0016X', '103TP0814X', '103TP2700X', '103TR0400X', '103TS0200X',
        '103TW0100X', '104100000X', '1041C0700X', '1041S0200X', '106E00000X', '106H00000X',
        '106S00000X', '163WP0808X', '163WP0809X', '163WP0807X', '167G00000X', '1835P1300X',
        '2080P0006X', '2080P0008X', '2084B0040X', '2084P0804X', '2084F0202X', '2084P0805X',
        '2084P0005X', '2084P0015X', '2084P0800X', '225XM0800X', '251S00000X', '252Y00000X',
        '261QM0801X', '261QM0850X', '261QM0855X', '273R00000X', '283Q00000X', '320600000X',
        '320900000X', '3104A0630X', '3104A0625X', '310500000X', '311500000X', '315P00000X',
        '320800000X', '320900000X', '322D00000X', '323P00000X', '363LP0808X', '364SP0807X',
        '364SP0808X', '364SP0809X', '364SP0810X', '364SP0811X', '364SP0812X', '364SP0813X',
        '385HR2055X', '385HR2060X']

    otstax = [
        '101YA0400X', '103TA0400X', '163WA0400X', '207LA0401X', '207QA0401X',
        '207RA0401X', '2084A0401X', '2084P0802X', '261QM2800X', '261QR0405X',
        '276400000X', '324500000X', '3245S0500X', '2083A0300X']

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def __init__(self, runner: TAF_Runner):

        self.runner = runner
        self.rep_yr = runner.reporting_period.year
        self.rep_mo = runner.reporting_period.month

    # ---------------------------------------------------------------------------------
    #
    #   major diagnostic indicator
    #
    # ---------------------------------------------------------------------------------
    def mdc(self, MDC: bool, filetyp: str):

        select = []
        if MDC:

            select.append(f",coalesce(m14.XREF_VAL, m13.XREF_VAL, m12.XREF_VAL, NULL) as MAJOR_DIAGNOSTIC_CATEGORY")

        return "\n".join(select)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def iap(self, IAP: bool, filetyp: str):

        select = []
        if IAP:

            select.append(f"typeof(NULL) sa IAP_CONDITION_IND")

        return "\n".join(select)

    # ---------------------------------------------------------------------------------
    #
    #   code primary hierarchial conditions
    #
    # ---------------------------------------------------------------------------------
    def phc(self, PHC: bool, filetyp: str):

        select = []
        if PHC:
            # these columns are mutex and take on the first populated value
            select.append(f",coalesce(h12.XREF_VAL, h13.XREF_VAL, h14.XREF_VAL, h16.XREF_VAL, NULL) as PRIMARY_HIERARCHICAL_CONDITION")

        return "\n".join(select)

    # ---------------------------------------------------------------------------------
    #
    #   dgns_cd_ind: 1-> ICD-9, 2-> ICD-10
    #
    # ---------------------------------------------------------------------------------
    def icd(self, MH_SUD: bool, filetyp: str):

        select = []
        if MH_SUD:

            select.append(f",case when (DGNS_1_CD_IND = '1') and (substring (DGNS_1_TEMP, 1, 3) in {tuple(TAF_Grouper.otmh9f)}) then 1 else 0 end as {filetyp}M91")
            select.append(f",case when (DGNS_1_CD_IND = '1') and (substring (DGNS_1_TEMP, 1, 3) in {tuple(TAF_Grouper.otsu9f)}) then 1 else 0 end as {filetyp}S91")

            select.append(f",case when (DGNS_2_CD_IND = '1') and (substring (DGNS_2_TEMP, 1, 3) in {tuple(TAF_Grouper.otmh9f)}) then 1 else 0 end as {filetyp}M92")
            select.append(f",case when (DGNS_2_CD_IND = '1') and (substring (DGNS_2_TEMP, 1, 3) in {tuple(TAF_Grouper.otsu9f)}) then 1 else 0 end as {filetyp}S92")

            select.append(f",case when (DGNS_1_CD_IND = '2') and (substring (DGNS_1_TEMP, 1, 3) in {tuple(TAF_Grouper.otmh0f)}) then 1 else 0 end as {filetyp}M01")
            select.append(f",case when (DGNS_1_CD_IND = '2') and (substring (DGNS_1_TEMP, 1, 3) in {tuple(TAF_Grouper.otsu0f)}) then 1 else 0 end as {filetyp}S01")

            select.append(f",case when (DGNS_2_CD_IND = '2') and (substring (DGNS_2_TEMP, 1, 3) in {tuple(TAF_Grouper.otmh0f)}) then 1 else 0 end as {filetyp}M02")
            select.append(f",case when (DGNS_2_CD_IND = '2') and (substring (DGNS_2_TEMP, 1, 3) in {tuple(TAF_Grouper.otsu0f)}) then 1 else 0 end as {filetyp}S02")

        return "\n".join(select)

    # ---------------------------------------------------------------------------------
    #
    #   dgns_cd_ind: 1-> ICD-9, 2-> ICD-10
    #
    # ---------------------------------------------------------------------------------
    def icd_inner(self, MH_SUD: bool, filetyp: str):

        select = []
        if MH_SUD:

            select.append(',')
            select.append(TAF_Closure.var_set_fills('DGNS_1_CD', cond1='0', cond2='8', cond3='9', cond4='#', new='DGNS_1_TEMP'))
            select.append(',')
            select.append(TAF_Closure.var_set_fills('DGNS_2_CD', cond1='0', cond2='8', cond3='9', cond4='#', new='DGNS_2_TEMP'))

            return "\n".join(select)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def select_taxonomy(self, TAXONOMY: bool, filetyp: str):

        select = []
        if TAXONOMY:

            select.append(f",case when TEMP_TAXONMY is NULL then null when TEMP_TAXONMY in {tuple(TAF_Grouper.otmtax)} then 1 else 0 end as {filetyp}_MH_TAXONOMY_IND_HDR")
            select.append(f",case when TEMP_TAXONMY is NULL then null when TEMP_TAXONMY in {tuple(TAF_Grouper.otstax)} then 1 else 0 end as {filetyp}_SUD_TAXONOMY_IND_HDR")

        return "\n".join(select)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def select_taxonomy_inner(self, TAXONOMY: bool, filetyp: str):

        select = []
        if TAXONOMY:

            select.append(',')
            select.append(TAF_Closure.var_set_taxo('BLG_PRVDR_TXNMY_CD', cond1='8888888888',  cond2='9999999999', cond3='000000000X', cond4='999999999X', cond5='NONE', cond6='XXXXXXXXXX', cond7='NO TAXONOMY', new='TEMP_TAXONMY'))

        return "\n".join(select)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def join_MDC(self, MDC: str):

        join = []
        if MDC:

            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF m12 on m12.LKP_VAL=a.drg_cd and {self.rep_yr} <= 2012 and m12.FRMT_NAME_TXT = 'MDC12FM'")
            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF m13 on m13.LKP_VAL=a.drg_cd and {self.rep_yr}  = 2013 and m13.FRMT_NAME_TXT = 'MDC13FM'")
            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF m14 on m14.LKP_VAL=a.drg_cd and {self.rep_yr} >= 2014 and m14.FRMT_NAME_TXT = 'MDC14FM'")

        return "\n".join(join)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def join_IAP(self, IAP: str):

        join = []
        if IAP:

            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF i93 on i93.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=3 and a.DGNS_1_CD_IND='1' and i93.FRMT_NAME_TXT = 'IAP93F'")
            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF i94 on i94.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=4 and a.DGNS_1_CD_IND='1' and i94.FRMT_NAME_TXT = 'IAP94F'")
            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF i95 on i95.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=5 and a.DGNS_1_CD_IND='1' and i95.FRMT_NAME_TXT = 'IAP95F'")
            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF i04 on i04.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=4 and a.DGNS_1_CD_IND='2' and i04.FRMT_NAME_TXT = 'IAP04F'")
            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF i05 on i05.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=5 and a.DGNS_1_CD_IND='2' and i05.FRMT_NAME_TXT = 'IAP05F'")
            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF i06 on i06.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=6 and a.DGNS_1_CD_IND='2' and i06.FRMT_NAME_TXT = 'IAP06F'")
            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF i07 on i07.LKP_VAL=a.DGNS_1_CD and length(trim(a.DGNS_1_CD))=7 and a.DGNS_1_CD_IND='2' and i07.FRMT_NAME_TXT = 'IAP07F'")

        return "\n".join(join)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def join_PHC(self, PHC: str):

        join = []
        if PHC:

            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF h12 on h12.LKP_VAL=a.dgns_1_cd and {self.rep_yr} <= 2012 and h12.FRMT_NAME_TXT = 'HCC12FM'")
            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF h13 on h13.LKP_VAL=a.dgns_1_cd and {self.rep_yr}  = 2013 and h13.FRMT_NAME_TXT = 'HCC13FM'")

            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF h14 on h14.LKP_VAL=a.dgns_1_cd and ({self.rep_yr}   = 2014  or ({self.rep_yr} = 2015 and {self.rep_mo} < 10)) and h14.FRMT_NAME_TXT = 'HCC14FM'")
            join.append(f"left join {self.runner.DA_SCHEMA}.FRMT_NAME_XREF h16 on h16.LKP_VAL=a.dgns_1_cd and (({self.rep_yr} >= 2016) or ({self.rep_yr} = 2015 and {self.rep_mo} >=10)) and h16.FRMT_NAME_TXT = 'HCC16FM'")

        return "\n".join(join)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def mh_sud(self, MH_SUD: bool, filetyp: str):

        select = []
        if MH_SUD:

            select.append(f""",
                case when ((DGNS_1_CD_IND < '1' or DGNS_1_CD_IND > '2' or DGNS_1_CD_IND is NULL or DGNS_1_TEMP is NULL) and
                           (DGNS_2_CD_IND < '1' or DGNS_2_CD_IND > '2' or DGNS_2_CD_IND is NULL or DGNS_2_TEMP is NULL)) then null
                    when ({filetyp}M91+{filetyp}M92+{filetyp}M01+{filetyp}M02 > 0) then 1
                    when ({filetyp}M91+{filetyp}M92 = 0 or {filetyp}M01+{filetyp}M02 = 0) then 0
                end as {filetyp}_MH_DX_IND""")

            select.append(f""",
                case when ((DGNS_1_CD_IND < '1' or DGNS_1_CD_IND > '2' or DGNS_1_CD_IND is NULL or DGNS_1_TEMP is NULL) and
                           (DGNS_2_CD_IND < '1' or DGNS_2_CD_IND > '2' or DGNS_2_CD_IND is NULL or DGNS_2_TEMP is NULL)) then null
                    when ({filetyp}S91+{filetyp}S92+{filetyp}S01+{filetyp}S02 > 0) then 1
                    when ({filetyp}S91+{filetyp}S92 = 0 or {filetyp}S01+{filetyp}S02 = 0) then 0
                end as {filetyp}_SUD_DX_IND""")

        return "\n".join(select)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def taxonomy(self, TAXONOMY: bool, filetyp: str):

        select = []
        if TAXONOMY:

            select.append(f""",
                case when a.{filetyp}_MH_TAXONOMY_IND_HDR is null
                    and l.{filetyp}_MH_TAXONOMY_IND_LINE is null then null
                    when (coalesce(a.{filetyp}_MH_TAXONOMY_IND_HDR,0) + coalesce(l.{filetyp}_MH_TAXONOMY_IND_LINE,0))=2 then 1
                    when a.{filetyp}_MH_TAXONOMY_IND_HDR=1 then 2
                    when l.{filetyp}_MH_TAXONOMY_IND_LINE=1 then 3 else 0 end as {filetyp}_MH_TAXONOMY_IND""")

            select.append(f""",
                case when a.{filetyp}_SUD_TAXONOMY_IND_HDR is null
                    and l.{filetyp}_SUD_TAXONOMY_IND_LINE is null then null
                    when (coalesce(a.{filetyp}_SUD_TAXONOMY_IND_HDR,0) + coalesce(l.{filetyp}_SUD_TAXONOMY_IND_LINE,0))=2 then 1
                    when a.{filetyp}_SUD_TAXONOMY_IND_HDR=1 then 2
                    when l.{filetyp}_SUD_TAXONOMY_IND_LINE=1 then 3 else 0 end as {filetyp}_SUD_TAXONOMY_IND""")

        return "\n".join(select)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def join_taxonomy(self, TAXONOMY: bool, filetyp: str):

        join = []
        if TAXONOMY:

            join.append(f"""
                left join {filetyp}_TAXONOMY l
                on
                    l.NEW_SUBMTG_STATE_CD_LINE = a.NEW_SUBMTG_STATE_CD
                and l.ORGNL_CLM_NUM_LINE = a.ORGNL_CLM_NUM
                and l.ADJSTMT_CLM_NUM_LINE = a.ADJSTMT_CLM_NUM
                and l.ADJDCTN_DT_LINE = a.ADJDCTN_DT
                and l.LINE_ADJSTMT_IND = a.ADJSTMT_IND""")

        return "\n".join(join)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def select_condition_category(self, MDC: str, IAP: str, PHC: str):

        select = []
        if MDC:

            # code Major Diagnostic Indicator
            select.append(",coalesce(m14.XREF_VAL,m13.XREF_VAL,m12.XREF_VAL,null) as MAJOR_DIAGNOSTIC_CATEGORY")

        if IAP:

            # code IAP_CONDITION_IND
            select.append(",null as IAP_CONDITION_IND")

        if PHC:

            # code Primary Hierarchical Condition
            # these variables are mutually exclusive and it takes the first populated value
            select.append(",coalesce(h12.XREF_VAL,h13.XREF_VAL,h14.XREF_VAL,h16.XREF_VAL,null) as PRIMARY_HIERARCHICAL_CONDITION")

        return "\n".join(select)

    # ---------------------------------------------------------------------------------
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def AWS_Assign_Grouper_Data_Conv(self, filetyp, clm_tbl, line_tbl, analysis_date, MDC=True, IAP=True, PHC=True, MH_SUD=True, TAXONOMY=True):

        z = f"""
            create or replace temporary view {clm_tbl}_STEP1 as
            select
                a.*
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
            ) as a

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

                    ,max(case when TEMP_TAXONMY_LINE is null then null when TEMP_TAXONMY_LINE in { tuple(TAF_Grouper.otmtax) } then 1 else 0 end) as {filetyp}_MH_TAXONOMY_IND_LINE
                    ,max(case when TEMP_TAXONMY_LINE is null then null when TEMP_TAXONMY_LINE in { tuple(TAF_Grouper.otstax) } then 1 else 0 end) as {filetyp}_SUD_TAXONOMY_IND_LINE

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
