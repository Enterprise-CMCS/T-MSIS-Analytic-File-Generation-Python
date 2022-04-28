CREATE TEMP TABLE IP_HEADER_STEP1 distkey (ORGNL_CLM_NUM) sortkey(
    NEW_SUBMTG_STATE_CD,
    ORGNL_CLM_NUM,
    ADJSTMT_CLM_NUM,
    ADJDCTN_DT,
    ADJSTMT_IND
) as
select
    a.*,
    coalesce(m14.XREF_VAL, m13.XREF_VAL, m12.XREF_VAL, null) as MAJOR_DIAGNOSTIC_CATEGORY,
    null as IAP_CONDITION_IND,
    coalesce(
        h12.XREF_VAL,
        h13.XREF_VAL,
        h14.XREF_VAL,
        h16.XREF_VAL,
        null
    ) as PRIMARY_HIERARCHICAL_CONDITION,
    case
        when (
            regexp_count(trim(DGNS_1_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_1_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_1_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_1_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_1_CD), '') = null then NULL
        else DGNS_1_CD
    end as DGNS_1_TEMP,
    case
        when (
            regexp_count(trim(DGNS_2_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_2_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_2_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_2_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_2_CD), '') = null then NULL
        else DGNS_2_CD
    end as DGNS_2_TEMP,
    case
        when (DGNS_1_CD_IND = '1')
        and (
            substring (DGNS_1_TEMP, 1, 3) in (
                '290',
                '291',
                '292',
                '293',
                '294',
                '295',
                '296',
                '297',
                '298',
                '299',
                '300',
                '301',
                '302',
                '306',
                '307',
                '308',
                '309',
                '310',
                '311',
                '312',
                '313',
                '314',
                '315',
                '316',
                '317',
                '318',
                '319'
            )
        ) then 1
        else 0
    end as IPM91,
    case
        when (DGNS_1_CD_IND = '1')
        and (
            substring (DGNS_1_TEMP, 1, 3) in ('303', '304', '305')
        ) then 1
        else 0
    end as IPS91,
    case
        when (DGNS_2_CD_IND = '1')
        and (
            substring (DGNS_2_TEMP, 1, 3) in (
                '290',
                '291',
                '292',
                '293',
                '294',
                '295',
                '296',
                '297',
                '298',
                '299',
                '300',
                '301',
                '302',
                '306',
                '307',
                '308',
                '309',
                '310',
                '311',
                '312',
                '313',
                '314',
                '315',
                '316',
                '317',
                '318',
                '319'
            )
        ) then 1
        else 0
    end as IPM92,
    case
        when (DGNS_2_CD_IND = '1')
        and (
            substring (DGNS_2_TEMP, 1, 3) in ('303', '304', '305')
        ) then 1
        else 0
    end as IPS92,
    case
        when (DGNS_1_CD_IND = '2')
        and (
            substring (DGNS_1_TEMP, 1, 3) in (
                'F01',
                'F02',
                'F03',
                'F04',
                'F05',
                'F06',
                'F07',
                'F08',
                'F09',
                'F20',
                'F21',
                'F22',
                'F23',
                'F24',
                'F25',
                'F26',
                'F27',
                'F28',
                'F29',
                'F30',
                'F31',
                'F32',
                'F33',
                'F34',
                'F35',
                'F36',
                'F37',
                'F38',
                'F39',
                'F40',
                'F41',
                'F42',
                'F43',
                'F44',
                'F45',
                'F46',
                'F47',
                'F48',
                'F49',
                'F50',
                'F51',
                'F52',
                'F53',
                'F54',
                'F55',
                'F56',
                'F57',
                'F58',
                'F59',
                'F60',
                'F61',
                'F62',
                'F63',
                'F64',
                'F65',
                'F66',
                'F67',
                'F68',
                'F69',
                'F70',
                'F71',
                'F72',
                'F73',
                'F74',
                'F75',
                'F76',
                'F77',
                'F78',
                'F79',
                'F80',
                'F81',
                'F82',
                'F83',
                'F84',
                'F85',
                'F86',
                'F87',
                'F88',
                'F89',
                'F90',
                'F91',
                'F92',
                'F93',
                'F94',
                'F95',
                'F96',
                'F97',
                'F98',
                'F99'
            )
        ) then 1
        else 0
    end as IPM01,
    case
        when (DGNS_1_CD_IND = '2')
        and (
            substring (DGNS_1_TEMP, 1, 3) in (
                'F10',
                'F11',
                'F12',
                'F13',
                'F14',
                'F15',
                'F16',
                'F17',
                'F18',
                'F19'
            )
        ) then 1
        else 0
    end as IPS01,
    case
        when (DGNS_2_CD_IND = '2')
        and (
            substring (DGNS_2_TEMP, 1, 3) in (
                'F01',
                'F02',
                'F03',
                'F04',
                'F05',
                'F06',
                'F07',
                'F08',
                'F09',
                'F20',
                'F21',
                'F22',
                'F23',
                'F24',
                'F25',
                'F26',
                'F27',
                'F28',
                'F29',
                'F30',
                'F31',
                'F32',
                'F33',
                'F34',
                'F35',
                'F36',
                'F37',
                'F38',
                'F39',
                'F40',
                'F41',
                'F42',
                'F43',
                'F44',
                'F45',
                'F46',
                'F47',
                'F48',
                'F49',
                'F50',
                'F51',
                'F52',
                'F53',
                'F54',
                'F55',
                'F56',
                'F57',
                'F58',
                'F59',
                'F60',
                'F61',
                'F62',
                'F63',
                'F64',
                'F65',
                'F66',
                'F67',
                'F68',
                'F69',
                'F70',
                'F71',
                'F72',
                'F73',
                'F74',
                'F75',
                'F76',
                'F77',
                'F78',
                'F79',
                'F80',
                'F81',
                'F82',
                'F83',
                'F84',
                'F85',
                'F86',
                'F87',
                'F88',
                'F89',
                'F90',
                'F91',
                'F92',
                'F93',
                'F94',
                'F95',
                'F96',
                'F97',
                'F98',
                'F99'
            )
        ) then 1
        else 0
    end as IPM02,
    case
        when (DGNS_2_CD_IND = '2')
        and (
            substring (DGNS_2_TEMP, 1, 3) in (
                'F10',
                'F11',
                'F12',
                'F13',
                'F14',
                'F15',
                'F16',
                'F17',
                'F18',
                'F19'
            )
        ) then 1
        else 0
    end as IPS02,
    case
        when regexp_count(trim(BLG_PRVDR_TXNMY_CD), '[^0]+') = 0
        or BLG_PRVDR_TXNMY_CD in (
            '8888888888',
            '9999999999',
            '000000000X',
            '999999999X',
            'NONE',
            'XXXXXXXXXX',
            'NO TAXONOMY'
        )
        or nullif(trim('8888888888'), '') = NULL
        or nullif(trim('9999999999'), '') = NULL
        or nullif(trim('000000000X'), '') = NULL
        or nullif(trim('999999999X'), '') = NULL
        or nullif(trim('NONE'), '') = NULL
        or nullif(trim('XXXXXXXXXX'), '') = NULL
        or nullif(trim('NO TAXONOMY'), '') = NULL then NULL
        else BLG_PRVDR_TXNMY_CD
    end as TEMP_TAXONMY,
    case
        when TEMP_TAXONMY is NULL then null
        when TEMP_TAXONMY in (
            '101Y00000X',
            '101YM0800X',
            '101YP1600X',
            '101YP2500X',
            '101YS0200X',
            '102L00000X',
            '102X00000X',
            '103G00000X',
            '103GC0700X',
            '103K00000X',
            '103T00000X',
            '103TA0700X',
            '103TC0700X',
            '103TC2200X',
            '103TB0200X',
            '103TC1900X',
            '103TE1000X',
            '103TE1100X',
            '103TF0000X',
            '103TF0200X',
            '103TP2701X',
            '103TH0004X',
            '103TH0100X',
            '103TM1700X',
            '103TM1800X',
            '103TP0016X',
            '103TP0814X',
            '103TP2700X',
            '103TR0400X',
            '103TS0200X',
            '103TW0100X',
            '104100000X',
            '1041C0700X',
            '1041S0200X',
            '106E00000X',
            '106H00000X',
            '106S00000X',
            '163WP0808X',
            '163WP0809X',
            '163WP0807X',
            '167G00000X',
            '1835P1300X',
            '2080P0006X',
            '2080P0008X',
            '2084B0040X',
            '2084P0804X',
            '2084F0202X',
            '2084P0805X',
            '2084P0005X',
            '2084P0015X',
            '2084P0800X',
            '225XM0800X',
            '251S00000X',
            '252Y00000X',
            '261QM0801X',
            '261QM0850X',
            '261QM0855X',
            '273R00000X',
            '283Q00000X',
            '320600000X',
            '320900000X',
            '3104A0630X',
            '3104A0625X',
            '310500000X',
            '311500000X',
            '315P00000X',
            '320800000X',
            '320900000X',
            '322D00000X',
            '323P00000X',
            '363LP0808X',
            '364SP0807X',
            '364SP0808X',
            '364SP0809X',
            '364SP0810X',
            '364SP0811X',
            '364SP0812X',
            '364SP0813X',
            '385HR2055X',
            '385HR2060X'
        ) then 1
        else 0
    end as IP_MH_TAXONOMY_IND_HDR,
    case
        when TEMP_TAXONMY is NULL then null
        when TEMP_TAXONMY in (
            '101YA0400X',
            '103TA0400X',
            '163WA0400X',
            '207LA0401X',
            '207QA0401X',
            '207RA0401X',
            '2084A0401X',
            '2084P0802X',
            '261QM2800X',
            '261QR0405X',
            '276400000X',
            '324500000X',
            '3245S0500X',
            '2083A0300X'
        ) then 1
        else 0
    end as IP_SUD_TAXONOMY_IND_HDR
from
    IP_HEADER a
    left join data_anltcs_dm_prod.FRMT_NAME_XREF m12 on m12.LKP_VAL = a.drg_cd
    and 2021 <= 2012
    and m12.FRMT_NAME_TXT = 'MDC12FM'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF m13 on m13.LKP_VAL = a.drg_cd
    and 2021 = 2013
    and m13.FRMT_NAME_TXT = 'MDC13FM'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF m14 on m14.LKP_VAL = a.drg_cd
    and 2021 >= 2014
    and m14.FRMT_NAME_TXT = 'MDC14FM'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF i93 on i93.LKP_VAL = a.DGNS_1_CD
    and length(trim(a.DGNS_1_CD)) = 3
    and a.DGNS_1_CD_IND = '1'
    and i93.FRMT_NAME_TXT = 'IAP93F'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF i94 on i94.LKP_VAL = a.DGNS_1_CD
    and length(trim(a.DGNS_1_CD)) = 4
    and a.DGNS_1_CD_IND = '1'
    and i94.FRMT_NAME_TXT = 'IAP94F'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF i95 on i95.LKP_VAL = a.DGNS_1_CD
    and length(trim(a.DGNS_1_CD)) = 5
    and a.DGNS_1_CD_IND = '1'
    and i95.FRMT_NAME_TXT = 'IAP95F'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF i04 on i04.LKP_VAL = a.DGNS_1_CD
    and length(trim(a.DGNS_1_CD)) = 4
    and a.DGNS_1_CD_IND = '2'
    and i04.FRMT_NAME_TXT = 'IAP04F'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF i05 on i05.LKP_VAL = a.DGNS_1_CD
    and length(trim(a.DGNS_1_CD)) = 5
    and a.DGNS_1_CD_IND = '2'
    and i05.FRMT_NAME_TXT = 'IAP05F'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF i06 on i06.LKP_VAL = a.DGNS_1_CD
    and length(trim(a.DGNS_1_CD)) = 6
    and a.DGNS_1_CD_IND = '2'
    and i06.FRMT_NAME_TXT = 'IAP06F'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF i07 on i07.LKP_VAL = a.DGNS_1_CD
    and length(trim(a.DGNS_1_CD)) = 7
    and a.DGNS_1_CD_IND = '2'
    and i07.FRMT_NAME_TXT = 'IAP07F'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF h12 on h12.LKP_VAL = a.dgns_1_cd
    and 2021 <= 2012
    and h12.FRMT_NAME_TXT = 'HCC12FM'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF h13 on h13.LKP_VAL = a.dgns_1_cd
    and 2021 = 2013
    and h13.FRMT_NAME_TXT = 'HCC13FM'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF h14 on h14.LKP_VAL = a.dgns_1_cd
    and (
        2021 = 2014
        or (
            2021 = 2015
            and 10 < 10
        )
    )
    and h14.FRMT_NAME_TXT = 'HCC14FM'
    left join data_anltcs_dm_prod.FRMT_NAME_XREF h16 on h16.LKP_VAL = a.dgns_1_cd
    and (
        (2021 >= 2016)
        or (
            2021 = 2015
            and 10 >= 10
        )
    )
    and h16.FRMT_NAME_TXT = 'HCC16FM'