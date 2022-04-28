CREATE TEMP TABLE IP_TAXONOMY distkey (ORGNL_CLM_NUM_LINE) sortkey(
    NEW_SUBMTG_STATE_CD_LINE,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND
) as
select
    NEW_SUBMTG_STATE_CD_LINE,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND,
    max(
        case
            when TEMP_TAXONMY_LINE is null then null
            when TEMP_TAXONMY_LINE in (
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
        end
    ) as IP_MH_TAXONOMY_IND_LINE,
    max(
        case
            when TEMP_TAXONMY_LINE is null then null
            when TEMP_TAXONMY_LINE in (
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
        end
    ) as IP_SUD_TAXONOMY_IND_LINE
from
    (
        select
            *,
            case
                when regexp_count(trim(srvcng_prvdr_txnmy_cd), '[^0]+') = 0
                or srvcng_prvdr_txnmy_cd in (
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
                else srvcng_prvdr_txnmy_cd
            end as TEMP_TAXONMY_LINE
        from
            IP_LINE
    ) line
group by
    NEW_SUBMTG_STATE_CD_LINE,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND