CREATE TEMP TABLE IPL distkey(ORGNL_CLM_NUM) AS
SELECT
    6000 as DA_RUN_ID,
    cast (
        (
            '0A' || '-' || 202110 || '-' || NEW_SUBMTG_STATE_CD_LINE || '-' || trim(COALESCE(NULLIF(ORGNL_CLM_NUM_LINE, '~'), '0')) || '-' || trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM_LINE, '~'), '0')) || '-' || CAST(DATE_PART_YEAR(ADJDCTN_DT_LINE) AS CHAR(4)) || CAST(DATE_PART(MONTH, ADJDCTN_DT_LINE) AS CHAR(2)) || CAST(DATE_PART(DAY, ADJDCTN_DT_LINE) AS CHAR(2)) || '-' || COALESCE(LINE_ADJSTMT_IND_CLEAN, 'X')
        ) as varchar(126)
    ) as IP_LINK_KEY,
    '0A' as IP_VRSN,
    '202110' as IP_FIL_DT,
    TMSIS_RUN_ID_LINE as TMSIS_RUN_ID,
    trim(MSIS_IDENT_NUM_LINE) as MSIS_IDENT_NUM,
    NEW_SUBMTG_STATE_CD_LINE as SUBMTG_STATE_CD,
    case
        when ORGNL_CLM_NUM_LINE in ('~')
        or nullif(trim('~'), '') = NULL then NULL
        else ORGNL_CLM_NUM_LINE
    end as ORGNL_CLM_NUM,
    case
        when ADJSTMT_CLM_NUM_LINE in ('~')
        or nullif(trim('~'), '') = NULL then NULL
        else ADJSTMT_CLM_NUM_LINE
    end as ADJSTMT_CLM_NUM,
    trim(ORGNL_LINE_NUM) as ORGNL_LINE_NUM,
    trim(ADJSTMT_LINE_NUM) as ADJSTMT_LINE_NUM,
    case
        when date_cmp(ADJDCTN_DT_LINE, '1600-01-01') = -1 then '1599-12-31' :: date
        else nullif(ADJDCTN_DT_LINE, '01JAN1960')
    end as ADJDCTN_DT,
    LINE_ADJSTMT_IND_CLEAN as LINE_ADJSTMT_IND,
    case
        when regexp_count(lpad(TOS_CD, 3, '0'), '[0-9]{3}') > 0 then case
            when (
                (
                    TOS_CD :: smallint >= 1
                    and TOS_CD :: smallint <= 93
                )
                or (
                    TOS_CD :: smallint in (
                        115,
                        119,
                        120,
                        121,
                        122,
                        123,
                        127,
                        131,
                        134,
                        135,
                        136,
                        137,
                        138,
                        139,
                        140,
                        141,
                        142,
                        143,
                        144,
                        145,
                        146
                    )
                )
            ) then lpad(TOS_CD, 3, '0')
        end
        else NULL
    end as TOS_CD,
    case
        when lpad(IMNZTN_TYPE_CD, 2, '0') = '88' then NULL
        else case
            when IMNZTN_type_cd is not NULL
            and regexp_count(lpad(IMNZTN_type_cd, 2, '0'), '[0-9]{2}') > 0 then case
                when (
                    IMNZTN_type_cd :: smallint >= 0
                    and IMNZTN_type_cd :: smallint <= 29
                ) then lpad(IMNZTN_type_cd, 2, '0')
                else NULL
            end
            else NULL
        end
    end as IMNZTN_type_cd,
    case
        when CMS_64_FED_REIMBRSMT_CTGRY_CD is NOT NULL
        and lpad(trim(CMS_64_FED_REIMBRSMT_CTGRY_CD), 2, '0') in ('01', '02', '03', '04') then lpad(trim(CMS_64_FED_REIMBRSMT_CTGRY_CD), 2, '0')
        else NULL
    end as CMS_64_FED_REIMBRSMT_CTGRY_CD,
    case
        when XIX_SRVC_CTGRY_CD in (
            '001A',
            '001B',
            '001C',
            '001D',
            '002A',
            '002B',
            '002C',
            '003A',
            '003B',
            '004A',
            '004B',
            '004C',
            '005A',
            '005B',
            '005C',
            '005D',
            '006A',
            '006B',
            '0007',
            '07A1',
            '07A2',
            '07A3',
            '07A4',
            '07A5',
            '07A6',
            '0008',
            '009A',
            '009B',
            '0010',
            '0011',
            '0012',
            '0013',
            '0014',
            '0015',
            '0016',
            '017A',
            '017B',
            '17C1',
            '017D',
            '018A',
            '18A1',
            '18A2',
            '18A3',
            '18A4',
            '18A5',
            '18B1',
            '18B2',
            '018C',
            '018D',
            '018E',
            '019A',
            '019B',
            '019C',
            '019D',
            '0022',
            '023A',
            '023B',
            '024A',
            '024B',
            '0025',
            '0026',
            '0027',
            '0028',
            '0029',
            '0030',
            '0031',
            '0032',
            '0033',
            '0034',
            '034A',
            '0035',
            '0036',
            '0037',
            '0038',
            '0039',
            '0040',
            '0041',
            '0042',
            '0043',
            '0044',
            '0045',
            '0046',
            '46A1',
            '46A2',
            '46A3',
            '46A4',
            '46A5',
            '46A6',
            '046B',
            '0049',
            '0050'
        ) then XIX_SRVC_CTGRY_CD
        else null
    end as XIX_SRVC_CTGRY_CD,
    case
        when XXI_SRVC_CTGRY_CD in (
            '01A',
            '01B',
            '01C',
            '01D',
            '002',
            '003',
            '004',
            '005',
            '006',
            '007',
            '008',
            '08A',
            '009',
            '010',
            '011',
            '012',
            '013',
            '014',
            '015',
            '016',
            '017',
            '018',
            '019',
            '020',
            '021',
            '022',
            '023',
            '024',
            '025',
            '031',
            '032',
            '32A',
            '32B',
            '033',
            '034',
            '035',
            '35A',
            '35B',
            '048',
            '049',
            '050'
        ) then XXI_SRVC_CTGRY_CD
        else null
    end as XXI_SRVC_CTGRY_CD,
    trim(CLL_STUS_CD) as CLL_STUS_CD,
    case
        when date_cmp(SRVC_BGNNG_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else nullif(SRVC_BGNNG_DT, '01JAN1960')
    end as SRVC_BGNNG_DT,
    case
        when date_cmp(SRVC_ENDG_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else nullif(SRVC_ENDG_DT, '01JAN1960')
    end as SRVC_ENDG_DT,
    case
        when BNFT_TYPE_CD is not NULL
        and regexp_count(lpad(BNFT_TYPE_CD, 3, '0'), '[0-9]{3}') > 0 then case
            when (
                BNFT_TYPE_CD :: smallint >= 001
                and BNFT_TYPE_CD :: smallint <= 108
            ) then lpad(BNFT_TYPE_CD, 3, '0')
            else NULL
        end
        else NULL
    end as BNFT_TYPE_CD,
    case
        when REV_CD is NOT NULL then lpad(trim(REV_CD), 4, '0')
        else NULL
    end as REV_CD,
    case
        when IP_LT_ACTL_SRVC_QTY in (999999, 88888.888, 99999.990) then NULL
        else IP_LT_ACTL_SRVC_QTY
    end as ACTL_SRVC_QTY,
    case
        when IP_LT_ALOWD_SRVC_QTY in (888888.89, 88888.888) then NULL
        else IP_LT_ALOWD_SRVC_QTY
    end as ALOWD_SRVC_QTY,
    case
        when REV_CHRG_AMT in (
            88888888888.88,
            99999999.9,
            888888888.88,
            8888888888.88,
            88888888.88
        ) then NULL
        else REV_CHRG_AMT
    end as REV_CHRG_AMT,
    trim(SRVCNG_PRVDR_NUM) as SRVCNG_PRVDR_NUM,
    trim(PRSCRBNG_PRVDR_NPI_NUM) as SRVCNG_PRVDR_NPI_NUM,
    case
        when regexp_count(trim(SRVCNG_PRVDR_TXNMY_CD), '[^0]+') = 0
        or SRVCNG_PRVDR_TXNMY_CD in (
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
        else SRVCNG_PRVDR_TXNMY_CD
    end as SRVCNG_PRVDR_TXNMY_CD,
    case
        when regexp_count(lpad(SRVCNG_PRVDR_TYPE_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                SRVCNG_PRVDR_TYPE_CD :: smallint >= 1
                and SRVCNG_PRVDR_TYPE_CD :: smallint <= 57
            ) then lpad(SRVCNG_PRVDR_TYPE_CD, 2, '0')
            else NULL
        end
        else NULL
    end as SRVCNG_PRVDR_TYPE_CD,
    case
        when regexp_count(lpad(SRVCNG_PRVDR_SPCLTY_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                SRVCNG_PRVDR_SPCLTY_CD :: smallint >= 1
                and SRVCNG_PRVDR_SPCLTY_CD :: smallint <= 98
            ) then upper(lpad(SRVCNG_PRVDR_SPCLTY_CD, 2, '0'))
            else NULL
        end
        else case
            when upper(SRVCNG_PRVDR_SPCLTY_CD) in (
                'A0',
                'A1',
                'A2',
                'A3',
                'A4',
                'A5',
                'A6',
                'A7',
                'A8',
                'A9',
                'B1',
                'B2',
                'B3',
                'B4',
                'B5'
            ) then upper(lpad(SRVCNG_PRVDR_SPCLTY_CD, 2, '0'))
            else NULL
        end
    end as SRVCNG_PRVDR_SPCLTY_CD,
    trim(OPRTG_PRVDR_NPI_NUM) as OPRTG_PRVDR_NPI_NUM,
    case
        when PRVDR_FAC_TYPE_CD in (
            '100000000',
            '170000000',
            '250000000',
            '260000000',
            '270000000',
            '280000000',
            '290000000',
            '300000000',
            '310000000',
            '320000000',
            '330000000',
            '340000000',
            '380000000'
        ) then prvdr_fac_type_cd
        else NULL
    end as prvdr_fac_type_cd,
    case
        when NDC_QTY in (
            999999,
            888888,
            88888.888,
            888888.888,
            999999.998,
            888888.880
        ) then NULL
        else NDC_QTY
    end as NDC_QTY,
    trim(HCPCS_RATE) as HCPCS_RATE,
    case
        when (
            regexp_count(trim(NDC_CD), '[^0]+') = 0
            or regexp_count(trim(NDC_CD), '[^8]+') = 0
            or regexp_count(trim(NDC_CD), '[^9]+') = 0
            or regexp_count(trim(NDC_CD), '[^#]+') = 0
        )
        or nullif(trim(NDC_CD), '') = null then NULL
        else NDC_CD
    end as NDC_CD,
    case
        when trim(upper(UOM_CD)) in (
            'F2',
            'ML',
            'GR',
            'UN',
            'ME'
        ) then trim(upper(UOM_CD))
        else NULL
    end as UOM_CD,
    case
        when ALOWD_AMT in (
            888888888.88,
            99999999.00,
            9999999999.99
        ) then NULL
        else ALOWD_AMT
    end as ALOWD_AMT,
    case
        when MDCD_PD_AMT in (888888888.88) then NULL
        else MDCD_PD_AMT
    end as MDCD_PD_AMT,
    case
        when OTHR_INSRNC_AMT in (
            888888888.88,
            88888888888.00,
            88888888888.88
        ) then NULL
        else OTHR_INSRNC_AMT
    end as OTHR_INSRNC_AMT,
    case
        when MDCD_FFS_EQUIV_AMT in (
            888888888.88,
            88888888888.80,
            999999.99
        ) then NULL
        else MDCD_FFS_EQUIV_AMT
    end as MDCD_FFS_EQUIV_AMT,
    RN as LINE_NUM
FROM
    (
        select
            *,
            case
                when LINE_ADJSTMT_IND is NOT NULL
                and trim(LINE_ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6') then trim(LINE_ADJSTMT_IND)
                else NULL
            end as LINE_ADJSTMT_IND_CLEAN
        from
            IP_LINE
    ) H