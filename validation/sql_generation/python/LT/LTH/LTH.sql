create
or replace temporary view LTH as
select
    6194 as DA_RUN_ID,
    'lt_link_key' as LT_LINK_KEY,
    '0A' as LT_VRSN,
    '202110' as LT_FIL_DT,
    TMSIS_RUN_ID,
    trim(MSIS_IDENT_NUM) as MSIS_IDENT_NUM,
    NEW_SUBMTG_STATE_CD as SUBMTG_STATE_CD,
    case
        when ORGNL_CLM_NUM in ('~')
        or nullif(trim('~'), '') = NULL then NULL
        else ORGNL_CLM_NUM
    end as ORGNL_CLM_NUM,
    case
        when ADJSTMT_CLM_NUM in ('~')
        or nullif(trim('~'), '') = NULL then NULL
        else ADJSTMT_CLM_NUM
    end as ADJSTMT_CLM_NUM,
    ADJSTMT_IND_CLEAN as ADJSTMT_IND,
    case
        when regexp_count(lpad(ADJSTMT_RSN_CD, 3, '0'), '[0-9]{3}') > 0 then case
            when (
                ADJSTMT_RSN_CD >= 1
                and ADJSTMT_RSN_CD <= 99
            ) then upper(lpad(ADJSTMT_RSN_CD, 3, '0'))
            else ADJSTMT_RSN_CD
        end
        else ADJSTMT_RSN_CD
    end as ADJSTMT_RSN_CD,
    case
        when date_cmp('SRVC_BGNNG_DT', '1600-01-01') = '-1 then ' 1599 -12 -31 ' else nullif(' SRVC_BGNNG_DT ', ' 01JAN1960 ') end as SRVC_BGNNG_DT
                , nullif(' SRVC_ENDG_DT ', ' 01JAN1960 ') as SRVC_ENDG_DT
                , case when (ADMSN_DT < to_date(' 1600 -01 -01 ')) then to_date(' 1599 -12 -31 ') else ADMSN_DT end as ADMSN_DT
                , case when ADMSN_HR_NUM is not NULL and regexp_count(lpad(ADMSN_HR_NUM, 2, ' 0 '), ' [ 0 -9 ] { 2 } ') > 0 then
       case when (ADMSN_HR_NUM >= 0 and ADMSN_HR_NUM <= 23) then lpad(ADMSN_HR_NUM, 2, ' 0 ')
        else NULL end
    else NULL end
    as ADMSN_HR_NUM
                , case when (DSCHRG_DT < to_date(' 1600 -01 -01 ')) then to_date(' 1599 -12 -31 ') else DSCHRG_DT end as DSCHRG_DT
                , case when DSCHRG_HR_NUM is not NULL and regexp_count(lpad(DSCHRG_HR_NUM, 2, ' 0 '), ' [ 0 -9 ] { 2 } ') > 0 then
       case when (DSCHRG_HR_NUM >= 0 and DSCHRG_HR_NUM <= 23) then lpad(DSCHRG_HR_NUM, 2, ' 0 ')
        else NULL end
    else NULL end
    as DSCHRG_HR_NUM
                , case when date_cmp(' ADJDCTN_DT ', ' 1600 -01 -01 ')=' -1 then '1599-12-31'
        else nullif('ADJDCTN_DT', '01JAN1960')
    end as ADJDCTN_DT,
    case
        when (MDCD_PD_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else MDCD_PD_DT
    end as MDCD_PD_DT,
    case
        when SECT_1115A_DEMO_IND is NOT NULL
        and trim(SECT_1115A_DEMO_IND) in ('0', '1') then trim(SECT_1115A_DEMO_IND)
        else NULL
    end as SECT_1115A_DEMO_IND,
    trim(BILL_TYPE_CD) as BILL_TYPE_CD,
    case
        when upper(CLM_TYPE_CD) in (
            '1',
            '2',
            '3',
            '4',
            '5',
            'A',
            'B',
            'C',
            'D',
            'E',
            'U',
            'V',
            'W',
            'X',
            'Y',
            'Z'
        ) then upper(CLM_TYPE_CD)
        else NULL
    end as CLM_TYPE_CD,
    case
        when lpad(pgm_type_cd, 2, '0') in ('06', '09') then NULL
        else case
            when pgm_type_cd is not NULL
            and regexp_count(lpad(pgm_type_cd, 2, '0'), '[0-9]{2}') > 0 then case
                when (
                    pgm_type_cd >= 0
                    and pgm_type_cd <= 17
                ) then lpad(pgm_type_cd, 2, '0')
                else NULL
            end
            else NULL
        end
    end as pgm_type_cd,
    trim(MC_PLAN_ID) as MC_PLAN_ID,
    trim(upper(ELGBL_LAST_NAME)) as ELGBL_LAST_NAME,
    trim(upper(ELGBL_1ST_NAME)) as ELGBL_1ST_NAME,
    trim(upper(ELGBL_MDL_INITL_NAME)) as ELGBL_MDL_INITL_NAME,
    case
        when (BIRTH_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else BIRTH_DT
    end as BIRTH_DT,
    case
        when lpad(wvr_type_cd, 2, '0') = '88' then NULL
        else case
            when wvr_type_cd is not NULL
            and regexp_count(lpad(wvr_type_cd, 2, '0'), '[0-9]{2}') > 0 then case
                when (
                    wvr_type_cd >= 1
                    and wvr_type_cd <= 33
                ) then lpad(wvr_type_cd, 2, '0')
                else NULL
            end
            else NULL
        end
    end as wvr_type_cd,
    trim(WVR_ID) as WVR_ID,
    case
        when SRVC_TRKNG_TYPE_CD is NOT NULL
        and lpad(trim(SRVC_TRKNG_TYPE_CD), 2, '0') in ('00', '01', '02', '03', '04', '05', '06') then lpad(trim(SRVC_TRKNG_TYPE_CD), 2, '0')
        else NULL
    end as SRVC_TRKNG_TYPE_CD,
    case
        when SRVC_TRKNG_PYMT_AMT in (888888888.88) then NULL
        else SRVC_TRKNG_PYMT_AMT
    end as SRVC_TRKNG_PYMT_AMT,
    case
        when OTHR_INSRNC_IND is NOT NULL
        and trim(OTHR_INSRNC_IND) in ('0', '1') then trim(OTHR_INSRNC_IND)
        else NULL
    end as OTHR_INSRNC_IND,
    case
        when OTHR_TPL_CLCTN_CD is NOT NULL
        and lpad(trim(OTHR_TPL_CLCTN_CD), 3, '0') in (
            '000',
            '001',
            '002',
            '003',
            '004',
            '005',
            '006',
            '007'
        ) then lpad(trim(OTHR_TPL_CLCTN_CD), 3, '0')
        else NULL
    end as OTHR_TPL_CLCTN_CD,
    case
        when FIXD_PYMT_IND is NOT NULL
        and trim(FIXD_PYMT_IND) in ('0', '1') then trim(FIXD_PYMT_IND)
        else NULL
    end as FIXD_PYMT_IND,
    case
        when trim(upper(FUNDNG_CD)) in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I') then trim(upper(FUNDNG_CD))
        else NULL
    end as FUNDNG_CD,
    case
        when fundng_src_non_fed_shr_cd is NOT NULL
        and lpad(trim(fundng_src_non_fed_shr_cd), 2, '0') in ('01', '02', '03', '04', '05', '06') then lpad(trim(fundng_src_non_fed_shr_cd), 2, '0')
        else NULL
    end as fundng_src_non_fed_shr_cd,
    case
        when BRDR_STATE_IND is NOT NULL
        and trim(BRDR_STATE_IND) in ('0', '1') then trim(BRDR_STATE_IND)
        else NULL
    end as BRDR_STATE_IND,
    case
        when XOVR_IND is NOT NULL
        and trim(XOVR_IND) in ('0', '1') then trim(XOVR_IND)
        else NULL
    end as XOVR_IND,
    trim(MDCR_HICN_NUM) as MDCR_HICN_NUM,
    trim(MDCR_BENE_ID) as MDCR_BENE_ID,
    trim(PTNT_CNTL_NUM) as PTNT_CNTL_NUM,
    case
        when HLTH_CARE_ACQRD_COND_CD is NOT NULL
        and trim(HLTH_CARE_ACQRD_COND_CD) in ('0', '1') then trim(HLTH_CARE_ACQRD_COND_CD)
        else NULL
    end as HLTH_CARE_ACQRD_COND_CD,
    case
        when lpad(PTNT_STUS_CD, 2, '0') in(
            '01',
            '02',
            '03',
            '04',
            '05',
            '06',
            '07',
            '08',
            '09',
            '20',
            '21',
            '30',
            '40',
            '41',
            '42',
            '43',
            '50',
            '51',
            '61',
            '62',
            '63',
            '64',
            '65',
            '66',
            '69',
            '70',
            '71',
            '72',
            '81',
            '82',
            '83',
            '84',
            '85',
            '86',
            '87',
            '88',
            '89',
            '90',
            '91',
            '92',
            '93',
            '94',
            '95'
        ) then lpad(PTNT_STUS_CD, 2, '0')
        else NULL
    end as PTNT_STUS_CD,
    case
        when (
            regexp_count(trim(ADMTG_DGNS_CD), '[^0]+') = 0
            or regexp_count(trim(ADMTG_DGNS_CD), '[^8]+') = 0
            or regexp_count(trim(ADMTG_DGNS_CD), '[^9]+') = 0
            or regexp_count(trim(ADMTG_DGNS_CD), '[^#]+') = 0
        )
        or nullif(trim(ADMTG_DGNS_CD), '') = null then NULL
        else ADMTG_DGNS_CD
    end as ADMTG_DGNS_CD,
    case
        when ADMTG_DGNS_CD_IND is NOT NULL
        and trim(ADMTG_DGNS_CD_IND) in ('1', '2', '3') then trim(ADMTG_DGNS_CD_IND)
        else NULL
    end as ADMTG_DGNS_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_1_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_1_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_1_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_1_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_1_CD), '') = null then NULL
        else DGNS_1_CD
    end as DGNS_1_CD,
    case
        when DGNS_1_CD_IND is NOT NULL
        and trim(DGNS_1_CD_IND) in ('1', '2', '3') then trim(DGNS_1_CD_IND)
        else NULL
    end as DGNS_1_CD_IND,
    case
        when (
            upper(DGNS_POA_1_CD_IND) in ('Y', 'N', 'U', 'W', '1')
        ) then upper(DGNS_POA_1_CD_IND)
        else NULL
    end as DGNS_POA_1_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_2_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_2_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_2_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_2_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_2_CD), '') = null then NULL
        else DGNS_2_CD
    end as DGNS_2_CD,
    case
        when DGNS_2_CD_IND is NOT NULL
        and trim(DGNS_2_CD_IND) in ('1', '2', '3') then trim(DGNS_2_CD_IND)
        else NULL
    end as DGNS_2_CD_IND,
    case
        when (
            upper(DGNS_POA_2_CD_IND) in ('Y', 'N', 'U', 'W', '1')
        ) then upper(DGNS_POA_2_CD_IND)
        else NULL
    end as DGNS_POA_2_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_3_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_3_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_3_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_3_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_3_CD), '') = null then NULL
        else DGNS_3_CD
    end as DGNS_3_CD,
    case
        when DGNS_3_CD_IND is NOT NULL
        and trim(DGNS_3_CD_IND) in ('1', '2', '3') then trim(DGNS_3_CD_IND)
        else NULL
    end as DGNS_3_CD_IND,
    case
        when (
            upper(DGNS_POA_3_CD_IND) in ('Y', 'N', 'U', 'W', '1')
        ) then upper(DGNS_POA_3_CD_IND)
        else NULL
    end as DGNS_POA_3_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_4_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_4_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_4_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_4_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_4_CD), '') = null then NULL
        else DGNS_4_CD
    end as DGNS_4_CD,
    case
        when DGNS_4_CD_IND is NOT NULL
        and trim(DGNS_4_CD_IND) in ('1', '2', '3') then trim(DGNS_4_CD_IND)
        else NULL
    end as DGNS_4_CD_IND,
    case
        when (
            upper(DGNS_POA_4_CD_IND) in ('Y', 'N', 'U', 'W', '1')
        ) then upper(DGNS_POA_4_CD_IND)
        else NULL
    end as DGNS_POA_4_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_5_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_5_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_5_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_5_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_5_CD), '') = null then NULL
        else DGNS_5_CD
    end as DGNS_5_CD,
    case
        when DGNS_5_CD_IND is NOT NULL
        and trim(DGNS_5_CD_IND) in ('1', '2', '3') then trim(DGNS_5_CD_IND)
        else NULL
    end as DGNS_5_CD_IND,
    case
        when (
            upper(DGNS_POA_5_CD_IND) in ('Y', 'N', 'U', 'W', '1')
        ) then upper(DGNS_POA_5_CD_IND)
        else NULL
    end as DGNS_POA_5_CD_IND,
    case
        when NCVRD_DAYS_CNT in (88888) then NULL
        else NCVRD_DAYS_CNT
    end as NCVRD_DAYS_CNT,
    case
        when NCVRD_CHRGS_AMT in (888888888.88) then NULL
        else NCVRD_CHRGS_AMT
    end as NCVRD_CHRGS_AMT,
    MDCD_CVRD_IP_DAYS_CNT,
    case
        when ICF_IID_DAYS_CNT in (8888) then NULL
        else ICF_IID_DAYS_CNT
    end as ICF_IID_DAYS_CNT,
    case
        when LVE_DAYS_CNT in (88888) then NULL
        else LVE_DAYS_CNT
    end as LVE_DAYS_CNT,
    case
        when NRSNG_FAC_DAYS_CNT in (88888) then NULL
        else NRSNG_FAC_DAYS_CNT
    end as NRSNG_FAC_DAYS_CNT,
    trim(ADMTG_PRVDR_NPI_NUM) as ADMTG_PRVDR_NPI_NUM,
    trim(ADMTG_PRVDR_NUM) as ADMTG_PRVDR_NUM,
    case
        when regexp_count(lpad(ADMTG_PRVDR_SPCLTY_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                ADMTG_PRVDR_SPCLTY_CD >= 1
                and ADMTG_PRVDR_SPCLTY_CD <= 98
            ) then upper(lpad(ADMTG_PRVDR_SPCLTY_CD, 2, '0'))
            else NULL
        end
        else case
            when upper(ADMTG_PRVDR_SPCLTY_CD) in (
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
            ) then upper(lpad(ADMTG_PRVDR_SPCLTY_CD, 2, '0'))
            else NULL
        end
    end as ADMTG_PRVDR_SPCLTY_CD,
    case
        when regexp_count(trim(ADMTG_PRVDR_TXNMY_CD), '[^0]+') = 0
        or ADMTG_PRVDR_TXNMY_CD in (
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
        else ADMTG_PRVDR_TXNMY_CD
    end as ADMTG_PRVDR_TXNMY_CD,
    case
        when regexp_count(lpad(ADMTG_PRVDR_TYPE_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                ADMTG_PRVDR_TYPE_CD >= 1
                and ADMTG_PRVDR_TYPE_CD <= 57
            ) then lpad(ADMTG_PRVDR_TYPE_CD, 2, '0')
            else NULL
        end
        else NULL
    end as ADMTG_PRVDR_TYPE_CD,
    trim(BLG_PRVDR_NPI_NUM) as BLG_PRVDR_NPI_NUM,
    trim(BLG_PRVDR_NUM) as BLG_PRVDR_NUM,
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
    end as BLG_PRVDR_TXNMY_CD,
    case
        when regexp_count(lpad(BLG_PRVDR_TYPE_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                BLG_PRVDR_TYPE_CD >= 1
                and BLG_PRVDR_TYPE_CD <= 57
            ) then lpad(BLG_PRVDR_TYPE_CD, 2, '0')
            else NULL
        end
        else NULL
    end as BLG_PRVDR_TYPE_CD,
    case
        when regexp_count(lpad(BLG_PRVDR_SPCLTY_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                BLG_PRVDR_SPCLTY_CD >= 1
                and BLG_PRVDR_SPCLTY_CD <= 98
            ) then upper(lpad(BLG_PRVDR_SPCLTY_CD, 2, '0'))
            else NULL
        end
        else case
            when upper(BLG_PRVDR_SPCLTY_CD) in (
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
            ) then upper(lpad(BLG_PRVDR_SPCLTY_CD, 2, '0'))
            else NULL
        end
    end as BLG_PRVDR_SPCLTY_CD,
    trim(RFRG_PRVDR_NUM) as RFRG_PRVDR_NUM,
    trim(RFRG_PRVDR_NPI_NUM) as RFRG_PRVDR_NPI_NUM,
    case
        when regexp_count(lpad(RFRG_PRVDR_TYPE_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                RFRG_PRVDR_TYPE_CD >= 1
                and RFRG_PRVDR_TYPE_CD <= 57
            ) then lpad(RFRG_PRVDR_TYPE_CD, 2, '0')
            else NULL
        end
        else NULL
    end as RFRG_PRVDR_TYPE_CD,
    case
        when regexp_count(lpad(RFRG_PRVDR_SPCLTY_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                RFRG_PRVDR_SPCLTY_CD >= 1
                and RFRG_PRVDR_SPCLTY_CD <= 98
            ) then upper(lpad(RFRG_PRVDR_SPCLTY_CD, 2, '0'))
            else NULL
        end
        else case
            when upper(RFRG_PRVDR_SPCLTY_CD) in (
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
            ) then upper(lpad(RFRG_PRVDR_SPCLTY_CD, 2, '0'))
            else NULL
        end
    end as RFRG_PRVDR_SPCLTY_CD,
    trim(PRVDR_LCTN_ID) as PRVDR_LCTN_ID,
    case
        when DAILY_RATE in (88888.80, 88888.00, 88888.88) then NULL
        else DAILY_RATE
    end as DAILY_RATE,
    case
        when PYMT_LVL_IND is NOT NULL
        and trim(PYMT_LVL_IND) in ('1', '2') then trim(PYMT_LVL_IND)
        else NULL
    end as PYMT_LVL_IND,
    case
        when LTC_RCP_LBLTY_AMT in (9999999999.99, 888888888.88) then NULL
        else LTC_RCP_LBLTY_AMT
    end as LTC_RCP_LBLTY_AMT,
    case
        when MDCR_PD_AMT in (
            9999999999.99,
            888888888.88,
            88888888888.00,
            88888888888.88,
            8888888.88,
            99999999999.00
        ) then NULL
        else MDCR_PD_AMT
    end as MDCR_PD_AMT,
    case
        when TOT_BILL_AMT in (
            9999999.99,
            888888888.88,
            99999999.90,
            999999.99,
            999999
        ) then NULL
        else TOT_BILL_AMT
    end as TOT_BILL_AMT,
    case
        when TOT_ALOWD_AMT in (888888888.88, 99999999.00) then NULL
        else TOT_ALOWD_AMT
    end as TOT_ALOWD_AMT,
    case
        when TOT_MDCD_PD_AMT in (888888888.88) then NULL
        else TOT_MDCD_PD_AMT
    end as TOT_MDCD_PD_AMT,
    case
        when TOT_MDCR_DDCTBL_AMT in (888888888.88, 99999, 88888888888.00) then NULL
        else TOT_MDCR_DDCTBL_AMT
    end as TOT_MDCR_DDCTBL_AMT,
    case
        when TOT_MDCR_COINSRNC_AMT in (888888888.88) then NULL
        else TOT_MDCR_COINSRNC_AMT
    end as TOT_MDCR_COINSRNC_AMT,
    case
        when TOT_TPL_AMT in (888888888.88, 999999.99) then NULL
        else TOT_TPL_AMT
    end as TOT_TPL_AMT,
    case
        when TOT_OTHR_INSRNC_AMT in (888888888.88) then NULL
        else TOT_OTHR_INSRNC_AMT
    end as TOT_OTHR_INSRNC_AMT,
    case
        when TP_COINSRNC_PD_AMT in (888888888.88) then NULL
        else TP_COINSRNC_PD_AMT
    end as TP_COINSRNC_PD_AMT,
    case
        when TP_COPMT_PD_AMT in (
            88888888888,
            888888888.00,
            888888888.88,
            99999999999.00
        ) then NULL
        else TP_COPMT_PD_AMT
    end as TP_COPMT_PD_AMT,
    case
        when MDCR_CMBND_DDCTBL_IND is NOT NULL
        and trim(MDCR_CMBND_DDCTBL_IND) in ('0', '1') then trim(MDCR_CMBND_DDCTBL_IND)
        else NULL
    end as MDCR_CMBND_DDCTBL_IND,
    case
        when MDCR_REIMBRSMT_TYPE_CD is NOT NULL
        and lpad(trim(MDCR_REIMBRSMT_TYPE_CD), 2, '0') in (
            '01',
            '02',
            '03',
            '04',
            '05',
            '06',
            '07',
            '08',
            '09'
        ) then lpad(trim(MDCR_REIMBRSMT_TYPE_CD), 2, '0')
        else NULL
    end as MDCR_REIMBRSMT_TYPE_CD,
    case
        when BENE_COINSRNC_AMT in (888888888.88, 888888888.00, 88888888888.00) then NULL
        else BENE_COINSRNC_AMT
    end as BENE_COINSRNC_AMT,
    case
        when BENE_COPMT_AMT in (888888888.88, 888888888.00, 88888888888.00) then NULL
        else BENE_COPMT_AMT
    end as BENE_COPMT_AMT,
    case
        when BENE_DDCTBL_AMT in (888888888.88, 888888888.00, 88888888888.00) then NULL
        else BENE_DDCTBL_AMT
    end as BENE_DDCTBL_AMT,
    case
        when COPAY_WVD_IND is NOT NULL
        and trim(COPAY_WVD_IND) in ('0', '1') then trim(COPAY_WVD_IND)
        else NULL
    end as COPAY_WVD_IND,
    case
        when (OCRNC_01_CD_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_01_CD_EFCTV_DT
    end as OCRNC_01_CD_EFCTV_DT,
    case
        when (OCRNC_01_CD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_01_CD_END_DT
    end as OCRNC_01_CD_END_DT,
    trim(OCRNC_01_CD) as OCRNC_01_CD,
    case
        when (OCRNC_02_CD_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_02_CD_EFCTV_DT
    end as OCRNC_02_CD_EFCTV_DT,
    case
        when (OCRNC_02_CD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_02_CD_END_DT
    end as OCRNC_02_CD_END_DT,
    trim(OCRNC_02_CD) as OCRNC_02_CD,
    case
        when (OCRNC_03_CD_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_03_CD_EFCTV_DT
    end as OCRNC_03_CD_EFCTV_DT,
    case
        when (OCRNC_03_CD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_03_CD_END_DT
    end as OCRNC_03_CD_END_DT,
    trim(OCRNC_03_CD) as OCRNC_03_CD,
    case
        when (OCRNC_04_CD_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_04_CD_EFCTV_DT
    end as OCRNC_04_CD_EFCTV_DT,
    case
        when (OCRNC_04_CD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_04_CD_END_DT
    end as OCRNC_04_CD_END_DT,
    trim(OCRNC_04_CD) as OCRNC_04_CD,
    case
        when (OCRNC_05_CD_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_05_CD_EFCTV_DT
    end as OCRNC_05_CD_EFCTV_DT,
    case
        when (OCRNC_05_CD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_05_CD_END_DT
    end as OCRNC_05_CD_END_DT,
    trim(OCRNC_05_CD) as OCRNC_05_CD,
    case
        when (OCRNC_06_CD_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_06_CD_EFCTV_DT
    end as OCRNC_06_CD_EFCTV_DT,
    case
        when (OCRNC_06_CD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_06_CD_END_DT
    end as OCRNC_06_CD_END_DT,
    trim(OCRNC_06_CD) as OCRNC_06_CD,
    case
        when (OCRNC_07_CD_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_07_CD_EFCTV_DT
    end as OCRNC_07_CD_EFCTV_DT,
    case
        when (OCRNC_07_CD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_07_CD_END_DT
    end as OCRNC_07_CD_END_DT,
    trim(OCRNC_07_CD) as OCRNC_07_CD,
    case
        when (OCRNC_08_CD_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_08_CD_EFCTV_DT
    end as OCRNC_08_CD_EFCTV_DT,
    case
        when (OCRNC_08_CD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_08_CD_END_DT
    end as OCRNC_08_CD_END_DT,
    trim(OCRNC_08_CD) as OCRNC_08_CD,
    case
        when (OCRNC_09_CD_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_09_CD_EFCTV_DT
    end as OCRNC_09_CD_EFCTV_DT,
    case
        when (OCRNC_09_CD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_09_CD_END_DT
    end as OCRNC_09_CD_END_DT,
    trim(OCRNC_09_CD) as OCRNC_09_CD,
    case
        when (OCRNC_10_CD_EFCTV_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_10_CD_EFCTV_DT
    end as OCRNC_10_CD_EFCTV_DT,
    case
        when (OCRNC_10_CD_END_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else OCRNC_10_CD_END_DT
    end as OCRNC_10_CD_END_DT,
    trim(OCRNC_10_CD) as OCRNC_10_CD,
    case
        when SPLIT_CLM_IND is NOT NULL
        and trim(SPLIT_CLM_IND) in ('0', '1') then trim(SPLIT_CLM_IND)
        else NULL
    end as SPLIT_CLM_IND,
    CLL_CNT,
    NUM_CLL,
    ACCOMMODATION_PAID as ACMDTN_PD,
    ANCILLARY_PAID as ANCLRY_PD,
    CVRD_MH_DAYS_OVER_65 as CVRD_MH_DAYS_OVR_65,
    CVRD_MH_DAYS_UNDER_21,
    LT_MH_DX_IND,
    LT_SUD_DX_IND,
    LT_MH_TAXONOMY_IND as LT_MH_TXNMY_IND,
    LT_SUD_TAXONOMY_IND as LT_SUD_TXNMY_IND,
    nullif(IAP_CONDITION_IND, IAP_CONDITION_IND) as IAP_COND_IND,
    nullif(
        PRIMARY_HIERARCHICAL_CONDITION,
        PRIMARY_HIERARCHICAL_CONDITION
    ) as PRMRY_HIRCHCL_COND
FROM
    (
        select
            *,
            case
                when ADJSTMT_IND is NOT NULL
                and trim(ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6') then trim(ADJSTMT_IND)
                else NULL
            end as ADJSTMT_IND_CLEAN
        from
            LT_HEADER_GROUPER
    ) H