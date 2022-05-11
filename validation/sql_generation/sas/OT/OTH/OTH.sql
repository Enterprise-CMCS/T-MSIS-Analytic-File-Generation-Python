create temp table OTH distkey (ORGNL_CLM_NUM) as
select
    6609 as DA_RUN_ID,
    cast (
        (
            '48' || '-' || 201710 || '-' || NEW_SUBMTG_STATE_CD || '-' || trim(COALESCE(NULLIF(ORGNL_CLM_NUM, '~'), '0')) || '-' || trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM, '~'), '0')) || '-' || CAST(DATE_PART_YEAR(ADJDCTN_DT) AS CHAR(4)) || CAST(DATE_PART(MONTH, ADJDCTN_DT) AS CHAR(2)) || CAST(DATE_PART(DAY, ADJDCTN_DT) AS CHAR(2)) || '-' || COALESCE(ADJSTMT_IND_CLEAN, 'X')
        ) as varchar(126)
    ) as OT_LINK_KEY,
    '48' as OT_VRSN,
    '201710' as OT_FIL_DT,
    TMSIS_RUN_ID,
    trim(MSIS_IDENT_NUM) as MSIS_IDENT_NUM,
    NEW_SUBMTG_STATE_CD as SUBMTG_STATE_CD,
    case
        when orgnl_clm_num in ('~')
        or nullif(trim('~'), '') = NULL then NULL
        else orgnl_clm_num
    end as orgnl_clm_num,
    case
        when adjstmt_clm_num in ('~')
        or nullif(trim('~'), '') = NULL then NULL
        else adjstmt_clm_num
    end as adjstmt_clm_num,
    ADJSTMT_IND_CLEAN as ADJSTMT_IND,
    case
        when regexp_count(lpad(ADJSTMT_RSN_CD, 3, '0'), '[0-9]{3}') > 0 then case
            when (
                ADJSTMT_RSN_CD :: smallint >= 1
                and ADJSTMT_RSN_CD :: smallint <= 99
            ) then upper(lpad(ADJSTMT_RSN_CD, 3, '0'))
            else ADJSTMT_RSN_CD
        end
        else ADJSTMT_RSN_CD
    end as ADJSTMT_RSN_CD,
    case
        when date_cmp(SRVC_BGNNG_DT_HEADER, '1600-01-01') = -1 then '1599-12-31' :: date
        else nullif(SRVC_BGNNG_DT_HEADER, '01JAN1960')
    end as SRVC_BGNNG_DT,
    nullif(SRVC_ENDG_DT_HEADER, '01JAN1960') as SRVC_ENDG_DT,
    case
        when date_cmp(ADJDCTN_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else nullif(ADJDCTN_DT, '01JAN1960')
    end as ADJDCTN_DT,
    case
        when date_cmp(MDCD_PD_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else MDCD_PD_DT
    end as MDCD_PD_DT,
    case
        when SECT_1115A_DEMO_IND is NOT NULL
        and trim(SECT_1115A_DEMO_IND) in ('0', '1') then trim(SECT_1115A_DEMO_IND)
        else NULL
    end as SECT_1115A_DEMO_IND,
    case
        when upper(clm_type_cd) in(
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
        ) then upper(clm_type_cd)
        else NULL
    end as clm_type_cd,
    trim(BILL_TYPE_CD) as BILL_TYPE_CD,
    case
        when lpad(pgm_type_cd, 2, '0') in ('06', '09') then NULL
        else case
            when pgm_type_cd is not NULL
            and regexp_count(lpad(pgm_type_cd, 2, '0'), '[0-9]{2}') > 0 then case
                when (
                    pgm_type_cd :: smallint >= 0
                    and pgm_type_cd :: smallint <= 17
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
        when date_cmp(BIRTH_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else BIRTH_DT
    end as BIRTH_DT,
    case
        when lpad(wvr_type_cd, 2, '0') = '88' then NULL
        else case
            when wvr_type_cd is not NULL
            and regexp_count(lpad(wvr_type_cd, 2, '0'), '[0-9]{2}') > 0 then case
                when (
                    wvr_type_cd :: smallint >= 1
                    and wvr_type_cd :: smallint <= 33
                ) then lpad(wvr_type_cd, 2, '0')
                else NULL
            end
            else NULL
        end
    end as wvr_type_cd,
    trim(WVR_ID) as WVR_ID,
    case
        when srvc_trkng_type_cd is NOT NULL
        and lpad(trim(srvc_trkng_type_cd), 2, '0') in ('00', '01', '02', '03', '04', '05', '06') then lpad(trim(srvc_trkng_type_cd), 2, '0')
        else NULL
    end as srvc_trkng_type_cd,
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
        when othr_tpl_clctn_cd is NOT NULL
        and lpad(trim(othr_tpl_clctn_cd), 3, '0') in (
            '000',
            '001',
            '002',
            '003',
            '004',
            '005',
            '006',
            '007'
        ) then lpad(trim(othr_tpl_clctn_cd), 3, '0')
        else NULL
    end as othr_tpl_clctn_cd,
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
    case
        when HLTH_CARE_ACQRD_COND_CD is NOT NULL
        and trim(HLTH_CARE_ACQRD_COND_CD) in ('0', '1') then trim(HLTH_CARE_ACQRD_COND_CD)
        else NULL
    end as HLTH_CARE_ACQRD_COND_CD,
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
        when SRVC_PLC_CD is NOT NULL then lpad(trim(SRVC_PLC_CD), 2, '0')
        else NULL
    end as SRVC_PLC_CD,
    trim(PRVDR_LCTN_ID) as PRVDR_LCTN_ID,
    trim(BLG_PRVDR_NUM) as BLG_PRVDR_NUM,
    trim(BLG_PRVDR_NPI_NUM) as BLG_PRVDR_NPI_NUM,
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
        when regexp_count(lpad(blg_prvdr_type_cd, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                blg_prvdr_type_cd :: smallint >= 1
                and blg_prvdr_type_cd :: smallint <= 57
            ) then lpad(blg_prvdr_type_cd, 2, '0')
            else NULL
        end
        else NULL
    end as blg_prvdr_type_cd,
    case
        when regexp_count(lpad(BLG_PRVDR_SPCLTY_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                BLG_PRVDR_SPCLTY_CD :: smallint >= 1
                and BLG_PRVDR_SPCLTY_CD :: smallint <= 98
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
        when regexp_count(trim(RFRG_PRVDR_TXNMY_CD), '[^0]+') = 0
        or RFRG_PRVDR_TXNMY_CD in (
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
        else RFRG_PRVDR_TXNMY_CD
    end as RFRG_PRVDR_TXNMY_CD,
    case
        when regexp_count(lpad(rfrg_prvdr_type_cd, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                rfrg_prvdr_type_cd :: smallint >= 1
                and rfrg_prvdr_type_cd :: smallint <= 57
            ) then lpad(rfrg_prvdr_type_cd, 2, '0')
            else NULL
        end
        else NULL
    end as rfrg_prvdr_type_cd,
    case
        when regexp_count(lpad(RFRG_PRVDR_SPCLTY_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                RFRG_PRVDR_SPCLTY_CD :: smallint >= 1
                and RFRG_PRVDR_SPCLTY_CD :: smallint <= 98
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
    trim(PRVDR_UNDER_DRCTN_NPI_NUM) as PRVDR_UNDER_DRCTN_NPI_NUM,
    case
        when regexp_count(trim(PRVDR_UNDER_DRCTN_TXNMY_CD), '[^0]+') = 0
        or PRVDR_UNDER_DRCTN_TXNMY_CD in (
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
        else PRVDR_UNDER_DRCTN_TXNMY_CD
    end as PRVDR_UNDER_DRCTN_TXNMY_CD,
    trim(PRVDR_UNDER_SPRVSN_NPI_NUM) as PRVDR_UNDER_SPRVSN_NPI_NUM,
    case
        when regexp_count(trim(PRVDR_UNDER_SPRVSN_TXNMY_CD), '[^0]+') = 0
        or PRVDR_UNDER_SPRVSN_TXNMY_CD in (
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
        else PRVDR_UNDER_SPRVSN_TXNMY_CD
    end as PRVDR_UNDER_SPRVSN_TXNMY_CD,
    case
        when HH_PRVDR_IND is NOT NULL
        and trim(HH_PRVDR_IND) in ('0', '1') then trim(HH_PRVDR_IND)
        else NULL
    end as HH_PRVDR_IND,
    trim(HH_PRVDR_NPI_NUM) as HH_PRVDR_NPI_NUM,
    trim(HH_ENT_NAME) as HH_ENT_NAME,
    trim(RMTNC_NUM) as RMTNC_NUM,
    case
        when DAILY_RATE in (88888, 88888.80, 88888.88) then NULL
        else DAILY_RATE
    end as DAILY_RATE,
    case
        when PYMT_LVL_IND is NOT NULL
        and trim(PYMT_LVL_IND) in ('1', '2') then trim(PYMT_LVL_IND)
        else NULL
    end as PYMT_LVL_IND,
    case
        when TOT_BILL_AMT in (
            999999.00,
            888888888.88,
            9999999.99,
            99999999.90,
            999999.99,
            9999999999.99
        ) then NULL
        else TOT_BILL_AMT
    end as TOT_BILL_AMT,
    case
        when TOT_ALOWD_AMT in (99999999, 888888888.88) then NULL
        else TOT_ALOWD_AMT
    end as TOT_ALOWD_AMT,
    case
        when TOT_MDCD_PD_AMT in (888888888.88) then NULL
        else TOT_MDCD_PD_AMT
    end as TOT_MDCD_PD_AMT,
    case
        when TOT_COPAY_AMT in (888888888.88, 9999999.99, 88888888888.00) then NULL
        else TOT_COPAY_AMT
    end as TOT_COPAY_AMT,
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
        when TOT_OTHR_INSRNC_AMT in (888888888.8, 888888888.88) then NULL
        else TOT_OTHR_INSRNC_AMT
    end as TOT_OTHR_INSRNC_AMT,
    case
        when TP_COINSRNC_PD_AMT in (888888888.88) then NULL
        else TP_COINSRNC_PD_AMT
    end as TP_COINSRNC_PD_AMT,
    case
        when TP_COPMT_PD_AMT in (
            888888888.88,
            888888888,
            888888888.00,
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
        when mdcr_reimbrsmt_type_cd is NOT NULL
        and lpad(trim(mdcr_reimbrsmt_type_cd), 2, '0') in (
            '01',
            '02',
            '03',
            '04',
            '05',
            '06',
            '07',
            '08',
            '09'
        ) then lpad(trim(mdcr_reimbrsmt_type_cd), 2, '0')
        else NULL
    end as mdcr_reimbrsmt_type_cd,
    case
        when BENE_COINSRNC_AMT in (888888888.88, 888888888, 88888888888.00) then NULL
        else BENE_COINSRNC_AMT
    end as BENE_COINSRNC_AMT,
    case
        when date_cmp(BENE_COINSRNC_PD_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else BENE_COINSRNC_PD_DT
    end as BENE_COINSRNC_PD_DT,
    case
        when BENE_COPMT_AMT in (888888888.88, 888888888, 88888888888.00) then NULL
        else BENE_COPMT_AMT
    end as BENE_COPMT_AMT,
    case
        when date_cmp(BENE_COPMT_PD_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else BENE_COPMT_PD_DT
    end as BENE_COPMT_PD_DT,
    case
        when BENE_DDCTBL_AMT in (888888888.88, 888888888, 88888888888.00) then NULL
        else BENE_DDCTBL_AMT
    end as BENE_DDCTBL_AMT,
    case
        when date_cmp(BENE_DDCTBL_PD_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else BENE_DDCTBL_PD_DT
    end as BENE_DDCTBL_PD_DT,
    case
        when COPAY_WVD_IND is NOT NULL
        and trim(COPAY_WVD_IND) in ('0', '1') then trim(COPAY_WVD_IND)
        else NULL
    end as COPAY_WVD_IND,
    case
        when date_cmp(CPTATD_AMT_RQSTD_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else CPTATD_AMT_RQSTD_DT
    end as CPTATD_AMT_RQSTD_DT,
    case
        when CPTATD_PYMT_RQSTD_AMT in (888888888.88, 888888888) then NULL
        else CPTATD_PYMT_RQSTD_AMT
    end as CPTATD_PYMT_RQSTD_AMT,
    trim(OCRNC_01_CD) as OCRNC_01_CD,
    trim(OCRNC_02_CD) as OCRNC_02_CD,
    trim(OCRNC_03_CD) as OCRNC_03_CD,
    trim(OCRNC_04_CD) as OCRNC_04_CD,
    trim(OCRNC_05_CD) as OCRNC_05_CD,
    trim(OCRNC_06_CD) as OCRNC_06_CD,
    trim(OCRNC_07_CD) as OCRNC_07_CD,
    trim(OCRNC_08_CD) as OCRNC_08_CD,
    trim(OCRNC_09_CD) as OCRNC_09_CD,
    trim(OCRNC_10_CD) as OCRNC_10_CD,
    case
        when date_cmp(OCRNC_01_CD_EFCTV_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_01_CD_EFCTV_DT
    end as OCRNC_01_CD_EFCTV_DT,
    case
        when date_cmp(OCRNC_02_CD_EFCTV_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_02_CD_EFCTV_DT
    end as OCRNC_02_CD_EFCTV_DT,
    case
        when date_cmp(OCRNC_03_CD_EFCTV_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_03_CD_EFCTV_DT
    end as OCRNC_03_CD_EFCTV_DT,
    case
        when date_cmp(OCRNC_04_CD_EFCTV_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_04_CD_EFCTV_DT
    end as OCRNC_04_CD_EFCTV_DT,
    case
        when date_cmp(OCRNC_05_CD_EFCTV_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_05_CD_EFCTV_DT
    end as OCRNC_05_CD_EFCTV_DT,
    case
        when date_cmp(OCRNC_06_CD_EFCTV_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_06_CD_EFCTV_DT
    end as OCRNC_06_CD_EFCTV_DT,
    case
        when date_cmp(OCRNC_07_CD_EFCTV_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_07_CD_EFCTV_DT
    end as OCRNC_07_CD_EFCTV_DT,
    case
        when date_cmp(OCRNC_08_CD_EFCTV_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_08_CD_EFCTV_DT
    end as OCRNC_08_CD_EFCTV_DT,
    case
        when date_cmp(OCRNC_09_CD_EFCTV_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_09_CD_EFCTV_DT
    end as OCRNC_09_CD_EFCTV_DT,
    case
        when date_cmp(OCRNC_10_CD_EFCTV_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_10_CD_EFCTV_DT
    end as OCRNC_10_CD_EFCTV_DT,
    case
        when date_cmp(OCRNC_01_CD_END_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_01_CD_END_DT
    end as OCRNC_01_CD_END_DT,
    case
        when date_cmp(OCRNC_02_CD_END_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_02_CD_END_DT
    end as OCRNC_02_CD_END_DT,
    case
        when date_cmp(OCRNC_03_CD_END_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_03_CD_END_DT
    end as OCRNC_03_CD_END_DT,
    case
        when date_cmp(OCRNC_04_CD_END_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_04_CD_END_DT
    end as OCRNC_04_CD_END_DT,
    case
        when date_cmp(OCRNC_05_CD_END_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_05_CD_END_DT
    end as OCRNC_05_CD_END_DT,
    case
        when date_cmp(OCRNC_06_CD_END_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_06_CD_END_DT
    end as OCRNC_06_CD_END_DT,
    case
        when date_cmp(OCRNC_07_CD_END_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_07_CD_END_DT
    end as OCRNC_07_CD_END_DT,
    case
        when date_cmp(OCRNC_08_CD_END_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_08_CD_END_DT
    end as OCRNC_08_CD_END_DT,
    case
        when date_cmp(OCRNC_09_CD_END_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_09_CD_END_DT
    end as OCRNC_09_CD_END_DT,
    case
        when date_cmp(OCRNC_10_CD_END_DT, '1600-01-01') = -1 then '1599-12-31' :: date
        else OCRNC_10_CD_END_DT
    end as OCRNC_10_CD_END_DT,
    CLL_CNT,
    NUM_CLL,
    OT_MH_DX_IND,
    OT_SUD_DX_IND,
    OT_MH_TAXONOMY_IND,
    OT_SUD_TAXONOMY_IND,
    cast(
        nullif(IAP_CONDITION_IND, IAP_CONDITION_IND) as char(6)
    ) as IAP_COND_IND,
    cast(
        nullif(
            PRIMARY_HIERARCHICAL_CONDITION,
            PRIMARY_HIERARCHICAL_CONDITION
        ) as char(9)
    ) as PRMRY_HIRCHCL_COND,
    CONVERT_TIMEZONE('EDT', GETDATE()) as REC_ADD_TS,
    CONVERT_TIMEZONE('EDT', GETDATE()) as REC_UPDT_TS,
    case
        when date_cmp(SRVC_ENDG_DT_DRVD, '1600-01-01') = -1 then '1599-12-31' :: date
        else SRVC_ENDG_DT_DRVD
    end as SRVC_ENDG_DT_DRVD,
    case
        when SRVC_ENDG_DT_CD is NOT NULL
        and trim(SRVC_ENDG_DT_CD) in ('1', '2', '3', '4', '5') then trim(SRVC_ENDG_DT_CD)
        else NULL
    end as SRVC_ENDG_DT_CD,
    case
        when regexp_count(trim(BLG_PRVDR_NPPES_TXNMY_CD), '[^0]+') = 0
        or BLG_PRVDR_NPPES_TXNMY_CD in (
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
        else BLG_PRVDR_NPPES_TXNMY_CD
    end as BLG_PRVDR_NPPES_TXNMY_CD,
    DGNS_1_CCSR_DFLT_CTGRY_CD
from
    (
        select
            *,
            case
                when ADJSTMT_IND is NOT NULL
                and trim(ADJSTMT_IND) in ('0', '1', '2', '3', '4', '5', '6') then trim(ADJSTMT_IND)
                else NULL
            end as ADJSTMT_IND_CLEAN
        from
            OT_HEADER_GROUPER
    ) H