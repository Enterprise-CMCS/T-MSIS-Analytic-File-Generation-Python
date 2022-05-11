create
or replace temporary view RXH as
select
    6194 as DA_RUN_ID,
    'rx_link_key' as RX_LINK_KEY,
    '0A' as RX_VRSN,
    '202110' as RX_FIL_DT,
    tmsis_run_id,
    trim(MSIS_IDENT_NUM) as MSIS_IDENT_NUM,
    new_submtg_state_cd as submtg_state_cd,
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
                ADJSTMT_RSN_CD >= 1
                and ADJSTMT_RSN_CD <= 99
            ) then upper(lpad(ADJSTMT_RSN_CD, 3, '0'))
            else ADJSTMT_RSN_CD
        end
        else ADJSTMT_RSN_CD
    end as ADJSTMT_RSN_CD,
    case
        when (ADJDCTN_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else nullif(ADJDCTN_DT < to_date('01JAN1960'))
    end as ADJDCTN_DT,
    case
        when (mdcd_pd_dt < to_date('1600-01-01')) then to_date('1599-12-31')
        else mdcd_pd_dt
    end as mdcd_pd_dt,
    rx_fill_dt,
    case
        when (prscrbd_dt < to_date('1600-01-01')) then to_date('1599-12-31')
        else prscrbd_dt
    end as prscrbd_dt,
    case
        when CMPND_DRUG_IND is NOT NULL
        and trim(CMPND_DRUG_IND) in ('0', '1') then trim(CMPND_DRUG_IND)
        else NULL
    end as CMPND_DRUG_IND,
    case
        when SECT_1115A_DEMO_IND is NOT NULL
        and trim(SECT_1115A_DEMO_IND) in ('0', '1') then trim(SECT_1115A_DEMO_IND)
        else NULL
    end as SECT_1115A_DEMO_IND,
    case
        when upper(clm_type_cd) in (
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
        when wvr_type_cd is not NULL
        and regexp_count(lpad(wvr_type_cd, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                wvr_type_cd >= 1
                and wvr_type_cd <= 33
            ) then lpad(wvr_type_cd, 2, '0')
            else NULL
        end
        else NULL
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
        and lpad(trim(othr_tpl_clctn_cd), 3, '0') in ('000', '001', '002', '003', '004', '005', '006', '007') then lpad(trim(othr_tpl_clctn_cd), 3, '0')
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
    trim(PRSCRBNG_PRVDR_NUM) as PRSCRBNG_PRVDR_NUM,
    trim(SRVCNG_PRVDR_NPI_NUM) as SRVCNG_PRVDR_NPI_NUM,
    trim(DSPNSNG_PD_PRVDR_NPI_NUM) as DSPNSNG_PD_PRVDR_NPI_NUM,
    trim(DSPNSNG_PD_PRVDR_NUM) as DSPNSNG_PD_PRVDR_NUM,
    trim(PRVDR_LCTN_ID) as PRVDR_LCTN_ID,
    case
        when PYMT_LVL_IND is NOT NULL
        and trim(PYMT_LVL_IND) in ('1', '2') then trim(PYMT_LVL_IND)
        else NULL
    end as PYMT_LVL_IND,
    case
        when tot_bill_amt in (
            999999.99,
            69999999999.93,
            999999.00,
            888888888.88,
            9999999.99,
            99999999.90
        ) then NULL
        else tot_bill_amt
    end as tot_bill_amt,
    case
        when tot_alowd_amt in (888888888.88, 99999999.00) then NULL
        else tot_alowd_amt
    end as tot_alowd_amt,
    case
        when tot_mdcd_pd_amt in (999999.99, 888888888.88) then NULL
        else tot_mdcd_pd_amt
    end as tot_mdcd_pd_amt,
    case
        when tot_copay_amt in (88888888888.00, 888888888.88, 9999999.99) then NULL
        else tot_copay_amt
    end as tot_copay_amt,
    case
        when tot_tpl_amt in (999999.99, 888888888.88) then NULL
        else tot_tpl_amt
    end as tot_tpl_amt,
    case
        when tot_othr_insrnc_amt in (888888888.88) then NULL
        else tot_othr_insrnc_amt
    end as tot_othr_insrnc_amt,
    case
        when tot_mdcr_ddctbl_amt in (99999, 88888888888.00, 888888888.88) then NULL
        else tot_mdcr_ddctbl_amt
    end as tot_mdcr_ddctbl_amt,
    case
        when tot_mdcr_coinsrnc_amt in (888888888.88) then NULL
        else tot_mdcr_coinsrnc_amt
    end as tot_mdcr_coinsrnc_amt,
    case
        when TP_COINSRNC_PD_AMT in (888888888.88) then NULL
        else TP_COINSRNC_PD_AMT
    end as TP_COINSRNC_PD_AMT,
    case
        when TP_COPMT_PD_AMT in (
            99999999999.00,
            888888888.88,
            888888888.00,
            88888888888.00
        ) then NULL
        else TP_COPMT_PD_AMT
    end as TP_COPMT_PD_AMT,
    case
        when bene_coinsrnc_amt in (888888888.88, 888888888.00, 88888888888.00) then NULL
        else bene_coinsrnc_amt
    end as bene_coinsrnc_amt,
    case
        when bene_copmt_amt in (88888888888.00, 888888888.88, 888888888.00) then NULL
        else bene_copmt_amt
    end as bene_copmt_amt,
    case
        when bene_ddctbl_amt in (88888888888.00, 888888888.88, 888888888.00) then NULL
        else bene_ddctbl_amt
    end as bene_ddctbl_amt,
    case
        when COPAY_WVD_IND is NOT NULL
        and trim(COPAY_WVD_IND) in ('0', '1') then trim(COPAY_WVD_IND)
        else NULL
    end as COPAY_WVD_IND,
    cll_cnt,
    num_cll
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
            RX_HEADER
    ) H
