create
or replace temporary view IPH as
select
    6194 as DA_RUN_ID,
    cast (
        (
            concat(
                '0A',
                '-',
                202110,
                '-',
                NEW_SUBMTG_STATE_CD,
                '-',
                trim(COALESCE(NULLIF(ORGNL_CLM_NUM, '~'), '0')),
                '-',
                trim(COALESCE(NULLIF(ADJSTMT_CLM_NUM, '~'), '0')),
                '-',
                CAST(DATE_PART_YEAR(ADJDCTN_DT) AS CHAR(4)),
                CAST(DATE_PART(MONTH, ADJDCTN_DT) AS CHAR(2)),
                CAST(DATE_PART(DAY, ADJDCTN_DT) AS CHAR(2)),
                '-',
                COALESCE(ADJSTMT_IND_CLEAN, 'X')
            )
        ) as varchar(126)
    ) as IP_LINK_KEY,
    '0A' as IP_VRSN,
    '202110' as IP_FIL_DT,
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
                ADJSTMT_RSN_CD >= 1
                and ADJSTMT_RSN_CD <= 99
            ) then upper(lpad(ADJSTMT_RSN_CD, 3, '0'))
            else ADJSTMT_RSN_CD
        end
        else ADJSTMT_RSN_CD
    end as ADJSTMT_RSN_CD,
    case
        when (ADMSN_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else ADMSN_DT
    end as ADMSN_DT,
    case
        when ADMSN_HR_NUM is not NULL
        and regexp_count(lpad(ADMSN_HR_NUM, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                ADMSN_HR_NUM >= 0
                and ADMSN_HR_NUM <= 23
            ) then lpad(ADMSN_HR_NUM, 2, '0')
            else NULL
        end
        else NULL
    end as ADMSN_HR_NUM,
    nullif(DSCHRG_DT, to_date(to_date('1960-01-01'))) as DSCHRG_DT,
    case
        when DSCHRG_HR_NUM is not NULL
        and regexp_count(lpad(DSCHRG_HR_NUM, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                DSCHRG_HR_NUM >= 0
                and DSCHRG_HR_NUM <= 23
            ) then lpad(DSCHRG_HR_NUM, 2, '0')
            else NULL
        end
        else NULL
    end as DSCHRG_HR_NUM,
    case
        when (ADJDCTN_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else nulllif(ADJDCTN_DT, to_date('1960-01-01'))
    end as ADJDCTN_DT,
    case
        when (MDCD_PD_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else MDCD_PD_DT
    end as MDCD_PD_DT,
    case
        when ADMSN_TYPE_CD is NOT NULL
        and trim(ADMSN_TYPE_CD) in ('1', '2', '3', '4', '5') then trim(ADMSN_TYPE_CD)
        else NULL
    end as ADMSN_TYPE_CD,
    case
        when HOSP_TYPE_CD is NOT NULL
        and lpad(trim(HOSP_TYPE_CD), 2, '0') in ('00', '01', '02', '03', '04', '05', '06', '07', '08') then lpad(trim(HOSP_TYPE_CD), 2, '0')
        else NULL
    end as HOSP_TYPE_CD,
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
        when BIRTH_WT_GRMS_QTY <= 0
        or BIRTH_WT_GRMS_QTY in (888889.000, 88888.888, 888888.000) then NULL
        else cast(BIRTH_WT_GRMS_QTY as decimal(9, 3))
    end as BIRTH_WT_GRMS_QTY,
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
        when (upper(DGNS_POA_1_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_1_CD_IND)
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
        when (upper(DGNS_POA_2_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_2_CD_IND)
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
        when (upper(DGNS_POA_3_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_3_CD_IND)
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
        when (upper(DGNS_POA_4_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_4_CD_IND)
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
        when (upper(DGNS_POA_5_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_5_CD_IND)
        else NULL
    end as DGNS_POA_5_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_6_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_6_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_6_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_6_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_6_CD), '') = null then NULL
        else DGNS_6_CD
    end as DGNS_6_CD,
    case
        when DGNS_6_CD_IND is NOT NULL
        and trim(DGNS_6_CD_IND) in ('1', '2', '3') then trim(DGNS_6_CD_IND)
        else NULL
    end as DGNS_6_CD_IND,
    case
        when (upper(DGNS_POA_6_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_6_CD_IND)
        else NULL
    end as DGNS_POA_6_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_7_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_7_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_7_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_7_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_7_CD), '') = null then NULL
        else DGNS_7_CD
    end as DGNS_7_CD,
    case
        when DGNS_7_CD_IND is NOT NULL
        and trim(DGNS_7_CD_IND) in ('1', '2', '3') then trim(DGNS_7_CD_IND)
        else NULL
    end as DGNS_7_CD_IND,
    case
        when (upper(DGNS_POA_7_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_7_CD_IND)
        else NULL
    end as DGNS_POA_7_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_8_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_8_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_8_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_8_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_8_CD), '') = null then NULL
        else DGNS_8_CD
    end as DGNS_8_CD,
    case
        when DGNS_8_CD_IND is NOT NULL
        and trim(DGNS_8_CD_IND) in ('1', '2', '3') then trim(DGNS_8_CD_IND)
        else NULL
    end as DGNS_8_CD_IND,
    case
        when (upper(DGNS_POA_8_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_8_CD_IND)
        else NULL
    end as DGNS_POA_8_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_9_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_9_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_9_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_9_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_9_CD), '') = null then NULL
        else DGNS_9_CD
    end as DGNS_9_CD,
    case
        when DGNS_9_CD_IND is NOT NULL
        and trim(DGNS_9_CD_IND) in ('1', '2', '3') then trim(DGNS_9_CD_IND)
        else NULL
    end as DGNS_9_CD_IND,
    case
        when (upper(DGNS_POA_9_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_9_CD_IND)
        else NULL
    end as DGNS_POA_9_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_10_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_10_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_10_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_10_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_10_CD), '') = null then NULL
        else DGNS_10_CD
    end as DGNS_10_CD,
    case
        when DGNS_10_CD_IND is NOT NULL
        and trim(DGNS_10_CD_IND) in ('1', '2', '3') then trim(DGNS_10_CD_IND)
        else NULL
    end as DGNS_10_CD_IND,
    case
        when (upper(DGNS_POA_10_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_10_CD_IND)
        else NULL
    end as DGNS_POA_10_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_11_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_11_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_11_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_11_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_11_CD), '') = null then NULL
        else DGNS_11_CD
    end as DGNS_11_CD,
    case
        when DGNS_11_CD_IND is NOT NULL
        and trim(DGNS_11_CD_IND) in ('1', '2', '3') then trim(DGNS_11_CD_IND)
        else NULL
    end as DGNS_11_CD_IND,
    case
        when (upper(DGNS_POA_11_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_11_CD_IND)
        else NULL
    end as DGNS_POA_11_CD_IND,
    case
        when (
            regexp_count(trim(DGNS_12_CD), '[^0]+') = 0
            or regexp_count(trim(DGNS_12_CD), '[^8]+') = 0
            or regexp_count(trim(DGNS_12_CD), '[^9]+') = 0
            or regexp_count(trim(DGNS_12_CD), '[^#]+') = 0
        )
        or nullif(trim(DGNS_12_CD), '') = null then NULL
        else DGNS_12_CD
    end as DGNS_12_CD,
    case
        when DGNS_12_CD_IND is NOT NULL
        and trim(DGNS_12_CD_IND) in ('1', '2', '3') then trim(DGNS_12_CD_IND)
        else NULL
    end as DGNS_12_CD_IND,
    case
        when (upper(DGNS_POA_12_CD_IND) in ('Y', 'N', 'U', 'W', '1')) then upper(DGNS_POA_12_CD_IND)
        else NULL
    end as DGNS_POA_12_CD_IND,
    DRG_CD,
    trim(DRG_CD_IND) as DRG_CD_IND,
    trim(DRG_DESC) as DRG_DESC,
    case
        when (PRCDR_1_CD_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else PRCDR_1_CD_DT
    end as PRCDR_1_CD_DT,
    case
        when PRCDR_1_CD = '0.00'
        or (
            regexp_count(trim(PRCDR_1_CD), '[^0]+') = 0
            or regexp_count(trim(PRCDR_1_CD), '[^8]+') = 0
            or regexp_count(trim(PRCDR_1_CD), '[^9]+') = 0
            or regexp_count(trim(PRCDR_1_CD), '[^#]+') = 0
        )
        or nullif(trim(PRCDR_1_CD), '') = null then NULL
        else PRCDR_1_CD
    end as PRCDR_1_CD,
    case
        when lpad(PRCDR_1_CD_IND, 2, '0') in(
            '01',
            '02',
            '06',
            '07',
            '10',
            '11',
            '12',
            '13',
            '14',
            '15',
            '16',
            '17',
            '18',
            '19',
            '20',
            '21',
            '22',
            '23',
            '24',
            '25',
            '26',
            '27',
            '28',
            '29',
            '30',
            '31',
            '32',
            '33',
            '34',
            '35',
            '36',
            '37',
            '38',
            '39',
            '40',
            '41',
            '42',
            '43',
            '44',
            '45',
            '46',
            '47',
            '48',
            '49',
            '50',
            '51',
            '52',
            '53',
            '54',
            '55',
            '56',
            '57',
            '58',
            '59',
            '60',
            '61',
            '62',
            '63',
            '64',
            '65',
            '66',
            '67',
            '68',
            '69',
            '70',
            '71',
            '72',
            '73',
            '74',
            '75',
            '76',
            '77',
            '78',
            '79',
            '80',
            '81',
            '82',
            '83',
            '84',
            '85',
            '86',
            '87'
        ) then lpad(PRCDR_1_CD_IND, 2, '0')
        else NULL
    end as PRCDR_1_CD_IND,
    case
        when (PRCDR_2_CD_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else PRCDR_2_CD_DT
    end as PRCDR_2_CD_DT,
    case
        when PRCDR_2_CD = '0.00'
        or (
            regexp_count(trim(PRCDR_2_CD), '[^0]+') = 0
            or regexp_count(trim(PRCDR_2_CD), '[^8]+') = 0
            or regexp_count(trim(PRCDR_2_CD), '[^9]+') = 0
            or regexp_count(trim(PRCDR_2_CD), '[^#]+') = 0
        )
        or nullif(trim(PRCDR_2_CD), '') = null then NULL
        else PRCDR_2_CD
    end as PRCDR_2_CD,
    case
        when lpad(PRCDR_2_CD_IND, 2, '0') in(
            '01',
            '02',
            '06',
            '07',
            '10',
            '11',
            '12',
            '13',
            '14',
            '15',
            '16',
            '17',
            '18',
            '19',
            '20',
            '21',
            '22',
            '23',
            '24',
            '25',
            '26',
            '27',
            '28',
            '29',
            '30',
            '31',
            '32',
            '33',
            '34',
            '35',
            '36',
            '37',
            '38',
            '39',
            '40',
            '41',
            '42',
            '43',
            '44',
            '45',
            '46',
            '47',
            '48',
            '49',
            '50',
            '51',
            '52',
            '53',
            '54',
            '55',
            '56',
            '57',
            '58',
            '59',
            '60',
            '61',
            '62',
            '63',
            '64',
            '65',
            '66',
            '67',
            '68',
            '69',
            '70',
            '71',
            '72',
            '73',
            '74',
            '75',
            '76',
            '77',
            '78',
            '79',
            '80',
            '81',
            '82',
            '83',
            '84',
            '85',
            '86',
            '87'
        ) then lpad(PRCDR_2_CD_IND, 2, '0')
        else NULL
    end as PRCDR_2_CD_IND,
    case
        when (PRCDR_3_CD_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else PRCDR_3_CD_DT
    end as PRCDR_3_CD_DT,
    case
        when PRCDR_3_CD = '0.00'
        or (
            regexp_count(trim(PRCDR_3_CD), '[^0]+') = 0
            or regexp_count(trim(PRCDR_3_CD), '[^8]+') = 0
            or regexp_count(trim(PRCDR_3_CD), '[^9]+') = 0
            or regexp_count(trim(PRCDR_3_CD), '[^#]+') = 0
        )
        or nullif(trim(PRCDR_3_CD), '') = null then NULL
        else PRCDR_3_CD
    end as PRCDR_3_CD,
    case
        when lpad(PRCDR_3_CD_IND, 2, '0') in(
            '01',
            '02',
            '06',
            '07',
            '10',
            '11',
            '12',
            '13',
            '14',
            '15',
            '16',
            '17',
            '18',
            '19',
            '20',
            '21',
            '22',
            '23',
            '24',
            '25',
            '26',
            '27',
            '28',
            '29',
            '30',
            '31',
            '32',
            '33',
            '34',
            '35',
            '36',
            '37',
            '38',
            '39',
            '40',
            '41',
            '42',
            '43',
            '44',
            '45',
            '46',
            '47',
            '48',
            '49',
            '50',
            '51',
            '52',
            '53',
            '54',
            '55',
            '56',
            '57',
            '58',
            '59',
            '60',
            '61',
            '62',
            '63',
            '64',
            '65',
            '66',
            '67',
            '68',
            '69',
            '70',
            '71',
            '72',
            '73',
            '74',
            '75',
            '76',
            '77',
            '78',
            '79',
            '80',
            '81',
            '82',
            '83',
            '84',
            '85',
            '86',
            '87'
        ) then lpad(PRCDR_3_CD_IND, 2, '0')
        else NULL
    end as PRCDR_3_CD_IND,
    case
        when (PRCDR_4_CD_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else PRCDR_4_CD_DT
    end as PRCDR_4_CD_DT,
    case
        when PRCDR_4_CD = '0.00'
        or (
            regexp_count(trim(PRCDR_4_CD), '[^0]+') = 0
            or regexp_count(trim(PRCDR_4_CD), '[^8]+') = 0
            or regexp_count(trim(PRCDR_4_CD), '[^9]+') = 0
            or regexp_count(trim(PRCDR_4_CD), '[^#]+') = 0
        )
        or nullif(trim(PRCDR_4_CD), '') = null then NULL
        else PRCDR_4_CD
    end as PRCDR_4_CD,
    case
        when lpad(PRCDR_4_CD_IND, 2, '0') in(
            '01',
            '02',
            '06',
            '07',
            '10',
            '11',
            '12',
            '13',
            '14',
            '15',
            '16',
            '17',
            '18',
            '19',
            '20',
            '21',
            '22',
            '23',
            '24',
            '25',
            '26',
            '27',
            '28',
            '29',
            '30',
            '31',
            '32',
            '33',
            '34',
            '35',
            '36',
            '37',
            '38',
            '39',
            '40',
            '41',
            '42',
            '43',
            '44',
            '45',
            '46',
            '47',
            '48',
            '49',
            '50',
            '51',
            '52',
            '53',
            '54',
            '55',
            '56',
            '57',
            '58',
            '59',
            '60',
            '61',
            '62',
            '63',
            '64',
            '65',
            '66',
            '67',
            '68',
            '69',
            '70',
            '71',
            '72',
            '73',
            '74',
            '75',
            '76',
            '77',
            '78',
            '79',
            '80',
            '81',
            '82',
            '83',
            '84',
            '85',
            '86',
            '87'
        ) then lpad(PRCDR_4_CD_IND, 2, '0')
        else NULL
    end as PRCDR_4_CD_IND,
    case
        when (PRCDR_5_CD_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else PRCDR_5_CD_DT
    end as PRCDR_5_CD_DT,
    case
        when PRCDR_5_CD = '0.00'
        or (
            regexp_count(trim(PRCDR_5_CD), '[^0]+') = 0
            or regexp_count(trim(PRCDR_5_CD), '[^8]+') = 0
            or regexp_count(trim(PRCDR_5_CD), '[^9]+') = 0
            or regexp_count(trim(PRCDR_5_CD), '[^#]+') = 0
        )
        or nullif(trim(PRCDR_5_CD), '') = null then NULL
        else PRCDR_5_CD
    end as PRCDR_5_CD,
    case
        when lpad(PRCDR_5_CD_IND, 2, '0') in(
            '01',
            '02',
            '06',
            '07',
            '10',
            '11',
            '12',
            '13',
            '14',
            '15',
            '16',
            '17',
            '18',
            '19',
            '20',
            '21',
            '22',
            '23',
            '24',
            '25',
            '26',
            '27',
            '28',
            '29',
            '30',
            '31',
            '32',
            '33',
            '34',
            '35',
            '36',
            '37',
            '38',
            '39',
            '40',
            '41',
            '42',
            '43',
            '44',
            '45',
            '46',
            '47',
            '48',
            '49',
            '50',
            '51',
            '52',
            '53',
            '54',
            '55',
            '56',
            '57',
            '58',
            '59',
            '60',
            '61',
            '62',
            '63',
            '64',
            '65',
            '66',
            '67',
            '68',
            '69',
            '70',
            '71',
            '72',
            '73',
            '74',
            '75',
            '76',
            '77',
            '78',
            '79',
            '80',
            '81',
            '82',
            '83',
            '84',
            '85',
            '86',
            '87'
        ) then lpad(PRCDR_5_CD_IND, 2, '0')
        else NULL
    end as PRCDR_5_CD_IND,
    case
        when (PRCDR_6_CD_DT < to_date('1600-01-01')) then to_date('1599-12-31')
        else PRCDR_6_CD_DT
    end as PRCDR_6_CD_DT,
    case
        when PRCDR_6_CD = '0.00'
        or (
            regexp_count(trim(PRCDR_6_CD), '[^0]+') = 0
            or regexp_count(trim(PRCDR_6_CD), '[^8]+') = 0
            or regexp_count(trim(PRCDR_6_CD), '[^9]+') = 0
            or regexp_count(trim(PRCDR_6_CD), '[^#]+') = 0
        )
        or nullif(trim(PRCDR_6_CD), '') = null then NULL
        else PRCDR_6_CD
    end as PRCDR_6_CD,
    case
        when lpad(PRCDR_6_CD_IND, 2, '0') in(
            '01',
            '02',
            '06',
            '07',
            '10',
            '11',
            '12',
            '13',
            '14',
            '15',
            '16',
            '17',
            '18',
            '19',
            '20',
            '21',
            '22',
            '23',
            '24',
            '25',
            '26',
            '27',
            '28',
            '29',
            '30',
            '31',
            '32',
            '33',
            '34',
            '35',
            '36',
            '37',
            '38',
            '39',
            '40',
            '41',
            '42',
            '43',
            '44',
            '45',
            '46',
            '47',
            '48',
            '49',
            '50',
            '51',
            '52',
            '53',
            '54',
            '55',
            '56',
            '57',
            '58',
            '59',
            '60',
            '61',
            '62',
            '63',
            '64',
            '65',
            '66',
            '67',
            '68',
            '69',
            '70',
            '71',
            '72',
            '73',
            '74',
            '75',
            '76',
            '77',
            '78',
            '79',
            '80',
            '81',
            '82',
            '83',
            '84',
            '85',
            '86',
            '87'
        ) then lpad(PRCDR_6_CD_IND, 2, '0')
        else NULL
    end as PRCDR_6_CD_IND,
    case
        when NCVRD_DAYS_CNT in (88888) then NULL
        else NCVRD_DAYS_CNT
    end as NCVRD_DAYS_CNT,
    case
        when NCVRD_CHRGS_AMT in (88888888888.00) then NULL
        else NCVRD_CHRGS_AMT
    end as NCVRD_CHRGS_AMT,
    case
        when MDCD_CVRD_IP_DAYS_CNT in (88888) then NULL
        else MDCD_CVRD_IP_DAYS_CNT
    end as MDCD_CVRD_IP_DAYS_CNT,
    case
        when OUTLIER_DAYS_CNT in (888) then NULL
        else OUTLIER_DAYS_CNT
    end as OUTLIER_DAYS_CNT,
    case
        when lpad(OUTLIER_CD, 2, '0') in ('03', '04', '05') then NULL
        else case
            when outlier_cd is not NULL
            and regexp_count(lpad(outlier_cd, 2, '0'), '[0-9]{2}') > 0 then case
                when (
                    outlier_cd >= 0
                    and outlier_cd <= 10
                ) then lpad(outlier_cd, 2, '0')
                else NULL
            end
            else NULL
        end
    end as outlier_cd,
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
        when regexp_count(lpad(admtg_prvdr_type_cd, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                admtg_prvdr_type_cd >= 1
                and admtg_prvdr_type_cd <= 57
            ) then lpad(admtg_prvdr_type_cd, 2, '0')
            else NULL
        end
        else NULL
    end as admtg_prvdr_type_cd,
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
                blg_prvdr_type_cd >= 1
                and blg_prvdr_type_cd <= 57
            ) then lpad(blg_prvdr_type_cd, 2, '0')
            else NULL
        end
        else NULL
    end as blg_prvdr_type_cd,
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
        when regexp_count(lpad(rfrg_prvdr_type_cd, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                rfrg_prvdr_type_cd >= 1
                and rfrg_prvdr_type_cd <= 57
            ) then lpad(rfrg_prvdr_type_cd, 2, '0')
            else NULL
        end
        else NULL
    end as rfrg_prvdr_type_cd,
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
        when PYMT_LVL_IND is NOT NULL
        and trim(PYMT_LVL_IND) in ('1', '2') then trim(PYMT_LVL_IND)
        else NULL
    end as PYMT_LVL_IND,
    case
        when TOT_BILL_AMT in (
            888888888.88,
            99999999.90,
            9999999.99,
            999999.99,
            999999.00
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
        when TOT_COPAY_AMT in (9999999.99, 888888888.88, 88888888888.00) then NULL
        else TOT_COPAY_AMT
    end as TOT_COPAY_AMT,
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
            888888888.88,
            888888888.00,
            88888888888.00,
            99999999999.00
        ) then NULL
        else TP_COPMT_PD_AMT
    end as TP_COPMT_PD_AMT,
    case
        when MDCD_DSH_PD_AMT in (888888888.88) then NULL
        else MDCD_DSH_PD_AMT
    end as MDCD_DSH_PD_AMT,
    case
        when DRG_OUTLIER_AMT in (888888888.88) then NULL
        else DRG_OUTLIER_AMT
    end as DRG_OUTLIER_AMT,
    case
        when regexp_count(drg_rltv_wt_num, '[^0-9.]') > 0
        or regexp_count(drg_rltv_wt_num, '[.]') > 1 then null
        when cast(drg_rltv_wt_num as numeric(8, 0)) > 9999999 then null
        else cast(drg_rltv_wt_num as numeric(11, 4))
    end as DRG_RLTV_WT_NUM,
    case
        when MDCR_PD_AMT in (
            888888888.88,
            8888888.88,
            88888888888.00,
            88888888888.88,
            99999999999.00,
            9999999999.99
        ) then NULL
        else MDCR_PD_AMT
    end as MDCR_PD_AMT,
    case
        when TOT_MDCR_DDCTBL_AMT in (888888888.88, 99999, 88888888888.00) then NULL
        else TOT_MDCR_DDCTBL_AMT
    end as TOT_MDCR_DDCTBL_AMT,
    case
        when TOT_MDCR_COINSRNC_AMT in (888888888.88) then NULL
        else TOT_MDCR_COINSRNC_AMT
    end as TOT_MDCR_COINSRNC_AMT,
    case
        when MDCR_CMBND_DDCTBL_IND is NOT NULL
        and trim(MDCR_CMBND_DDCTBL_IND) in ('0', '1') then trim(MDCR_CMBND_DDCTBL_IND)
        else NULL
    end as MDCR_CMBND_DDCTBL_IND,
    case
        when mdcr_reimbrsmt_type_cd is NOT NULL
        and lpad(trim(mdcr_reimbrsmt_type_cd), 2, '0') in ('01', '02', '03', '04', '05', '06', '07', '08', '09') then lpad(trim(mdcr_reimbrsmt_type_cd), 2, '0')
        else NULL
    end as mdcr_reimbrsmt_type_cd,
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
    IP_MH_DX_IND,
    IP_SUD_DX_IND,
    IP_MH_TAXONOMY_IND as IP_MH_TXNMY_IND,
    IP_SUD_TAXONOMY_IND as IP_SUD_TXNMY_IND,
    null:: char(3) as MAJ_DGNSTC_CTGRY,
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
        when (SRVC_ENDG_DT_DRVD < to_date('1600-01-01')) then to_date('1599-12-31') CONVERT_TIMEZONE
        else SRVC_ENDG_DT_DRVD
    end as SRVC_ENDG_DT_DRVD,
    case
        when SRVC_ENDG_DT_CD is NOT NULL
        and trim(SRVC_ENDG_DT_CD) in ('1', '2', '3', '4', '5') then trim(SRVC_ENDG_DT_CD)
        else NULL
    end as SRVC_ENDG_DT_CD
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
            IP_HEADER_GROUPER
    ) H
