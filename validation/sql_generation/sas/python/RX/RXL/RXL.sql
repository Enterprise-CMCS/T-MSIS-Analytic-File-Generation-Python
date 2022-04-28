create
or replace temporary view RXL as
select
    6194 as DA_RUN_ID,
    'rx_link_key' as RX_LINK_KEY,
    '0A' as RX_VRSN,
    '202110' as RX_FIL_DT,
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
        when date_cmp(ADJDCTN_DT_LINE < to_date('1600-01-01')) then to_date('1599-12-31')
        else nullif(ADJDCTN_DT_LINE < to_date('01JAN1960'))
    end as ADJDCTN_DT,
    LINE_ADJSTMT_IND_CLEAN as LINE_ADJSTMT_IND,
    case
        when regexp_count(lpad(TOS_CD, 3, '0'), '[0-9]{3}') > 0 then case
            when (
                (
                    TOS_CD >= 1
                    and TOS_CD <= 93
                )
                or (
                    TOS_CD in (
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
            'ME',
            'EA',
            'GM'
        ) then trim(upper(UOM_CD))
        else NULL
    end as UOM_CD,
    case
        when suply_days_cnt in (8888, 999, 0) then NULL
        else suply_days_cnt
    end as suply_days_cnt,
    case
        when NEW_REFL_IND is not NULL
        and regexp_count(lpad(NEW_REFL_IND, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                NEW_REFL_IND >= 0
                and NEW_REFL_IND <= 98
            ) then lpad(NEW_REFL_IND, 2, '0')
            else NULL
        end
        else NULL
    end as NEW_REFL_IND,
    case
        when BRND_GNRC_IND is NOT NULL
        and trim(BRND_GNRC_IND) in (
            '0',
            '1',
            '2',
            '3',
            '4'
        ) then trim(BRND_GNRC_IND)
        else NULL
    end as BRND_GNRC_IND,
    case
        when dspns_fee_amt in (88888.88) then NULL
        else dspns_fee_amt
    end as dspns_fee_amt,
    case
        when trim(DRUG_UTLZTN_CD) is not NULL then upper(DRUG_UTLZTN_CD)
        else NULL
    end as DRUG_UTLZTN_CD,
    case
        when dtl_mtrc_dcml_qty in (999999.999) then NULL
        else dtl_mtrc_dcml_qty
    end as dtl_mtrc_dcml_qty,
    case
        when lpad(CMPND_DSG_FORM_CD, 2, '0') in ('08', '09') then NULL
        else case
            when CMPND_DSG_FORM_CD is not NULL
            and regexp_count(lpad(CMPND_DSG_FORM_CD, 2, '0'), '[0-9]{2}') > 0 then case
                when (
                    CMPND_DSG_FORM_CD >= 1
                    and CMPND_DSG_FORM_CD <= 18
                ) then lpad(CMPND_DSG_FORM_CD, 2, '0')
                else NULL
            end
            else NULL
        end
    end as CMPND_DSG_FORM_CD,
    case
        when REBT_ELGBL_IND is NOT NULL
        and trim(REBT_ELGBL_IND) in ('0', '1', '2') then trim(REBT_ELGBL_IND)
        else NULL
    end as REBT_ELGBL_IND,
    case
        when IMNZTN_TYPE_CD is not NULL
        and regexp_count(lpad(IMNZTN_TYPE_CD, 2, '0'), '[0-9]{2}') > 0 then case
            when (
                IMNZTN_TYPE_CD >= 0
                and IMNZTN_TYPE_CD <= 29
            ) then lpad(IMNZTN_TYPE_CD, 2, '0')
            else NULL
        end
        else NULL
    end as IMNZTN_TYPE_CD,
    case
        when BNFT_TYPE_CD is not NULL
        and regexp_count(lpad(BNFT_TYPE_CD, 3, '0'), '[0-9]{3}') > 0 then case
            when (
                BNFT_TYPE_CD >= 001
                and BNFT_TYPE_CD <= 108
            ) then lpad(BNFT_TYPE_CD, 3, '0')
            else NULL
        end
        else NULL
    end as BNFT_TYPE_CD,
    case
        when othr_toc_rx_clm_alowd_qty in (
            99999,
            99999.999,
            888888.000,
            999999,
            888888.880
        ) then NULL
        else othr_toc_rx_clm_alowd_qty
    end as alowd_srvc_qty,
    case
        when othr_toc_rx_clm_actl_qty in (
            999999.99,
            888888,
            999999,
            0
        ) then NULL
        else othr_toc_rx_clm_actl_qty
    end as actl_srvc_qty,
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
        when bill_amt in (
            9999999999.99,
            999999.99,
            999999,
            888888888.88
        ) then NULL
        else bill_amt
    end as bill_amt,
    case
        when alowd_amt in (
            9999999999.99,
            888888888.88,
            99999999.00
        ) then NULL
        else alowd_amt
    end as alowd_amt,
    case
        when copay_amt in (888888888.88, 88888888888.00) then NULL
        else copay_amt
    end as copay_amt,
    case
        when tpl_amt in (888888888.88) then NULL
        else tpl_amt
    end as tpl_amt,
    case
        when mdcd_pd_amt in (888888888.88) then NULL
        else mdcd_pd_amt
    end as mdcd_pd_amt,
    case
        when mdcr_pd_amt in (
            88888888888.88,
            99999999999.00,
            888888888.88,
            88888888888.00,
            8888888.88,
            9999999999.99
        ) then NULL
        else mdcr_pd_amt
    end as mdcr_pd_amt,
    case
        when mdcd_ffs_equiv_amt in (
            999999.99,
            888888888.88,
            88888888888.80
        ) then NULL
        else mdcd_ffs_equiv_amt
    end as mdcd_ffs_equiv_amt,
    case
        when mdcr_coinsrnc_pd_amt in (88888888888.00, 888888888.88) then NULL
        else mdcr_coinsrnc_pd_amt
    end as mdcr_coinsrnc_pd_amt,
    case
        when mdcr_ddctbl_amt in (88888888888.00, 888888888.88) then NULL
        else mdcr_ddctbl_amt
    end as mdcr_ddctbl_amt,
    case
        when othr_insrnc_amt in (
            88888888888.00,
            88888888888.88,
            888888888.88
        ) then NULL
        else othr_insrnc_amt
    end as othr_insrnc_amt,
    trim(upper(RSN_SRVC_CD)) as RSN_SRVC_CD,
    trim(upper(PROF_SRVC_CD)) as PROF_SRVC_CD,
    trim(upper(RSLT_SRVC_CD)) as RSLT_SRVC_CD,
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
            RX_LINE
    ) H