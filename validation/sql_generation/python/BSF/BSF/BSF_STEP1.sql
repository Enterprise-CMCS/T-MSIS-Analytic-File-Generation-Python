create
or replace temporary view BSF_STEP1 as
select
    t1.*,
    t2.BIRTH_DT,
    case
        when upper(trim(t2.GNDR_CD)) in('M', 'F') then upper(trim(t2.GNDR_CD))
        else null
    end as GNDR_CD,
    t2.ELGBL_1ST_NAME,
    t2.ELGBL_LAST_NAME,
    t2.ELGBL_MDL_INITL_NAME,
    t2.PRMRY_DMGRPHC_ELE_EFCTV_DT,
    t2.PRMRY_DMGRPHC_ELE_END_DT,
    DEATH_DATE as DEATH_DT,
    t2.AGE,
    t2.DECEASED_FLG as DECEASED_FLAG,
    t2.AGE_GROUP_FLG as AGE_GROUP_FLAG,
    t2.GNDR_CODE,
    lpad(cast(t3.SSN_NUM as char(9)), 9, '0') as SSN_NUM,
    case
        when upper(trim(t3.MRTL_STUS_CD)) in(
            '01',
            '02',
            '03',
            '04',
            '05',
            '06',
            '07',
            '08',
            '09',
            '10',
            '11',
            '12',
            '13',
            '14'
        ) then upper(trim(t3.MRTL_STUS_CD))
        else null
    end as MRTL_STUS_CD,
    case
        when upper(trim(t3.SSN_VRFCTN_IND)) in('0', '1', '2') then upper(trim(t3.SSN_VRFCTN_IND))
        else null
    end as SSN_VRFCTN_IND,
    case
        when upper(trim(t3.INCM_CD)) in('01', '02', '03', '04', '05', '06', '07', '08') then upper(trim(t3.INCM_CD))
        else null
    end as INCM_CD,
    case
        when upper(trim(t3.VET_IND)) in('0', '1') then upper(trim(t3.VET_IND))
        else null
    end as VET_IND,
    case
        when upper(trim(t3.CTZNSHP_IND)) in('0', '1', '2') then upper(trim(t3.CTZNSHP_IND))
        else null
    end as CTZNSHP_IND,
    case
        when upper(trim(t3.CTZNSHP_VRFCTN_IND)) in('0', '1') then upper(trim(t3.CTZNSHP_VRFCTN_IND))
        else null
    end as CTZNSHP_VRFCTN_IND,
    case
        when upper(trim(t3.IMGRTN_STUS_CD)) in('1', '2', '3', '8') then upper(trim(t3.IMGRTN_STUS_CD))
        else null
    end as IMGRTN_STUS_CD,
    t3.IMGRTN_STUS_5_YR_BAR_END_DT,
    case
        when upper(trim(t3.IMGRTN_VRFCTN_IND)) in('0', '1') then upper(trim(t3.IMGRTN_VRFCTN_IND))
        else null
    end as IMGRTN_VRFCTN_IND,
    upper(t3.PRMRY_LANG_CD) as PRMRY_LANG_CD,
    case
        when upper(trim(t3.PRMRY_LANG_ENGLSH_PRFCNCY_CD)) in('0', '1', '2', '3') then upper(trim(t3.PRMRY_LANG_ENGLSH_PRFCNCY_CD))
        else null
    end as PRMRY_LANG_ENGLSH_PRFCNCY_CD,
    case
        when upper(trim(t3.HSEHLD_SIZE_CD)) in('01', '02', '03', '04', '05', '06', '07', '08') then upper(trim(t3.HSEHLD_SIZE_CD))
        else null
    end as HSEHLD_SIZE_CD,
    case
        when upper(trim(t3.PRGNT_IND)) in('0', '1') then upper(trim(t3.PRGNT_IND))
        else null
    end as PRGNT_IND,
    t3.MDCR_HICN_NUM,
    t3.MDCR_BENE_ID,
    case
        when upper(trim(t3.CHIP_CD)) in('0', '1', '2', '3', '4') then upper(trim(t3.CHIP_CD))
        else null
    end as CHIP_CD,
    t3.VAR_DMGRPHC_ELE_EFCTV_DT,
    t3.VAR_DMGRPHC_ELE_END_DT,
    t3.PRMRY_LANG_CODE,
    t3.PRMRY_LANG_FLG as PRMRY_LANG_FLAG,
    t3.PREGNANCY_FLG as PREGNANCY_FLAG,
    t4.ELGBL_ADR_EFCTV_DT,
    t4.ELGBL_ADR_END_DT,
    t4.ELGBL_LINE_1_ADR_HOME,
    t4.ELGBL_LINE_2_ADR_HOME,
    t4.ELGBL_LINE_3_ADR_HOME,
    t4.ELGBL_CITY_NAME_HOME,
    t4.ELGBL_CNTY_CD_HOME,
    t4.ELGBL_PHNE_NUM_HOME,
    t4.ELGBL_STATE_CD_HOME,
    t4.ELGBL_ZIP_CD_HOME,
    t4.ELGBL_LINE_1_ADR_MAIL,
    t4.ELGBL_LINE_2_ADR_MAIL,
    t4.ELGBL_LINE_3_ADR_MAIL,
    t4.ELGBL_CITY_NAME_MAIL,
    t4.ELGBL_CNTY_CD_MAIL,
    t4.ELGBL_PHNE_NUM_MAIL,
    t4.ELGBL_STATE_CD_MAIL,
    t4.ELGBL_ZIP_CD_MAIL,
    t5.MSIS_CASE_NUM,
    case
        when upper(trim(t5.ELGBLTY_MDCD_BASIS_CD)) in(
            '00',
            '01',
            '02',
            '03',
            '04',
            '05',
            '06',
            '07',
            '08',
            '10',
            '11'
        ) then upper(trim(t5.ELGBLTY_MDCD_BASIS_CD))
        else null
    end as ELGBLTY_MDCD_BASIS_CD,
    case
        when upper(trim(t5.CARE_LVL_STUS_CD)) in(
            '001',
            '002',
            '003',
            '004',
            '005',
            '01',
            '02',
            '03',
            '04',
            '05',
            '1',
            '2',
            '3',
            '4',
            '5'
        ) then upper(trim(t5.CARE_LVL_STUS_CD))
        else null
    end as CARE_LVL_STUS_CD,
    case
        when upper(trim(t5.SSDI_IND)) in('0', '1') then upper(trim(t5.SSDI_IND))
        else null
    end as SSDI_IND,
    case
        when upper(trim(t5.SSI_IND)) in('0', '1') then upper(trim(t5.SSI_IND))
        else null
    end as SSI_IND,
    case
        when upper(trim(t5.SSI_STATE_SPLMT_STUS_CD)) in('000', '001', '002') then upper(trim(t5.SSI_STATE_SPLMT_STUS_CD))
        else null
    end as SSI_STATE_SPLMT_STUS_CD,
    case
        when upper(trim(t5.SSI_STUS_CD)) in('000', '001', '002') then upper(trim(t5.SSI_STUS_CD))
        else null
    end as SSI_STUS_CD,
    t5.STATE_SPEC_ELGBLTY_FCTR_TXT,
    case
        when upper(trim(t5.BIRTH_CNCPTN_IND)) in('0', '1') then upper(trim(t5.BIRTH_CNCPTN_IND))
        else null
    end as BIRTH_CNCPTN_IND,
    case
        when upper(trim(t5.MAS_CD)) in('0', '1', '2', '3', '4', '5') then upper(trim(t5.MAS_CD))
        else null
    end as MAS_CD,
    case
        when upper(trim(t5.RSTRCTD_BNFTS_CD)) in(
            '0',
            '1',
            '2',
            '3',
            '4',
            '5',
            '6',
            '7',
            'A',
            'B',
            'C',
            'D',
            'E',
            'F'
        ) then upper(trim(t5.RSTRCTD_BNFTS_CD))
        else null
    end as RSTRCTD_BNFTS_CD,
    case
        when upper(trim(t5.TANF_CASH_CD)) in('0', '1', '2') then upper(trim(t5.TANF_CASH_CD))
        else null
    end as TANF_CASH_CD,
    t5.ELGBLTY_DTRMNT_EFCTV_DT,
    t5.ELGBLTY_DTRMNT_END_DT,
    case
        when upper(trim(t5.PRMRY_ELGBLTY_GRP_IND)) in('0', '1') then upper(trim(t5.PRMRY_ELGBLTY_GRP_IND))
        else null
    end as PRMRY_ELGBLTY_GRP_IND,
    ELGBLTY_GRP_CODE as ELGBLTY_GRP_CD,
    DUAL_ELGBL_CODE as DUAL_ELGBL_CD,
    lpad(trim(ELGBLTY_CHG_RSN_CD), 2, '0') as ELGBLTY_CHG_RSN_CD,
    t5.CARE_LVL_STUS_CODE,
    t5.DUAL_ELIGIBLE_FLG as DUAL_ELIGIBLE_FLAG,
    t5.ELIGIBILITY_GROUP_CATEGORY_FLG as ELIGIBILITY_GROUP_CATEGORY_FLAG,
    t5.MASBOE,
    t6.HH_ENT_NAME,
    t6.HH_SNTRN_NAME,
    t6.HH_SNTRN_PRTCPTN_EFCTV_DT,
    t6.HH_SNTRN_PRTCPTN_END_DT,
    case
        when t6.HH_PROGRAM_PARTICIPANT_FLG = 2
        and t8.ANY_VALID_HH_CC = 0 then null
        when coalesce(t6.HH_PROGRAM_PARTICIPANT_FLG, 0) = 1
        or coalesce(t8.ANY_VALID_HH_CC, 0) = 1 then 1
        else 0
    end as HH_PROGRAM_PARTICIPANT_FLAG,
    t7.HH_PRVDR_NUM,
    t7.HH_SNTRN_PRVDR_EFCTV_DT,
    t7.HH_SNTRN_PRVDR_END_DT,
    t8.MH_HH_CHRONIC_COND_FLG as MH_HH_CHRONIC_COND_FLAG,
    t8.SA_HH_CHRONIC_COND_FLG as SA_HH_CHRONIC_COND_FLAG,
    t8.ASTHMA_HH_CHRONIC_COND_FLG as ASTHMA_HH_CHRONIC_COND_FLAG,
    t8.DIABETES_HH_CHRONIC_COND_FLG as DIABETES_HH_CHRONIC_COND_FLAG,
    t8.HEART_DIS_HH_CHRONIC_COND_FLG as HEART_DIS_HH_CHRONIC_COND_FLAG,
    t8.OVERWEIGHT_HH_CHRONIC_COND_FLG as OVERWEIGHT_HH_CHRONIC_COND_FLAG,
    t8.HIV_AIDS_HH_CHRONIC_COND_FLG as HIV_AIDS_HH_CHRONIC_COND_FLAG,
    t8.OTHER_HH_CHRONIC_COND_FLG as OTHER_HH_CHRONIC_COND_FLAG,
    t9.LCKIN_PRVDR_NUM1,
    t9.LCKIN_PRVDR_NUM2,
    t9.LCKIN_PRVDR_NUM3,
    t9.LCKIN_PRVDR_TYPE_CD1,
    t9.LCKIN_PRVDR_TYPE_CD2,
    t9.LCKIN_PRVDR_TYPE_CD3,
    nullif(coalesce(t9.LOCK_IN_FLG, 0), 2) as LOCK_IN_FLAG,
    t10.MFP_ENRLMT_EFCTV_DT,
    t10.MFP_ENRLMT_END_DT,
    case
        when upper(trim(t10.MFP_LVS_WTH_FMLY_CD)) in('0', '1', '2') then upper(trim(t10.MFP_LVS_WTH_FMLY_CD))
        else null
    end as MFP_LVS_WTH_FMLY_CD,
    case
        when upper(trim(t10.MFP_QLFYD_INSTN_CD)) in(
            '00',
            '01',
            '02',
            '03',
            '04',
            '05',
            '0',
            '1',
            '2',
            '3',
            '4',
            '5'
        ) then upper(trim(t10.MFP_QLFYD_INSTN_CD))
        else null
    end as MFP_QLFYD_INSTN_CD,
    case
        when upper(trim(t10.MFP_QLFYD_RSDNC_CD)) in(
            '00',
            '01',
            '02',
            '03',
            '04',
            '05',
            '0',
            '1',
            '2',
            '3',
            '4',
            '5'
        ) then upper(trim(t10.MFP_QLFYD_RSDNC_CD))
        else null
    end as MFP_QLFYD_RSDNC_CD,
    case
        when upper(trim(t10.MFP_PRTCPTN_ENDD_RSN_CD)) in(
            '00',
            '01',
            '02',
            '03',
            '04',
            '05',
            '06',
            '07',
            '0',
            '1',
            '2',
            '3',
            '4',
            '5',
            '6',
            '7'
        ) then upper(trim(t10.MFP_PRTCPTN_ENDD_RSN_CD))
        else null
    end as MFP_PRTCPTN_ENDD_RSN_CD,
    case
        when upper(trim(t10.MFP_RINSTLZD_RSN_CD)) in(
            '00',
            '01',
            '02',
            '03',
            '04',
            '05',
            '06',
            '07',
            '08',
            '0',
            '1',
            '2',
            '3',
            '4',
            '5',
            '6',
            '7',
            '8'
        ) then upper(trim(t10.MFP_RINSTLZD_RSN_CD))
        else null
    end as MFP_RINSTLZD_RSN_CD,
    t10.mfp_prtcptn_endd_rsn_code,
    t10.mfp_qlfyd_instn_code,
    t10.mfp_qlfyd_rsdnc_code,
    t10.mfp_rinstlzd_rsn_code,
    coalesce(t10.MFP_PARTICIPANT_FLG, 0) as MFP_PARTICIPANT_FLAG,
    t11.COMMUNITY_FIRST_CHOICE_SPO_FLG as COMMUNITY_FIRST_CHOICE_SPO_FLAG,
    t11._1915I_SPO_FLG as _1915I_SPO_FLAG,
    t11._1915J_SPO_FLG as _1915J_SPO_FLAG,
    t11._1915A_SPO_FLG as _1915A_SPO_FLAG,
    t11._1932A_SPO_FLG as _1932A_SPO_FLAG,
    t11._1937_ABP_SPO_FLG as _1937_ABP_SPO_FLAG,
    t12.WVR_ID1,
    t12.WVR_ID2,
    t12.WVR_ID3,
    t12.WVR_ID4,
    t12.WVR_ID5,
    t12.WVR_ID6,
    t12.WVR_ID7,
    t12.WVR_ID8,
    t12.WVR_ID9,
    t12.WVR_ID10,
    t12.WVR_TYPE_CD1,
    t12.WVR_TYPE_CD2,
    t12.WVR_TYPE_CD3,
    t12.WVR_TYPE_CD4,
    t12.WVR_TYPE_CD5,
    t12.WVR_TYPE_CD6,
    t12.WVR_TYPE_CD7,
    t12.WVR_TYPE_CD8,
    t12.WVR_TYPE_CD9,
    t12.WVR_TYPE_CD10,
    t13.LTSS_PRVDR_NUM1,
    t13.LTSS_PRVDR_NUM2,
    t13.LTSS_PRVDR_NUM3,
    t13.LTSS_LVL_CARE_CD1,
    t13.LTSS_LVL_CARE_CD2,
    t13.LTSS_LVL_CARE_CD3,
    t14.MC_PLAN_ID1,
    t14.MC_PLAN_ID2,
    t14.MC_PLAN_ID3,
    t14.MC_PLAN_ID4,
    t14.MC_PLAN_ID5,
    t14.MC_PLAN_ID6,
    t14.MC_PLAN_ID7,
    t14.MC_PLAN_ID8,
    t14.MC_PLAN_ID9,
    t14.MC_PLAN_ID10,
    t14.MC_PLAN_ID11,
    t14.MC_PLAN_ID12,
    t14.MC_PLAN_ID13,
    t14.MC_PLAN_ID14,
    t14.MC_PLAN_ID15,
    t14.MC_PLAN_ID16,
    t14.MC_PLAN_TYPE_CD1,
    t14.MC_PLAN_TYPE_CD2,
    t14.MC_PLAN_TYPE_CD3,
    t14.MC_PLAN_TYPE_CD4,
    t14.MC_PLAN_TYPE_CD5,
    t14.MC_PLAN_TYPE_CD6,
    t14.MC_PLAN_TYPE_CD7,
    t14.MC_PLAN_TYPE_CD8,
    t14.MC_PLAN_TYPE_CD9,
    t14.MC_PLAN_TYPE_CD10,
    t14.MC_PLAN_TYPE_CD11,
    t14.MC_PLAN_TYPE_CD12,
    t14.MC_PLAN_TYPE_CD13,
    t14.MC_PLAN_TYPE_CD14,
    t14.MC_PLAN_TYPE_CD15,
    t14.MC_PLAN_TYPE_CD16,
    t15.ETHNCTY_DCLRTN_EFCTV_DT,
    t15.ETHNCTY_DCLRTN_END_DT,
    case
        when upper(trim(t15.ETHNCTY_CD)) in('0', '1', '2', '3', '4', '5') then upper(trim(t15.ETHNCTY_CD))
        else null
    end as ETHNCTY_CD,
    t15.HISPANIC_ETHNICITY_FLG as HISPANIC_ETHNICITY_FLAG,
    case
        when upper(trim(t16.CRTFD_AMRCN_INDN_ALSKN_NTV_IND)) in('0', '1', '2') then upper(trim(t16.CRTFD_AMRCN_INDN_ALSKN_NTV_IND))
        else null
    end as CRTFD_AMRCN_INDN_ALSKN_NTV_IND,
    t16.NATIVE_HI_FLG as NATIVE_HI_FLAG,
    t16.GUAM_CHAMORRO_FLG as GUAM_CHAMORRO_FLAG,
    t16.SAMOAN_FLG as SAMOAN_FLAG,
    t16.OTHER_PAC_ISLANDER_FLG as OTHER_PAC_ISLANDER_FLAG,
    t16.UNK_PAC_ISLANDER_FLG as UNK_PAC_ISLANDER_FLAG,
    t16.ASIAN_INDIAN_FLG as ASIAN_INDIAN_FLAG,
    t16.CHINESE_FLG as CHINESE_FLAG,
    t16.FILIPINO_FLG as FILIPINO_FLAG,
    t16.JAPANESE_FLG as JAPANESE_FLAG,
    t16.KOREAN_FLG as KOREAN_FLAG,
    t16.VIETNAMESE_FLG as VIETNAMESE_FLAG,
    t16.OTHER_ASIAN_FLG as OTHER_ASIAN_FLAG,
    t16.UNKNOWN_ASIAN_FLG as UNKNOWN_ASIAN_FLAG,
    t16.WHITE_FLG as WHITE_FLAG,
    t16.BLACK_AFRICAN_AMERICAN_FLG as BLACK_AFRICAN_AMERICAN_FLAG,
    t16.AIAN_FLG as AIAN_FLAG,
    case
        when (
            COALESCE(t16.WHITE_FLG, 0) + COALESCE(t16.BLACK_AFRICAN_AMERICAN_FLG, 0) + COALESCE(t16.AIAN_FLG, 0) + COALESCE(GLOBAL_ASIAN, 0) + COALESCE(GLOBAL_ISLANDER, 0)
        ) > 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 6
        when t15.HISPANIC_ETHNICITY_FLG = 1 then 7
        when t16.WHITE_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 1
        when t16.BLACK_AFRICAN_AMERICAN_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 2
        when t16.GLOBAL_ASIAN = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 3
        when t16.AIAN_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 4
        when t16.GLOBAL_ISLANDER = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 5
        when t15.HISPANIC_ETHNICITY_FLG = 0 then null
        else null
    end as RACE_ETHNICITY_FLAG,
    case
        when t15.HISPANIC_ETHNICITY_FLG = 1 then 20
        when (
            COALESCE(t16.WHITE_FLG, 0) + COALESCE(t16.BLACK_AFRICAN_AMERICAN_FLG, 0) + COALESCE(t16.AIAN_FLG, 0) + COALESCE(GLOBAL_ASIAN, 0) + COALESCE(GLOBAL_ISLANDER, 0)
        ) > 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 19
        when coalesce(t16.MULTI_ASIAN, 0) = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 12
        when coalesce(t16.MULTI_ISLANDER, 0) = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 18
        when t16.WHITE_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 1
        when t16.BLACK_AFRICAN_AMERICAN_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 2
        when t16.AIAN_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 3
        when t16.ASIAN_INDIAN_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 4
        when t16.CHINESE_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 5
        when t16.FILIPINO_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 6
        when t16.JAPANESE_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 7
        when t16.KOREAN_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 8
        when t16.VIETNAMESE_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 9
        when t16.OTHER_ASIAN_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 10
        when t16.UNKNOWN_ASIAN_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 11
        when t16.NATIVE_HI_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 13
        when t16.GUAM_CHAMORRO_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 14
        when t16.SAMOAN_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 15
        when t16.OTHER_PAC_ISLANDER_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 16
        when t16.UNK_PAC_ISLANDER_FLG = 1
        and COALESCE(t15.HISPANIC_ETHNICITY_FLG, 0) = 0 then 17
        when t15.HISPANIC_ETHNICITY_FLG = 0 then null
        else null
    end as RACE_ETHNCTY_EXP_FLAG,
    t17.DEAF_DISAB_FLG as DEAF_DISAB_FLAG,
    t17.BLIND_DISAB_FLG as BLIND_DISAB_FLAG,
    t17.DIFF_CONC_DISAB_FLG as DIFF_CONC_DISAB_FLAG,
    t17.DIFF_WALKING_DISAB_FLG as DIFF_WALKING_DISAB_FLAG,
    t17.DIFF_DRESSING_BATHING_DISAB_FLG as DIFF_DRESSING_BATHING_DISAB_FLAG,
    t17.DIFF_ERRANDS_ALONE_DISAB_FLG as DIFF_ERRANDS_ALONE_DISAB_FLAG,
    t17.OTHER_DISAB_FLG as OTHER_DISAB_FLAG,
    case
        when upper(trim(t18.SECT_1115A_DEMO_IND)) in('0', '1') then upper(trim(t18.SECT_1115A_DEMO_IND))
        else null
    end as SECT_1115A_DEMO_IND,
    t18.SECT_1115A_DEMO_EFCTV_DT,
    t18.SECT_1115A_DEMO_END_DT,
    t18._1115A_PARTICIPANT_FLG as _1115A_PARTICIPANT_FLAG,
    t19.HCBS_AGED_NON_HHCC_FLG as HCBS_AGED_NON_HHCC_FLAG,
    t19.HCBS_PHYS_DISAB_NON_HHCC_FLG as HCBS_PHYS_DISAB_NON_HHCC_FLAG,
    t19.HCBS_INTEL_DISAB_NON_HHCC_FLG as HCBS_INTEL_DISAB_NON_HHCC_FLAG,
    t19.HCBS_AUTISM_SP_DIS_NON_HHCC_FLG as HCBS_AUTISM_SP_DIS_NON_HHCC_FLAG,
    t19.HCBS_DD_NON_HHCC_FLG as HCBS_DD_NON_HHCC_FLAG,
    t19.HCBS_MI_SED_NON_HHCC_FLG as HCBS_MI_SED_NON_HHCC_FLAG,
    t19.HCBS_BRAIN_INJ_NON_HHCC_FLG as HCBS_BRAIN_INJ_NON_HHCC_FLAG,
    t19.HCBS_HIV_AIDS_NON_HHCC_FLG as HCBS_HIV_AIDS_NON_HHCC_FLAG,
    t19.HCBS_TECH_DEP_MF_NON_HHCC_FLG as HCBS_TECH_DEP_MF_NON_HHCC_FLAG,
    t19.HCBS_DISAB_OTHER_NON_HHCC_FLG as HCBS_DISAB_OTHER_NON_HHCC_FLAG,
    t20.ELGBL_PRSN_MN_EFCTV_DT,
    t20.ELGBL_PRSN_MN_END_DT,
    case
        when upper(trim(t20.TPL_INSRNC_CVRG_IND)) in('0', '1') then upper(trim(t20.TPL_INSRNC_CVRG_IND))
        else null
    end as TPL_INSRNC_CVRG_IND,
    case
        when upper(trim(t20.TPL_OTHR_CVRG_IND)) in('0', '1') then upper(trim(t20.TPL_OTHR_CVRG_IND))
        else null
    end as TPL_OTHR_CVRG_IND,
    t21.ELGBL_ID_ADDTNL,
    t21.ELGBL_ID_ADDTNL_ENT_ID,
    t21.ELGBL_ID_ADDTNL_RSN_CHG,
    t21.ELGBL_ID_MSIS_XWALK,
    t21.ELGBL_ID_MSIS_XWALK_ENT_ID,
    t21.ELGBL_ID_MSIS_XWALK_RSN_CHG,
    '202111' as BSF_FIL_DT,
    '0A' as BSF_VRSN,
    6194 as DA_RUN_ID
from
    ELG00021_202111_uniq t1
    left join ELG00002_202111_uniq t2 on t1.SUBMTG_STATE_CD = t2.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t2.MSIS_IDENT_NUM
    left join ELG00003_202111_uniq t3 on t1.SUBMTG_STATE_CD = t3.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t3.MSIS_IDENT_NUM
    left join ELG00004_202111_uniq t4 on t1.SUBMTG_STATE_CD = t4.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t4.MSIS_IDENT_NUM
    left join ELG00005_202111_uniq t5 on t1.SUBMTG_STATE_CD = t5.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t5.MSIS_IDENT_NUM
    left join ELG00006_202111_uniq t6 on t1.SUBMTG_STATE_CD = t6.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t6.MSIS_IDENT_NUM
    left join ELG00007_202111_uniq t7 on t1.SUBMTG_STATE_CD = t7.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t7.MSIS_IDENT_NUM
    left join ELG00008_202111_uniq t8 on t1.SUBMTG_STATE_CD = t8.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t8.MSIS_IDENT_NUM
    left join ELG00009_202111_uniq t9 on t1.SUBMTG_STATE_CD = t9.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t9.MSIS_IDENT_NUM
    left join ELG00010_202111_uniq t10 on t1.SUBMTG_STATE_CD = t10.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t10.MSIS_IDENT_NUM
    left join ELG00011_202111_uniq t11 on t1.SUBMTG_STATE_CD = t11.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t11.MSIS_IDENT_NUM
    left join ELG00012_202111_uniq t12 on t1.SUBMTG_STATE_CD = t12.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t12.MSIS_IDENT_NUM
    left join ELG00013_202111_uniq t13 on t1.SUBMTG_STATE_CD = t13.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t13.MSIS_IDENT_NUM
    left join ELG00014_202111_uniq t14 on t1.SUBMTG_STATE_CD = t14.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t14.MSIS_IDENT_NUM
    left join ELG00015_202111_uniq t15 on t1.SUBMTG_STATE_CD = t15.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t15.MSIS_IDENT_NUM
    left join ELG00016_202111_uniq t16 on t1.SUBMTG_STATE_CD = t16.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t16.MSIS_IDENT_NUM
    left join ELG00017_202111_uniq t17 on t1.SUBMTG_STATE_CD = t17.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t17.MSIS_IDENT_NUM
    left join ELG00018_202111_uniq t18 on t1.SUBMTG_STATE_CD = t18.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t18.MSIS_IDENT_NUM
    left join ELG00020_202111_uniq t19 on t1.SUBMTG_STATE_CD = t19.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t19.MSIS_IDENT_NUM
    left join TPL00002_202111_uniq t20 on t1.SUBMTG_STATE_CD = t20.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t20.MSIS_IDENT_NUM
    left join ELG00022_202111_uniq t21 on t1.SUBMTG_STATE_CD = t21.SUBMTG_STATE_CD
    and t1.MSIS_IDENT_NUM = t21.MSIS_IDENT_NUM