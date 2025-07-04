from taf.BSF.BSF_Runner import BSF_Runner


class BSF_Metadata:
    """
    Create metadata for BSF.  
    """

    def selectDataElements(segment_id: str, alias: str):
        """
        Function to select data elements.  Selected data elements will be cleansed, checked against a validator,
        and masked if there are invalid values.
        """

        new_line_comma = '\n\t\t\t,'

        columns = BSF_Metadata.columns.get(segment_id).copy()

        for i, item in enumerate(columns):
            if item in BSF_Metadata.cleanser.keys() and item.casefold() != 'imgrtn_stus_cd':
                columns[i] = BSF_Metadata.cleanser.copy().get(item)(alias)
            elif item in BSF_Metadata.validator.keys():
                columns[i] = BSF_Metadata.maskInvalidValues(item, alias)
            else:
                columns[i] = f"{alias}.{columns[i]}"

        return new_line_comma.join(columns)

    @staticmethod
    def finalFormatter():
        """
        Function for final formatting.  Handles epochal dates, absent data, and normalization.
        """

        new_line_comma = '\n\t\t\t,'

        columns = BSF_Metadata.output_columns.copy()

        for i, column in enumerate(columns):
            if column in BSF_Metadata.epochal:
                columns[i] = BSF_Metadata.epoch(column)
            elif column in BSF_Metadata.absent:
                columns[i] = f"null as {column}"
            else:
                columns[i] = BSF_Metadata.normalize(column)

        return new_line_comma.join(columns)

    @staticmethod
    def tagAlias(segment_id: str, alias: str):
        """
        Function to tag aliased names.  
        """

        # new_line_comma = '\n\t\t,'
        # aliased_cols = [alias + '.' + c for c in BSF_Metadata.final.get(segment)]
        # i = ','.join([new_line_comma.join(aliased_cols)])
        # if len(i) > 0:
        #     i += ','
        # return i

        new_line_comma = '\n\t\t\t,'

        columns = BSF_Metadata.final.get(segment_id).copy()

        for i, item in enumerate(columns):
            if item in BSF_Metadata.cleanser.keys():
                columns[i] = BSF_Metadata.cleanser.copy().get(item)(alias)
            elif item in BSF_Metadata.validator.keys():
                columns[i] = BSF_Metadata.maskInvalidValues(item, alias)
            else:
                columns[i] = f"{alias}.{columns[i]}"

        return new_line_comma.join(columns)

    @staticmethod
    def unifySelect(bsf: BSF_Runner):
        """
        This is a helper definition to combine all relevant file segments
        in order to insert records into the final target table for the beneficiary summary file.
        A formatted SQL string is returned, printed, then passed to spark.sql() to be executed. 
        These steps could be combined, but were intentionally left separate for traceability.
        """

        z = f"""
            create or replace temporary view BSF_STEP1 as
            select
                t1.*,

                {BSF_Metadata.tagAlias('ELG00002', 't2')},

                DEATH_DATE as DEATH_DT,
                t2.AGE,
                t2.DECEASED_FLG as DECEASED_FLAG,
                t2.AGE_GROUP_FLG as AGE_GROUP_FLAG,
                t2.SEX_CODE,

                {BSF_Metadata.tagAlias('ELG00003', 't3')},

                t3.PREFRD_LANG_CODE,
                t3.PRMRY_LANG_FLG as PRMRY_LANG_FLAG,
                t3.PREGNANCY_FLG as PREGNANCY_FLAG,

                {BSF_Metadata.tagAlias('ELG00004', 't4')},

                {BSF_Metadata.tagAlias('ELG00005', 't5')},

                ELGBLTY_GRP_CODE as ELGBLTY_GRP_CD,
                DUAL_ELGBL_CODE as DUAL_ELGBL_CD,
                lpad(trim(ELGBLTY_TRMNTN_RSN), 2, '0') as ELGBLTY_TRMNTN_RSN,
                lpad(trim(ELGBLTY_EXTNSN_CD), 3, '0') as ELGBLTY_EXTNSN_CD,
                lpad(trim(CNTNUS_ELGBLTY_CD), 3, '0') as CNTNUS_ELGBLTY_CD,
                lpad(trim(INCM_STD_CD), 2, '0') as INCM_STD_CD,

                t5.CARE_LVL_STUS_CODE,
                t5.DUAL_ELIGIBLE_FLG as DUAL_ELIGIBLE_FLAG,
                t5.ELIGIBILITY_GROUP_CATEGORY_FLG as ELIGIBILITY_GROUP_CATEGORY_FLAG,

                {BSF_Metadata.tagAlias('ELG00006', 't6')},

                case
                when t6.HH_PROGRAM_PARTICIPANT_FLG = 2 and
                     t8.ANY_VALID_HH_CC = 0 then null

                when coalesce(t6.HH_PROGRAM_PARTICIPANT_FLG,0)=1 or
                     coalesce(t8.ANY_VALID_HH_CC,0)=1 then 1
                else 0 end as HH_PROGRAM_PARTICIPANT_FLAG,

                {BSF_Metadata.tagAlias('ELG00007', 't7')},

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
                t9.LCKIN_SRVC1,
                t9.LCKIN_SRVC2,
                t9.LCKIN_SRVC3,
                nullif(coalesce(t9.LOCK_IN_FLG,0),2) as LOCK_IN_FLAG,

                {BSF_Metadata.tagAlias('ELG00010', 't10')},

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

                {BSF_Metadata.tagAlias('ELG00015', 't15')},

                t15.HISPANIC_ETHNICITY_FLG as HISPANIC_ETHNICITY_FLAG,

                {BSF_Metadata.tagAlias('ELG00016', 't16')},

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
                    when (COALESCE(t16.WHITE_FLG,0)
                    + COALESCE(t16.BLACK_AFRICAN_AMERICAN_FLG,0)
                    + COALESCE(t16.AIAN_FLG,0)
                    + COALESCE(GLOBAL_ASIAN,0)
                    + COALESCE(GLOBAL_ISLANDER,0)) > 1
                    and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 6

                    when t15.HISPANIC_ETHNICITY_FLG = 1 then 7
                    when t16.WHITE_FLG = 1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 1
                    when t16.BLACK_AFRICAN_AMERICAN_FLG = 1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 2
                    when t16.GLOBAL_ASIAN=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 3
                    when t16.AIAN_FLG = 1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 4
                    when t16.GLOBAL_ISLANDER=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 5
                    when t16.OTHER_OTHER_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 8
                    when t15.HISPANIC_ETHNICITY_FLG=0 then null
                    else null end as RACE_ETHNICITY_FLAG,

                case
                    when t15.HISPANIC_ETHNICITY_FLG = 1 then 20
                    when (COALESCE(t16.WHITE_FLG,0)
                    + COALESCE(t16.BLACK_AFRICAN_AMERICAN_FLG,0)
                    + COALESCE(t16.AIAN_FLG,0)
                    + COALESCE(GLOBAL_ASIAN,0)
                    + COALESCE(GLOBAL_ISLANDER,0)) > 1
                    and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 19

                    when coalesce(t16.MULTI_ASIAN,0) = 1
                    and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 12

                    when coalesce(t16.MULTI_ISLANDER,0) = 1
                     and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 18

                    when t16.WHITE_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 1
                    when t16.BLACK_AFRICAN_AMERICAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 2
                    when t16.AIAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 3
                    when t16.ASIAN_INDIAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 4
                    when t16.CHINESE_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 5
                    when t16.FILIPINO_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 6
                    when t16.JAPANESE_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 7
                    when t16.KOREAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 8
                    when t16.VIETNAMESE_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 9
                    when t16.OTHER_ASIAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 10
                    when t16.UNKNOWN_ASIAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 11
                    when t16.NATIVE_HI_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 13
                    when t16.GUAM_CHAMORRO_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 14
                    when t16.SAMOAN_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 15
                    when t16.OTHER_PAC_ISLANDER_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 16
                    when t16.UNK_PAC_ISLANDER_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 17
                    when t16.OTHER_OTHER_FLG=1 and COALESCE(t15.HISPANIC_ETHNICITY_FLG,0)=0 then 21
                    when t15.HISPANIC_ETHNICITY_FLG = 0 then null
                    else null end as RACE_ETHNCTY_EXP_FLAG,

                t17.DEAF_DISAB_FLG as DEAF_DISAB_FLAG,
                t17.BLIND_DISAB_FLG as BLIND_DISAB_FLAG,
                t17.DIFF_CONC_DISAB_FLG as DIFF_CONC_DISAB_FLAG,
                t17.DIFF_WALKING_DISAB_FLG as DIFF_WALKING_DISAB_FLAG,
                t17.DIFF_DRESSING_BATHING_DISAB_FLG as DIFF_DRESSING_BATHING_DISAB_FLAG,
                t17.DIFF_ERRANDS_ALONE_DISAB_FLG as DIFF_ERRANDS_ALONE_DISAB_FLAG,
                t17.OTHER_DISAB_FLG as OTHER_DISAB_FLAG,

                {BSF_Metadata.tagAlias('ELG00018', 't18')},

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

                {BSF_Metadata.tagAlias('TPL00002', 't20')},

                {BSF_Metadata.tagAlias('ELG00021', 't21')},

                '{bsf.TAF_FILE_DATE}' as BSF_FIL_DT,

                '{bsf.VERSION}' as BSF_VRSN,

                {bsf.DA_RUN_ID} as DA_RUN_ID

            from

                ELG00021_{bsf.BSF_FILE_DATE}_uniq t1

                {BSF_Metadata.join_segments(bsf.TAF_FILE_DATE)}
        """

        return z

    @staticmethod
    def finalTableOutput(bsf: BSF_Runner):
        """
        This is a “helper” definition to create the insert statement in order to insert
        records into the final target table for the beneficiary summary file.
        A formatted SQL string is returned, printed, then passed to spark.sql() to be executed.
        These steps could be combined, but were intentionally left separate for traceability.
        """
        # if this flag is set them don't insert to the tables
        # we're running to grab statistics only
        if bsf.run_stats_only:
            bsf.logger.info(f"** {bsf.__class__.__name__}: Run Stats Only is set to True. We will skip the table inserts and run post job functions only **")
            return

        z = f"""
                INSERT INTO {bsf.DA_SCHEMA}.taf_mon_bsf
                select
                    { BSF_Metadata.finalFormatter() }
                from (
                    select
                        t1.DA_RUN_ID,
                        t1.BSF_FIL_DT,
                        t1.BSF_VRSN,
                        t1.MSIS_IDENT_NUM,
                        t1.SSN_NUM,
                        t1.SSN_IND,
                        t1.SSN_VRFCTN_IND,
                        t1.MDCR_BENE_ID,
                        t1.MDCR_HICN_NUM,
                        t1.TMSIS_RUN_ID,
                        t1.SUBMTG_STATE_CD,
                        t1.REGION as REG_FLAG,
                        t1.MSIS_CASE_NUM,
                        t1.SINGLE_ENR_FLAG as SNGL_ENRLMT_FLAG,
                        t1.MDCD_ENRLMT_EFF_DT_1,
                        t1.MDCD_ENRLMT_END_DT_1,
                        t1.MDCD_ENRLMT_EFF_DT_2,
                        t1.MDCD_ENRLMT_END_DT_2,
                        t1.MDCD_ENRLMT_EFF_DT_3,
                        t1.MDCD_ENRLMT_END_DT_3,
                        t1.MDCD_ENRLMT_EFF_DT_4,
                        t1.MDCD_ENRLMT_END_DT_4,
                        t1.MDCD_ENRLMT_EFF_DT_5,
                        t1.MDCD_ENRLMT_END_DT_5,
                        t1.MDCD_ENRLMT_EFF_DT_6,
                        t1.MDCD_ENRLMT_END_DT_6,
                        t1.MDCD_ENRLMT_EFF_DT_7,
                        t1.MDCD_ENRLMT_END_DT_7,
                        t1.MDCD_ENRLMT_EFF_DT_8,
                        t1.MDCD_ENRLMT_END_DT_8,
                        t1.MDCD_ENRLMT_EFF_DT_9,
                        t1.MDCD_ENRLMT_END_DT_9,
                        t1.MDCD_ENRLMT_EFF_DT_10,
                        t1.MDCD_ENRLMT_END_DT_10,
                        t1.MDCD_ENRLMT_EFF_DT_11,
                        t1.MDCD_ENRLMT_END_DT_11,
                        t1.MDCD_ENRLMT_EFF_DT_12,
                        t1.MDCD_ENRLMT_END_DT_12,
                        t1.MDCD_ENRLMT_EFF_DT_13,
                        t1.MDCD_ENRLMT_END_DT_13,
                        t1.MDCD_ENRLMT_EFF_DT_14,
                        t1.MDCD_ENRLMT_END_DT_14,
                        t1.MDCD_ENRLMT_EFF_DT_15,
                        t1.MDCD_ENRLMT_END_DT_15,
                        t1.MDCD_ENRLMT_EFF_DT_16,
                        t1.MDCD_ENRLMT_END_DT_16,
                        t1.CHIP_ENRLMT_EFF_DT_1,
                        t1.CHIP_ENRLMT_END_DT_1,
                        t1.CHIP_ENRLMT_EFF_DT_2,
                        t1.CHIP_ENRLMT_END_DT_2,
                        t1.CHIP_ENRLMT_EFF_DT_3,
                        t1.CHIP_ENRLMT_END_DT_3,
                        t1.CHIP_ENRLMT_EFF_DT_4,
                        t1.CHIP_ENRLMT_END_DT_4,
                        t1.CHIP_ENRLMT_EFF_DT_5,
                        t1.CHIP_ENRLMT_END_DT_5,
                        t1.CHIP_ENRLMT_EFF_DT_6,
                        t1.CHIP_ENRLMT_END_DT_6,
                        t1.CHIP_ENRLMT_EFF_DT_7,
                        t1.CHIP_ENRLMT_END_DT_7,
                        t1.CHIP_ENRLMT_EFF_DT_8,
                        t1.CHIP_ENRLMT_END_DT_8,
                        t1.CHIP_ENRLMT_EFF_DT_9,
                        t1.CHIP_ENRLMT_END_DT_9,
                        t1.CHIP_ENRLMT_EFF_DT_10,
                        t1.CHIP_ENRLMT_END_DT_10,
                        t1.CHIP_ENRLMT_EFF_DT_11,
                        t1.CHIP_ENRLMT_END_DT_11,
                        t1.CHIP_ENRLMT_EFF_DT_12,
                        t1.CHIP_ENRLMT_END_DT_12,
                        t1.CHIP_ENRLMT_EFF_DT_13,
                        t1.CHIP_ENRLMT_END_DT_13,
                        t1.CHIP_ENRLMT_EFF_DT_14,
                        t1.CHIP_ENRLMT_END_DT_14,
                        t1.CHIP_ENRLMT_EFF_DT_15,
                        t1.CHIP_ENRLMT_END_DT_15,
                        t1.CHIP_ENRLMT_EFF_DT_16,
                        t1.CHIP_ENRLMT_END_DT_16,
                        t1.ELGBL_1ST_NAME,
                        t1.ELGBL_LAST_NAME,
                        t1.ELGBL_MDL_INITL_NAME,
                        t1.BIRTH_DT,
                        t1.DEATH_DT,
                        t1.AGE as AGE_NUM,
                        t1.AGE_GROUP_FLAG as AGE_GRP_FLAG,
                        t1.DECEASED_FLAG as DCSD_FLAG,
                        t1.SEX_CD,
                        t1.MRTL_STUS_CD,
                        t1.INCM_CD,
                        t1.VET_IND,
                        t1.CTZNSHP_IND,
                        t1.CTZNSHP_VRFCTN_IND,
                        t1.IMGRTN_STUS_CD,
                        t1.IMGRTN_VRFCTN_IND,
                        t1.IMGRTN_STUS_5_YR_BAR_END_DT,
                        t1.PREFRD_LANG_CODE as OTHR_LANG_HOME_CD,
                        t1.PRMRY_LANG_FLAG,
                        t1.ENGLSH_PRFCNCY_CD,
                        t1.HSEHLD_SIZE_CD,
                        t1.AMRCN_INDN_ALSKA_NTV_IND,
                        t1.ETHNCTY_CD,
                        t1.ELGBL_LINE_1_ADR_HOME,
                        t1.ELGBL_LINE_2_ADR_HOME,
                        t1.ELGBL_LINE_3_ADR_HOME,
                        t1.ELGBL_CITY_NAME_HOME,
                        t1.ELGBL_ZIP_CD_HOME,
                        t1.ELGBL_CNTY_CD_HOME,
                        t1.ELGBL_STATE_CD_HOME,
                        t1.ELGBL_PHNE_NUM_HOME,
                        t1.ELGBL_LINE_1_ADR_MAIL,
                        t1.ELGBL_LINE_2_ADR_MAIL,
                        t1.ELGBL_LINE_3_ADR_MAIL,
                        t1.ELGBL_CITY_NAME_MAIL,
                        t1.ELGBL_ZIP_CD_MAIL,
                        t1.ELGBL_CNTY_CD_MAIL,
                        t1.ELGBL_STATE_CD_MAIL,
                        t1.CARE_LVL_STUS_CODE as CARE_LVL_STUS_CD,
                        t1.DEAF_DISAB_FLAG as DEAF_DSBL_FLAG,
                        t1.BLIND_DISAB_FLAG as BLND_DSBL_FLAG,
                        t1.DIFF_CONC_DISAB_FLAG as DFCLTY_CONC_DSBL_FLAG,
                        t1.DIFF_WALKING_DISAB_FLAG as DFCLTY_WLKG_DSBL_FLAG,
                        t1.DIFF_DRESSING_BATHING_DISAB_FLAG as DFCLTY_DRSNG_BATHG_DSBL_FLAG,
                        t1.DIFF_ERRANDS_ALONE_DISAB_FLAG as DFCLTY_ERRANDS_ALN_DSBL_FLAG,
                        t1.OTHER_DISAB_FLAG as OTHR_DSBL_FLAG,
                        t1.HCBS_AGED_NON_HHCC_FLAG,
                        t1.HCBS_PHYS_DISAB_NON_HHCC_FLAG as HCBS_PHYS_DSBL_NON_HHCC_FLAG,
                        t1.HCBS_INTEL_DISAB_NON_HHCC_FLAG as HCBS_INTEL_DSBL_NON_HHCC_FLAG,
                        t1.HCBS_AUTISM_SP_DIS_NON_HHCC_FLAG as HCBS_AUTSM_NON_HHCC_FLAG,
                        t1.HCBS_DD_NON_HHCC_FLAG,
                        t1.HCBS_MI_SED_NON_HHCC_FLAG,
                        t1.HCBS_BRAIN_INJ_NON_HHCC_FLAG as HCBS_BRN_INJ_NON_HHCC_FLAG,
                        t1.HCBS_HIV_AIDS_NON_HHCC_FLAG,
                        t1.HCBS_TECH_DEP_MF_NON_HHCC_FLAG,
                        t1.HCBS_DISAB_OTHER_NON_HHCC_FLAG as HCBS_DSBL_OTHR_NON_HHCC_FLAG,
                        t1.ENROLLMENT_TYPE_FLAG as ENRL_TYPE_FLAG,
                        t1.DAYS_ELIG_IN_MO_CNT,
                        t1.ELIGIBLE_ENTIRE_MONTH_IND as ELGBL_ENTIR_MO_IND,
                        t1.ELIGIBLE_LAST_DAY_OF_MONTH_IND as ELGBL_LAST_DAY_OF_MO_IND,
                        t1.CHIP_CD,
                        t1.ELGBLTY_GRP_CD,
                        t1.PRMRY_ELGBLTY_GRP_IND,
                        t1.ELIGIBILITY_GROUP_CATEGORY_FLAG as ELGBLTY_GRP_CTGRY_FLAG,
                        t1.STATE_SPEC_ELGBLTY_FCTR_TXT,
                        t1.DUAL_ELGBL_CD,
                        t1.DUAL_ELIGIBLE_FLAG as DUAL_ELGBL_FLAG,
                        t1.RSTRCTD_BNFTS_CD,
                        t1.SSDI_IND,
                        t1.SSI_IND,
                        t1.SSI_STATE_SPLMT_STUS_CD,
                        t1.SSI_STUS_CD,
                        t1.BIRTH_CNCPTN_IND,
                        t1.TANF_CASH_CD,
                        t1.HH_PROGRAM_PARTICIPANT_FLAG as HH_PGM_PRTCPNT_FLAG,
                        t1.HH_PRVDR_NUM,
                        t1.HH_ENT_NAME,
                        t1.MH_HH_CHRONIC_COND_FLAG as MH_HH_CHRNC_COND_FLAG,
                        t1.SA_HH_CHRONIC_COND_FLAG as SA_HH_CHRNC_COND_FLAG,
                        t1.ASTHMA_HH_CHRONIC_COND_FLAG as asTHMA_HH_CHRNC_COND_FLAG,
                        t1.DIABETES_HH_CHRONIC_COND_FLAG as DBTS_HH_CHRNC_COND_FLAG,
                        t1.HEART_DIS_HH_CHRONIC_COND_FLAG as HRT_DIS_HH_CHRNC_COND_FLAG,
                        t1.OVERWEIGHT_HH_CHRONIC_COND_FLAG as OVRWT_HH_CHRNC_COND_FLAG,
                        t1.HIV_AIDS_HH_CHRONIC_COND_FLAG as HIV_AIDS_HH_CHRNC_COND_FLAG,
                        t1.OTHER_HH_CHRONIC_COND_FLAG as OTHR_HH_CHRNC_COND_FLAG,
                        t1.LCKIN_PRVDR_NUM1,
                        t1.LCKIN_PRVDR_TYPE_CD1,
                        t1.LCKIN_PRVDR_NUM2,
                        t1.LCKIN_PRVDR_TYPE_CD2,
                        t1.LCKIN_PRVDR_NUM3,
                        t1.LCKIN_PRVDR_TYPE_CD3,
                        t1.LOCK_IN_FLAG as LCKIN_FLAG,
                        t1.LTSS_PRVDR_NUM1,
                        t1.LTSS_LVL_CARE_CD1,
                        t1.LTSS_PRVDR_NUM2,
                        t1.LTSS_LVL_CARE_CD2,
                        t1.LTSS_PRVDR_NUM3,
                        t1.LTSS_LVL_CARE_CD3,
                        t1.MC_PLAN_ID1,
                        t1.MC_PLAN_TYPE_CD1,
                        t1.MC_PLAN_ID2,
                        t1.MC_PLAN_TYPE_CD2,
                        t1.MC_PLAN_ID3,
                        t1.MC_PLAN_TYPE_CD3,
                        t1.MC_PLAN_ID4,
                        t1.MC_PLAN_TYPE_CD4,
                        t1.MC_PLAN_ID5,
                        t1.MC_PLAN_TYPE_CD5,
                        t1.MC_PLAN_ID6,
                        t1.MC_PLAN_TYPE_CD6,
                        t1.MC_PLAN_ID7,
                        t1.MC_PLAN_TYPE_CD7,
                        t1.MC_PLAN_ID8,
                        t1.MC_PLAN_TYPE_CD8,
                        t1.MC_PLAN_ID9,
                        t1.MC_PLAN_TYPE_CD9,
                        t1.MC_PLAN_ID10,
                        t1.MC_PLAN_TYPE_CD10,
                        t1.MC_PLAN_ID11,
                        t1.MC_PLAN_TYPE_CD11,
                        t1.MC_PLAN_ID12,
                        t1.MC_PLAN_TYPE_CD12,
                        t1.MC_PLAN_ID13,
                        t1.MC_PLAN_TYPE_CD13,
                        t1.MC_PLAN_ID14,
                        t1.MC_PLAN_TYPE_CD14,
                        t1.MC_PLAN_ID15,
                        t1.MC_PLAN_TYPE_CD15,
                        t1.MC_PLAN_ID16,
                        t1.MC_PLAN_TYPE_CD16,
                        t1.MFP_LVS_WTH_FMLY_CD,
                        t1.mfp_qlfyd_instn_code as MFP_QLFYD_INSTN_CD,
                        t1.mfp_qlfyd_rsdnc_code as MFP_QLFYD_RSDNC_CD,
                        t1.mfp_prtcptn_endd_rsn_code as MFP_PRTCPTN_ENDD_RSN_CD,
                        t1.mfp_rinstlzd_rsn_code as MFP_RINSTLZD_RSN_CD,
                        t1.MFP_PARTICIPANT_FLAG as MFP_PRTCPNT_FLAG,
                        t1.COMMUNITY_FIRST_CHOICE_SPO_FLAG as CMNTY_1ST_CHS_SPO_FLAG,
                        t1._1915I_SPO_FLAG,
                        t1._1915J_SPO_FLAG,
                        t1._1932A_SPO_FLAG,
                        t1._1915A_SPO_FLAG,
                        t1._1937_ABP_SPO_FLAG,
                        t1._1115A_PARTICIPANT_FLAG as _1115A_PRTCPNT_FLAG,
                        t1.WVR_ID1,
                        t1.WVR_TYPE_CD1,
                        t1.WVR_ID2,
                        t1.WVR_TYPE_CD2,
                        t1.WVR_ID3,
                        t1.WVR_TYPE_CD3,
                        t1.WVR_ID4,
                        t1.WVR_TYPE_CD4,
                        t1.WVR_ID5,
                        t1.WVR_TYPE_CD5,
                        t1.WVR_ID6,
                        t1.WVR_TYPE_CD6,
                        t1.WVR_ID7,
                        t1.WVR_TYPE_CD7,
                        t1.WVR_ID8,
                        t1.WVR_TYPE_CD8,
                        t1.WVR_ID9,
                        t1.WVR_TYPE_CD9,
                        t1.WVR_ID10,
                        t1.WVR_TYPE_CD10,
                        t1.TPL_INSRNC_CVRG_IND,
                        t1.TPL_OTHR_CVRG_IND,
                        t1.SECT_1115A_DEMO_IND,
                        t1.NATIVE_HI_FLAG as NTV_HI_FLAG,
                        t1.GUAM_CHAMORRO_FLAG,
                        t1.SAMOAN_FLAG,
                        t1.OTHER_PAC_ISLANDER_FLAG as OTHR_PAC_ISLNDR_FLAG,
                        t1.UNK_PAC_ISLANDER_FLAG as UNK_PAC_ISLNDR_FLAG,
                        t1.ASIAN_INDIAN_FLAG as ASN_INDN_FLAG,
                        t1.CHINESE_FLAG,
                        t1.FILIPINO_FLAG,
                        t1.JAPANESE_FLAG,
                        t1.KOREAN_FLAG,
                        t1.VIETNAMESE_FLAG,
                        t1.OTHER_ASIAN_FLAG as OTHR_ASN_FLAG,
                        t1.UNKNOWN_ASIAN_FLAG as UNK_ASN_FLAG,
                        t1.WHITE_FLAG as WHT_FLAG,
                        t1.BLACK_AFRICAN_AMERICAN_FLAG as BLACK_AFRCN_AMRCN_FLAG,
                        t1.AIAN_FLAG,
                        t1.RACE_ETHNICITY_FLAG as RACE_ETHNCTY_FLAG,
                        t1.RACE_ETHNCTY_EXP_FLAG,
                        t1.HISPANIC_ETHNICITY_FLAG as HSPNC_ETHNCTY_FLAG,
                        from_utc_timestamp(current_timestamp(), 'EST') as REC_ADD_TS,
                        cast(NULL as timestamp) as REC_UPDT_TS,
                        t1.ELGBL_ID_ADDTNL,
                        t1.ELGBL_ID_ADDTNL_ENT_ID,
                        t1.ELGBL_ID_ADDTNL_RSN_CHG,
                        t1.ELGBL_ID_MSIS_XWALK,
                        t1.ELGBL_ID_MSIS_XWALK_ENT_ID,
                        t1.ELGBL_ID_MSIS_XWALK_RSN_CHG,
                        t1.ELGBLTY_TRMNTN_RSN as ELGBLTY_TRMNTN_RSN_CD,
                        t1.ELGBL_AFTR_EOM_IND,
                        t1.LCKIN_SRVC1,
                        t1.LCKIN_SRVC2,
                        t1.LCKIN_SRVC3,
                        t1.ELGBL_FED_PVT_LVL_PCTG as FED_PVT_LVL,
                        t1.ELGBLTY_EXTNSN_CD,
                        t1.CNTNUS_ELGBLTY_CD,
                        t1.INCM_STD_CD,   
                        t1.ELGBLTY_RDTRMNTN_DT
                    from
                        bsf_step1 as t1
                )
            """
        return z

    indices = {
        't2': 'ELG00002',
        't3': 'ELG00003',
        't4': 'ELG00004',
        't5': 'ELG00005',
        't6': 'ELG00006',
        't7': 'ELG00007',
        't8': 'ELG00008',
        't9': 'ELG00009',
        't10': 'ELG00010',
        't11': 'ELG00011',
        't12': 'ELG00012',
        't13': 'ELG00013',
        't14': 'ELG00014',
        't15': 'ELG00015',
        't16': 'ELG00016',
        't17': 'ELG00017',
        't18': 'ELG00018',
        't19': 'ELG00020',
        't20': 'TPL00002',
        't21': 'ELG00022'
    }

    st_abbrev = {
       '01' : 'AL',
       '02' : 'AK',
       '04' : 'AZ',
       '05' : 'AR',
       '06' : 'CA',
       '08' : 'CO',
       '09' : 'CT',
       '10' : 'DE',
       '11' : 'DC',
       '12' : 'FL',
       '13' : 'GA',
       '15' : 'HI',
       '16' : 'ID',
       '17' : 'IL',
       '18' : 'IN',
       '19' : 'IA',
       '20' : 'KS',
       '21' : 'KY',
       '22' : 'LA',
       '23' : 'ME',
       '24' : 'MD',
       '25' : 'MA',
       '26' : 'MI',
       '27' : 'MN',
       '28' : 'MS',
       '29' : 'MO',
       '30' : 'MT',
       '31' : 'NE',
       '32' : 'NV',
       '33' : 'NH',
       '34' : 'NJ',
       '35' : 'NM',
       '36' : 'NY',
       '37' : 'NC',
       '38' : 'ND',
       '39' : 'OH',
       '40' : 'OK',
       '41' : 'OR',
       '42' : 'PA',
       '44' : 'RI',
       '45' : 'SC',
       '46' : 'SD',
       '47' : 'TN',
       '48' : 'TX',
       '49' : 'UT',
       '50' : 'VT',
       '51' : 'VA',
       '53' : 'WA',
       '54' : 'WV',
       '55' : 'WI',
       '56' : 'WY',
       '66' : 'GU',
       '72' : 'PR',
       '78' : 'VI',
    }

    def join_segments(BSF_FILE_DATE):
        """
        Join BSF File Date segments.
        """

        joins = []
        new_line = '\n\t\t\t'
        for t, tbl in BSF_Metadata.indices.items():
            joins.append(f"""left join {tbl}_{BSF_FILE_DATE}_uniq {t}
                     on t1.SUBMTG_STATE_CD = {t}.SUBMTG_STATE_CD
                    and t1.MSIS_IDENT_NUM  = {t}.MSIS_IDENT_NUM
                """.format())
        return new_line.join(joins)

    def dedup_tbl_joiner(tab_no: str, range: range, max_keep: int):
        """
        Function to deduplicate table when joining.  
        """

        joins = []
        new_line = '\n\t\t\t'

        for i in list(range):
            if i <= max_keep:
                joins.append(f"""
                    left join (select * from {tab_no}_step2 where keeper={i}) t{i}
                         on t1.submtg_state_cd = t{i}.submtg_state_cd
                        and t1.msis_ident_num  = t{i}.msis_ident_num
                """.format())
        return new_line.join(joins)

    def tbl_joiner(tab_no: str, tblnum: int, type):
        """
        Function to join tables on submitting state code and msis_ident_num
        """

        return f"""
            left join (select * from {tab_no}_{type}_step4 where keeper={tblnum}) t{tblnum}
                 on m.submtg_state_cd = t{tblnum}.submtg_state_cd
                and m.msis_ident_num  = t{tblnum}.msis_ident_num
        """

    def cleanSubmittingStateCd(alias):
        """
        Function to clean submitting state code by aliasing them to standard codes.  
        """

        return f"""case
            when {alias}.SUBMTG_STATE_CD = '96' then '19'
            when {alias}.SUBMTG_STATE_CD = '97' then '42'
            when {alias}.SUBMTG_STATE_CD = '93' then '56'
            when {alias}.SUBMTG_STATE_CD = '94' then '30'
            else {alias}.SUBMTG_STATE_CD end as SUBMTG_STATE_CD
        """

    def cleanSSN(alias):
        """
        Clean social socurity number by left padding to a total length of 9 characters.
        """

        return f"lpad(cast({alias}.SSN_NUM as char(9)), 9, '0') as SSN_NUM"

    def cleanDisabilityTypeCd(alias):
        """
        Clean disability type code by left padding to a total length of 2 characters.  
        """

        return f"lpad(trim({alias}.DSBLTY_TYPE_CD), 2, '0') as DSBLTY_TYPE_CD"

    def cleanNDC_UOM_CHRNC_NON_HH_CD(alias):
        """
        Clean by left padding to a total length of 3 characters for NDC_UOM_CHRNC_NON_HH_CD.
        """

        return f"lpad(trim({alias}.NDC_UOM_CHRNC_NON_HH_CD), 3, '0') as NDC_UOM_CHRNC_NON_HH_CD"

    def cleanPreferredLangCd(alias):
        """
        Clean Preferred Language Code by uppercasing the language names.  
        """

        return f"upper({alias}.PREFRD_LANG_CD) as PREFRD_LANG_CD"

    def cleanImmigrationStatusCd(alias):
        """
        Clean immigration Status Code by aliasing "8" to "0" and uppercasing the code names.  
        """

        return f"""case
            when {alias}.IMGRTN_STUS_CD = '8' then '0'
            else upper(nullif(trim({alias}.IMGRTN_STUS_CD),'')) end as IMGRTN_STUS_CD
        """

    def cleanRaceCd(alias):
        """
        Clean race code values by left padding to a total length of 3 characters.
        """

        return f"lpad(trim({alias}.race_cd), 3, '0') AS race_cd"

    # ---------------------------------------------------------------------------------
    #
    #  'C' Chinese
    #  'D' German
    #  'E' English
    #  'F' French
    #  'G' Greek
    #  'I' Italian
    #  'J' Japanese
    #  'N' Norwegian
    #  'P' Polish
    #  'R' Russian
    #  'S' Spanish
    #  'V' Swedish
    #  'W' Serbo-Croatian
    #
    # ---------------------------------------------------------------------------------
    @staticmethod
    def encodePreferredLanguage():
        """
        Function to encode Preferred Languages.  
        """

        return """case
             when trim(PREFRD_LANG_CODE) in('CHI','ZHO')                         then  'C'
             when trim(PREFRD_LANG_CODE) in('DEU','GER','GMH','GOH','GSW','NDS') then  'D'
             when trim(PREFRD_LANG_CODE) in('ENG','ENM','ANG')                   then  'E'
             when trim(PREFRD_LANG_CODE) in('FRA','FRE','FRM','FRO')             then  'F'
             when trim(PREFRD_LANG_CODE) in('ELL','GRC','GRE')                   then  'G'
             when trim(PREFRD_LANG_CODE) in('ITA','SCN')                         then  'I'
             when trim(PREFRD_LANG_CODE) in('JPN')                               then  'J'
             when trim(PREFRD_LANG_CODE) in('NOB','NNO','NOR')                   then  'N'
             when trim(PREFRD_LANG_CODE) in('POL')                               then  'P'
             when trim(PREFRD_LANG_CODE) in('RUS')                               then  'R'
             when trim(PREFRD_LANG_CODE) in('SPA')                               then  'S'
             when trim(PREFRD_LANG_CODE) in('SWE')                               then  'V'
             when trim(PREFRD_LANG_CODE) in('SRP','HRV')                         then  'W'
             when trim(PREFRD_LANG_CODE) in('UND','','.')
                  or PREFRD_LANG_CODE is null                                    then  null
             else 'O' end as PRMRY_LANG_FLG
        """

    @staticmethod
    def encodeStateAsRegion():
        """
        Function to encode states to their region codes.  
        """

        return """case
            when ST_ABBREV in('CT','MA','ME','NH','RI','VT')           then '01'
            when ST_ABBREV in('NJ','NY','PR','VI')                     then '02'
            when ST_ABBREV in('DE','DC','MD','PA','VA','WV')           then '03'
            when ST_ABBREV in('AL','FL','GA','KY','MS','NC','SC','TN') then '04'
            when ST_ABBREV in('IL','IN','MI','MN','OH','WI')           then '05'
            when ST_ABBREV in('AR','LA','NM','OK','TX')                then '06'
            when ST_ABBREV in('IA','KS','MO','NE')                     then '07'
            when ST_ABBREV in('CO','MT','ND','SD','UT','WY')           then '08'
            when ST_ABBREV in('AZ','CA','HI','NV','AS','GU','MP')      then '09'
            when ST_ABBREV in('AK','ID','OR','WA')                     then '10'
            else '11' end as REGION
        """

    def maskInvalidValues(column: str, alias: str):
        """
        Function to mask invalid values using the validator() function.  
        """

        delim = '\',\''
        return f"""case
            when upper(trim({alias}.{column}))
                in('{delim.join(BSF_Metadata.validator.get(column))}')
                    then upper(trim({alias}.{column}))
                        else null end as {column}
        """

    def rename(column: str):
        """
        Function to rename columns based on a list of columns to be renamed.  
        """

        if column in BSF_Metadata.renames.keys():
            return f"{BSF_Metadata.renames.get(column)}"
        else:
            return column

    def normalize(column: str):
        """
        Normalize function.  
        """

        if column in BSF_Metadata.conform:
            return f"upper(nullif(trim({column}),'')) as {BSF_Metadata.rename(column)}"
        elif list(filter(column.startswith, BSF_Metadata.enumcols)) != []:
            return f"upper(nullif(trim({column}),'')) as {column}"
        else:
            return BSF_Metadata.rename(column)

    @staticmethod
    def epoch(column: str):
        """
        Function that takes any date earlier than 1600-01-01 and defaults it to 1599-12-31.
        """
        return f"""case
              when {column} < '1600-01-01' then CAST('1599-12-31' AS DATE)
              else CAST({column} AS DATE) 
              end as {column}"""

    cleanser = {
        'SSN_NUM': cleanSSN,
        'PREFRD_LANG_CD': cleanPreferredLangCd,
        'DSBLTY_TYPE_CD': cleanDisabilityTypeCd,
        'NDC_UOM_CHRNC_NON_HH_CD': cleanNDC_UOM_CHRNC_NON_HH_CD,
        'IMGRTN_STUS_CD': cleanImmigrationStatusCd,
        'RACE_CD': cleanRaceCd,
    }

    absent = [
        'PRGNT_IND',
        'PRGNCY_FLAG',
        '_1115A_PRTCPNT_FLAG',
        'MAS_CD',
        'ELGBLTY_MDCD_BASIS_CD',
        'MASBOE_CD',
    ]

    enumcols = [
        'LCKIN_PRVDR_NUM',
        'LCKIN_PRVDR_TYPE_CD',
        'LTSS_PRVDR_NUM',
        'LTSS_LVL_CARE_CD',
        'MC_PLAN_ID',
        'MC_PLAN_TYPE_CD',
        'WVR_ID',
        'WVR_TYPE_CD'
    ]

    epochal = [
        'BIRTH_DT',
        'DEATH_DT',
        'IMGRTN_STUS_5_YR_BAR_END_DT',
        'ELGBLTY_RDTRMNTN_DT',
    ]

    renames = {
        '_1115A_PARTICIPANT_FLAG': '_1115A_PRTCPNT_FLAG',
        ',DUAL_ELGBL_CODE': 'DUAL_ELGBL_CD',
        'AGE_GROUP_FLAG': 'AGE_GRP_FLAG',
        'AGE': 'AGE_NUM',
        'ASIAN_INDIAN_FLAG': 'asn_indn_flag',
        'ASTHMA_HH_CHRONIC_COND_FLAG': 'ASTHMA_HH_CHRNC_COND_FLAG',
        'BLACK_AFRICAN_AMERICAN_FLAG': 'black_afrcn_amrcn_flag',
        'BLIND_DISAB_FLAG': 'BLND_DSBL_FLAG',
        'CARE_LVL_STUS_CODE': 'CARE_LVL_STUS_CD',
        'COMMUNITY_FIRST_CHOICE_SPO_FLAG': 'CMNTY_1ST_CHS_SPO_FLAG',
        'DEAF_DISAB_FLAG': 'DEAF_DSBL_FLAG',
        'DECEASED_FLAG': 'DCSD_FLAG',
        'DIABETES_HH_CHRONIC_COND_FLAG': 'DBTS_HH_CHRNC_COND_FLAG',
        'DIFF_CONC_DISAB_FLAG': 'DFCLTY_CONC_DSBL_FLAG',
        'DIFF_DRESSING_BATHING_DISAB_FLAG': 'DFCLTY_DRSNG_BATHG_DSBL_FLAG',
        'DIFF_ERRANDS_ALONE_DISAB_FLAG': 'DFCLTY_ERRANDS_ALN_DSBL_FLAG',
        'DIFF_WALKING_DISAB_FLAG': 'DFCLTY_WLKG_DSBL_FLAG',
        'DUAL_ELIGIBLE_FLAG': 'DUAL_ELGBL_FLAG',
        'ELGBLTY_GRP_CODE': 'ELGBLTY_GRP_CD',
        'ELIGIBILITY_GROUP_CATEGORY_FLAG': 'ELGBLTY_GRP_CTGRY_FLAG',
        'ELIGIBLE_ENTIRE_MONTH_IND': 'ELGBL_ENTIR_MO_IND',
        'ELIGIBLE_LAST_DAY_OF_MONTH_IND': 'ELGBL_LAST_DAY_OF_MO_IND',
        'ENROLLMENT_TYPE_FLAG': 'ENRL_TYPE_FLAG',
        'SEX_CODE': 'SEX_CD',
        'HCBS_AUTISM_SP_DIS_NON_HHCC_FLAG': 'HCBS_AUTSM_NON_HHCC_FLAG',
        'HCBS_BRAIN_INJ_NON_HHCC_FLAG': 'HCBS_BRN_INJ_NON_HHCC_FLAG',
        'HCBS_DISAB_OTHER_NON_HHCC_FLAG': 'HCBS_DSBL_OTHR_NON_HHCC_FLAG',
        'HCBS_INTEL_DISAB_NON_HHCC_FLAG': 'HCBS_INTEL_DSBL_NON_HHCC_FLAG',
        'HCBS_PHYS_DISAB_NON_HHCC_FLAG': 'HCBS_PHYS_DSBL_NON_HHCC_FLAG',
        'HEART_DIS_HH_CHRONIC_COND_FLAG': 'HRT_DIS_HH_CHRNC_COND_FLAG',
        'HH_PROGRAM_PARTICIPANT_FLAG': 'HH_PGM_PRTCPNT_FLAG',
        'HISPANIC_ETHNICITY_FLAG': 'HSPNC_ETHNCTY_FLAG',
        'HIV_AIDS_HH_CHRONIC_COND_FLAG': 'HIV_AIDS_HH_CHRNC_COND_FLAG',
        'MFP_PARTICIPANT_FLAG': 'MFP_PRTCPNT_FLAG',
        'MFP_PRTCPTN_ENDD_RSN_CODE': 'MFP_PRTCPTN_ENDD_RSN_CD',
        'MFP_QLFYD_INSTN_CODE': 'MFP_QLFYD_INSTN_CD',
        'MFP_QLFYD_RSDNC_CODE': 'MFP_QLFYD_RSDNC_CD',
        'MFP_RINSTLZD_RSN_CODE': 'MFP_RINSTLZD_RSN_CD',
        'MH_HH_CHRONIC_COND_FLAG': 'MH_HH_CHRNC_COND_FLAG',
        'NATIVE_HI_FLAG': 'NTV_HI_FLAG',
        'OTHER_ASIAN_FLAG': 'othr_asn_flag',
        'OTHER_DISAB_FLAG': 'OTHR_DSBL_FLAG',
        'OTHER_HH_CHRONIC_COND_FLAG': 'OTHR_HH_CHRNC_COND_FLAG',
        'OTHER_PAC_ISLANDER_FLAG': 'othr_pac_islndr_flag',
        'PREFRD_LANG_CODE': 'OTHR_LANG_HOME_CD',
        'OVERWEIGHT_HH_CHRONIC_COND_FLAG': 'OVRWT_HH_CHRNC_COND_FLAG',
        'RACE_ETHNICITY_FLAG': 'RACE_ETHNCTY_FLAG',
        'REGION': 'REG_FLAG',
        'SA_HH_CHRONIC_COND_FLAG': 'SA_HH_CHRNC_COND_FLAG',
        'SINGLE_ENR_FLAG': 'SNGL_ENRLMT_FLAG',
        'UNK_PAC_ISLANDER_FLAG': 'unk_pac_islndr_flag',
        'UNKNOWN_ASIAN_FLAG': 'unk_asn_flag',
        'WHITE_FLAG': 'wht_flag',
        'LOCK_IN_FLAG': 'LCKIN_FLAG'
    }

    conform = [
        'BIRTH_CNCPTN_IND',
        'CARE_LVL_STUS_CD',
        'CHIP_CD',
        'AMRCN_INDN_ALSKA_NTV_IND',
        'CTZNSHP_IND',
        'CTZNSHP_VRFCTN_IND',
        'DUAL_ELGBL_CD',
        'ELGBL_1ST_NAME',
        'ELGBL_CITY_NAME_HOME',
        'ELGBL_CITY_NAME_MAIL',
        'ELGBL_CNTY_CD_HOME',
        'ELGBL_CNTY_CD_MAIL',
        'ELGBL_ID_ADDTNL_ENT_ID',
        'ELGBL_ID_ADDTNL_RSN_CHG',
        'ELGBL_ID_ADDTNL',
        'ELGBL_ID_MSIS_XWALK_ENT_ID',
        'ELGBL_ID_MSIS_XWALK_RSN_CHG',
        'ELGBL_ID_MSIS_XWALK',
        'ELGBL_LAST_NAME',
        'ELGBL_LINE_1_ADR_HOME',
        'ELGBL_LINE_1_ADR_MAIL',
        'ELGBL_LINE_2_ADR_HOME',
        'ELGBL_LINE_2_ADR_MAIL',
        'ELGBL_LINE_3_ADR_HOME',
        'ELGBL_LINE_3_ADR_MAIL',
        'ELGBL_MDL_INITL_NAME',
        'ELGBL_PHNE_NUM_HOME',
        'ELGBL_STATE_CD_HOME',
        'ELGBL_STATE_CD_MAIL',
        'ELGBL_ZIP_CD_HOME',
        'ELGBL_ZIP_CD_MAIL',
        'ELGBLTY_TRMNTN_RSN'
        'ELGBLTY_GRP_CD',
        'ETHNCTY_CD',
        'SEX_CD',
        'HH_ENT_NAME',
        'HH_PRVDR_NUM',
        'HSEHLD_SIZE_CD',
        'IMGRTN_VRFCTN_IND',
        'INCM_CD',
        'MDCR_BENE_ID',
        'MDCR_HICN_NUM',
        'MFP_LVS_WTH_FMLY_CD',
        'MFP_PRTCPTN_ENDD_RSN_CD',
        'MFP_QLFYD_INSTN_CD',
        'MFP_QLFYD_RSDNC_CD',
        'MFP_RINSTLZD_RSN_CD',
        'MRTL_STUS_CD',
        'MSIS_CASE_NUM',
        'OTHR_LANG_HOME_CD',
        'PRMRY_ELGBLTY_GRP_IND',
        'ENGLSH_PRFCNCY_CD',
        'RSTRCTD_BNFTS_CD',
        'SECT_1115A_DEMO_IND',
        'SSDI_IND',
        'SSI_IND',
        'SSI_STATE_SPLMT_STUS_CD',
        'SSI_STUS_CD',
        'SSN_IND',
        'SSN_VRFCTN_IND',
        'STATE_SPEC_ELGBLTY_FCTR_TXT',
        'TANF_CASH_CD',
        'TPL_INSRNC_CVRG_IND',
        'TPL_OTHR_CVRG_IND',
        'VET_IND',
        'ELGBLTY_EXTNSN_CD',
        'CNTNUS_ELGBLTY_CD',
        'INCM_STD_CD',
    ]

    created_vars = {

        'ELG00002': None,
        'ELG00003': None,
        'ELG00004': None,
        'ELG00005': None,
        'ELG00006': None,
        'ELG00007': None,
        'ELG00010': None,
        'ELG00015': None,
        'ELG00018': None,
        'TPL00002': None,
        'ELG00022': None
    }

    final = {

        'ELG00001': [],

        'ELG00002': [
            'BIRTH_DT',
            'SEX_CD',
            'ELGBL_1ST_NAME',
            'ELGBL_LAST_NAME',
            'ELGBL_MDL_INITL_NAME',
            'PRMRY_DMGRPHC_ELE_EFCTV_DT',
            'PRMRY_DMGRPHC_ELE_END_DT'
        ],

        'ELG00002A': [],

        'ELG00003': [
            'SSN_NUM',
            'MRTL_STUS_CD',
            'SSN_VRFCTN_IND',
            'INCM_CD',
            'VET_IND',
            'CTZNSHP_IND',
            'CTZNSHP_VRFCTN_IND',
            'IMGRTN_STUS_CD',
            'IMGRTN_STUS_5_YR_BAR_END_DT',
            'IMGRTN_VRFCTN_IND',
            'PREFRD_LANG_CD',
            'ENGLSH_PRFCNCY_CD',
            'HSEHLD_SIZE_CD',
            'PRGNT_IND',
            'MDCR_HICN_NUM',
            'MDCR_BENE_ID',
            'CHIP_CD',
            'VAR_DMGRPHC_ELE_EFCTV_DT',
            'VAR_DMGRPHC_ELE_END_DT',
            'ELGBL_FED_PVT_LVL_PCTG',
        ],

        'ELG00003A': [],

        'ELG00004': [
            'ELGBL_ADR_EFCTV_DT',
            'ELGBL_ADR_END_DT',
            'ELGBL_LINE_1_ADR_HOME',
            'ELGBL_LINE_2_ADR_HOME',
            'ELGBL_LINE_3_ADR_HOME',
            'ELGBL_CITY_NAME_HOME',
            'ELGBL_CNTY_CD_HOME',
            'ELGBL_PHNE_NUM_HOME',
            'ELGBL_STATE_CD_HOME',
            'ELGBL_ZIP_CD_HOME',
            'ELGBL_LINE_1_ADR_MAIL',
            'ELGBL_LINE_2_ADR_MAIL',
            'ELGBL_LINE_3_ADR_MAIL',
            'ELGBL_CITY_NAME_MAIL',
            'ELGBL_CNTY_CD_MAIL',
            'ELGBL_PHNE_NUM_MAIL',
            'ELGBL_STATE_CD_MAIL',
            'ELGBL_ZIP_CD_MAIL'
        ],

        'ELG00005': [
            'MSIS_CASE_NUM',
            'CARE_LVL_STUS_CD',
            'SSDI_IND',
            'SSI_IND',
            'SSI_STATE_SPLMT_STUS_CD',
            'SSI_STUS_CD',
            'STATE_SPEC_ELGBLTY_FCTR_TXT',
            'BIRTH_CNCPTN_IND',
            'RSTRCTD_BNFTS_CD',
            'TANF_CASH_CD',
            'ELGBLTY_DTRMNT_EFCTV_DT',
            'ELGBLTY_DTRMNT_END_DT',
            'PRMRY_ELGBLTY_GRP_IND',
            'ELGBLTY_RDTRMNTN_DT',            
        ],

        'ELG00006': [
            'HH_ENT_NAME',
            'HH_SNTRN_NAME',
            'HH_SNTRN_PRTCPTN_EFCTV_DT',
            'HH_SNTRN_PRTCPTN_END_DT'
        ],

        'ELG00007': [
            'HH_PRVDR_NUM',
            'HH_SNTRN_PRVDR_EFCTV_DT',
            'HH_SNTRN_PRVDR_END_DT',
        ],

        'ELG00008': [],
        'ELG00009': [],

        'ELG00010': [
            'MFP_ENRLMT_EFCTV_DT',
            'MFP_ENRLMT_END_DT',
            'MFP_LVS_WTH_FMLY_CD',
            'MFP_QLFYD_INSTN_CD',
            'MFP_QLFYD_RSDNC_CD',
            'MFP_PRTCPTN_ENDD_RSN_CD',
            'MFP_RINSTLZD_RSN_CD'
        ],

        'ELG00011': [],

        'ELG00012': [],

        'ELG00013': [],

        'ELG00014': [],

        'ELG00015': [
            'ETHNCTY_DCLRTN_EFCTV_DT',
            'ETHNCTY_DCLRTN_END_DT',
            'ETHNCTY_CD'
        ],

        'ELG00016': [
            'AMRCN_INDN_ALSKA_NTV_IND'
        ],

        'ELG00017': [],

        'ELG00018': [
            'SECT_1115A_DEMO_IND',
            'SECT_1115A_DEMO_EFCTV_DT',
            'SECT_1115A_DEMO_END_DT'
        ],

        'ELG00020': [],

        'ELG00021': [
            'ELGBL_ID_ADDTNL',
            'ELGBL_ID_ADDTNL_ENT_ID',
            'ELGBL_ID_ADDTNL_RSN_CHG',
            'ELGBL_ID_MSIS_XWALK',
            'ELGBL_ID_MSIS_XWALK_ENT_ID',
            'ELGBL_ID_MSIS_XWALK_RSN_CHG'
        ],

        'TPL00002': [
            'ELGBL_PRSN_MN_EFCTV_DT',
            'ELGBL_PRSN_MN_END_DT',
            'TPL_INSRNC_CVRG_IND',
            'TPL_OTHR_CVRG_IND'
        ],

        'ELG00022': []
    }

    columns = {

        'ELG00001': [
            'TMSIS_RUN_ID',
            'TMSIS_FIL_NAME',
            'TMSIS_OFST_BYTE_NUM',
            'TMSIS_COMT_ID',
            'TMSIS_DLTD_IND',
            'TMSIS_OBSLT_IND',
            'TMSIS_ACTV_IND',
            'TMSIS_SQNC_NUM',
            'TMSIS_RUN_TS',
            'TMSIS_RPTG_PRD',
            'TMSIS_REC_MD5_B64_TXT',
            'REC_TYPE_CD',
            'DATA_DCTNRY_VRSN_NUM',
            'DATA_MPNG_DOC_VRSN_NUM',
            'FIL_CREATD_DT',
            'PRD_END_TIME',
            'FIL_ENCRPTN_SPEC_CD',
            'FIL_NAME',
            'FIL_STUS_CD',
            'SQNC_NUM',
            'SSN_IND',
            'PRD_EFCTV_TIME',
            'STATE_NOTN_TXT',
            'SUBMSN_TRANS_TYPE_CD',
            'SUBMTG_STATE_CD',
            'TOT_REC_CNT'
        ],

        'ELG00002': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'DEATH_DT',
            'BIRTH_DT',
            'SEX_CD',
            'ELGBL_1ST_NAME',
            'ELGBL_LAST_NAME',
            'ELGBL_MDL_INITL_NAME',
            'PRMRY_DMGRPHC_ELE_EFCTV_DT',
            'PRMRY_DMGRPHC_ELE_END_DT'
        ],

        'ELG00002A': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'DEATH_DT',
            'PRMRY_DMGRPHC_ELE_EFCTV_DT',
            'PRMRY_DMGRPHC_ELE_END_DT'
        ],

        'ELG00003': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'SSN_NUM',
            'MRTL_STUS_CD',
            'SSN_VRFCTN_IND',
            'INCM_CD',
            'VET_IND',
            'CTZNSHP_IND',
            'CTZNSHP_VRFCTN_IND',
            'IMGRTN_STUS_CD',
            'IMGRTN_STUS_5_YR_BAR_END_DT',
            'IMGRTN_VRFCTN_IND',
            'PREFRD_LANG_CD',
            'ENGLSH_PRFCNCY_CD',
            'HSEHLD_SIZE_CD',
            'PRGNT_IND',
            'MDCR_HICN_NUM',
            'MDCR_BENE_ID',
            'CHIP_CD',
            'VAR_DMGRPHC_ELE_EFCTV_DT',
            'VAR_DMGRPHC_ELE_END_DT',
            'ELGBL_FED_PVT_LVL_PCTG',
        ],

        'ELG00003A': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'PRGNT_IND ',
            'SUBMTG_STATE_CD',
            'VAR_DMGRPHC_ELE_EFCTV_DT',
            'VAR_DMGRPHC_ELE_END_DT',
            'REC_NUM'
        ],

        'ELG00004': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'ELGBL_LINE_1_ADR',
            'ELGBL_LINE_2_ADR',
            'ELGBL_LINE_3_ADR',
            'ELGBL_ADR_TYPE_CD',
            'ELGBL_CITY_NAME',
            'ELGBL_CNTY_CD',
            'ELGBL_PHNE_NUM',
            'ELGBL_STATE_CD',
            'ELGBL_ZIP_CD',
            'ELGBL_ADR_EFCTV_DT',
            'ELGBL_ADR_END_DT',
        ],

        'ELG00005': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'ELGBLTY_GRP_CD',
            'DUAL_ELGBL_CD',
            'ELGBLTY_TRMNTN_RSN',
            'MSIS_CASE_NUM',
            'CARE_LVL_STUS_CD',
            'SSDI_IND',
            'SSI_IND',
            'SSI_STATE_SPLMT_STUS_CD',
            'SSI_STUS_CD',
            'STATE_SPEC_ELGBLTY_FCTR_TXT',
            'BIRTH_CNCPTN_IND',
            'RSTRCTD_BNFTS_CD',
            'TANF_CASH_CD',
            'ELGBLTY_DTRMNT_EFCTV_DT',
            'ELGBLTY_DTRMNT_END_DT',
            'PRMRY_ELGBLTY_GRP_IND',
            'ELGBLTY_EXTNSN_CD',
            'CNTNUS_ELGBLTY_CD',
            'INCM_STD_CD',
            'ELGBLTY_RDTRMNTN_DT',
        ],

        'ELG00006': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'HH_ENT_NAME',
            'HH_SNTRN_NAME',
            'HH_SNTRN_PRTCPTN_EFCTV_DT',
            'HH_SNTRN_PRTCPTN_END_DT'
        ],

        'ELG00007': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'HH_PRVDR_NUM',
            'HH_SNTRN_PRVDR_EFCTV_DT',
            'HH_SNTRN_PRVDR_END_DT'
        ],

        'ELG00008': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'HH_CHRNC_CD',
            'HH_CHRNC_EFCTV_DT',
            'HH_CHRNC_END_DT'
        ],

        'ELG00009': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'LCKIN_EFCTV_DT',
            'LCKIN_END_DT',
            'LCKIN_PRVDR_NUM',
            'LCKIN_PRVDR_TYPE_CD',
            'LCKD_IN_SRVC',
        ],

        'ELG00010': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'MFP_ENRLMT_EFCTV_DT',
            'MFP_ENRLMT_END_DT',
            'MFP_LVS_WTH_FMLY_CD',
            'MFP_QLFYD_INSTN_CD',
            'MFP_QLFYD_RSDNC_CD',
            'MFP_PRTCPTN_ENDD_RSN_CD',
            'MFP_RINSTLZD_RSN_CD'
        ],

        'ELG00011': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'STATE_PLAN_OPTN_EFCTV_DT',
            'STATE_PLAN_OPTN_END_DT',
            'STATE_PLAN_OPTN_TYPE_CD'
        ],

        'ELG00012': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'WVR_ENRLMT_EFCTV_DT',
            'WVR_ENRLMT_END_DT',
            'WVR_ID',
            'WVR_TYPE_CD'
        ],

        'ELG00013': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'LTSS_ELGBLTY_EFCTV_DT',
            'LTSS_ELGBLTY_END_DT',
            'LTSS_LVL_CARE_CD',
            'LTSS_PRVDR_NUM'
        ],

        'ELG00014': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'MC_PLAN_ID',
            'MC_PLAN_TYPE_CD',
            'MC_PLAN_ENRLMT_EFCTV_DT',
            'MC_PLAN_ENRLMT_END_DT'
        ],

        'ELG00015': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'ETHNCTY_DCLRTN_EFCTV_DT',
            'ETHNCTY_DCLRTN_END_DT',
            'ETHNCTY_CD'
        ],

        'ELG00016': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'RACE_CD',
            'RACE_DCLRTN_EFCTV_DT',
            'RACE_DCLRTN_END_DT',
            'RACE_OTHR_TXT',
            'AMRCN_INDN_ALSKA_NTV_IND'
        ],

        'ELG00017': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'DSBLTY_TYPE_CD',
            'DSBLTY_TYPE_EFCTV_DT',
            'DSBLTY_TYPE_END_DT'
        ],

        'ELG00018': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'SECT_1115A_DEMO_IND',
            'SECT_1115A_DEMO_EFCTV_DT',
            'SECT_1115A_DEMO_END_DT'
        ],

        'ELG00020': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'NDC_UOM_CHRNC_NON_HH_CD',
            'NDC_UOM_CHRNC_NON_HH_EFCTV_DT',
            'NDC_UOM_CHRNC_NON_HH_END_DT'
        ],

        'ELG00021': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'ENRLMT_EFCTV_DT',
            'ENRLMT_END_DT',
            'ENRLMT_TYPE_CD'
        ],

        'ELG00022': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'ELGBL_ID_EFCTV_DT',
            'ELGBL_ID_END_DT',
            'ELGBL_ID_TYPE_CD',
            'ELGBL_ID',
            'ELGBL_ID_ISSG_ENT_ID_TXT',
            'RSN_FOR_CHG'
        ],

        'TPL00002': [
            'TMSIS_RUN_ID',
            'TMSIS_ACTV_IND',
            'SUBMTG_STATE_CD',
            'REC_NUM',
            'ELGBL_PRSN_MN_EFCTV_DT',
            'ELGBL_PRSN_MN_END_DT',
            'TPL_INSRNC_CVRG_IND',
            'TPL_OTHR_CVRG_IND'
        ],

    }

    validator = {

        'SEX_CD':
            ['M', 'F'],
        'MRTL_STUS_CD':
            ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14',
             '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28',
             '29', '30', '31', '32', '33'],
        'SSN_VRFCTN_IND':
            ['0', '1', '2'],
        'INCM_CD':
            ['01', '02', '03', '04', '05', '06', '07', '08'],
        'VET_IND':
            ['0', '1'],
        'CTZNSHP_IND':
            ['0', '1', '2'],
        'CTZNSHP_VRFCTN_IND':
            ['0', '1'],
        'IMGRTN_VRFCTN_IND':
            ['0', '1'],
        'IMGRTN_STUS_CD':
            ['1', '2', '3', '8'],
        'ENGLSH_PRFCNCY_CD':
            ['0', '1', '2', '3'],
        'HSEHLD_SIZE_CD':
            ['01', '02', '03', '04', '05', '06', '07', '08'],
        'PRGNT_IND':
            ['0', '1'],
        'CHIP_CD':
            ['0', '1', '2', '3', '4'],
        'ELGBL_STATE_CD':
            ['1', '2', '4', '5', '6', '8', '9', '01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19',
             '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41',
             '42', '44', '45', '46', '47', '48', '49', '50', '51', '53', '54', '55', '56', '60', '64', '66', '67', '68', '69', '70', '71', '72',
             '74', '76', '78', '79', '81', '84', '86', '89', '95'],
        'ELGBLTY_GRP_CD':
            ['01', '02', '03', '04', '05', '06', '07', '08', '09', '1', '2', '3', '4', '5', '6', '7', '8', '9', '11', '12', '13', '14', '15',
             '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37',
             '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '59', '60', '61',
             '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76'],
        'ELGBLTY_TRMNTN_RSN':
            ['01', '02', '03', '04', '05', '06', '07', '08', '09', '1', '2', '3', '4', '5', '6', '7', '8', '9',
             '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22',
             '23', '24', '25', '26', '27', '28', '29', '30', '31'],
        'DUAL_ELGBL_CD':
            ['00', '01', '02', '03', '04', '05', '06', '08', '09', '10', '0', '1', '2', '3', '4', '5', '6', '8', '9'],
        'CARE_LVL_STUS_CD':
            ['001', '002', '003', '004', '005', '01', '02', '03', '04', '05', '1', '2', '3', '4', '5'],
        'SSDI_IND':
            ['0', '1'],
        'SSI_IND':
            ['0', '1'],
        'SSI_STATE_SPLMT_STUS_CD':
            ['000', '001', '002'],
        'SSI_STUS_CD':
            ['000', '001', '002'],
        'BIRTH_CNCPTN_IND':
            ['0', '1'],
        'RSTRCTD_BNFTS_CD':
            ['0', '1', '2', '3', '4', '5', '6', '7', 'A', 'B', 'C', 'D', 'E', 'F', 'G'],
        'TANF_CASH_CD':
            ['0', '1', '2'],
        'PRMRY_ELGBLTY_GRP_IND':
            ['0', '1'],
        'LCKIN_PRVDR_TYPE_CD':
            ['1', '2', '3', '4', '5', '6', '7', '8', '9', 
             '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
             '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', 
             '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', 
             '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', 
             '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', 
             '51', '52', '53', '54', '55', '56', '57', '58'],
        'LCKD_IN_SRVC':
            ['1', '2', '3', '4', '5', '6', '7', '8', '9', 
             '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
             '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
             '21', '22', '23', '24', '25', '26', '27', '28', '29', '30',
             '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
             '41', '42', '43', '44', '45', '46', '47', '48', '49', '50',
             '51', '52', '53', '54', '55', '56', '57', '58', '59', '60',
             '61', '62', '63', '64', '65', '66', '67', '68', '69', '70',
             '71', '72', '73', '74', '75', '76', '77', '78', '79', '80',
             '81', '82', '83', '84', '85', '86', '87', '88', '89', '90',
             '91', '92', '93',
             '001', '002', '003', '004', '005', '006', '007', '008', '009', '010',
             '011', '012', '013', '014', '015', '016', '017', '018', '019', '020',
             '021', '022', '023', '024', '025', '026', '027', '028', '029', '030',
             '031', '032', '033', '034', '035', '036', '037', '038', '039', '040',
             '041', '042', '043', '044', '045', '046', '047', '048', '049', '050',
             '051', '052', '053', '054', '055', '056', '057', '058', '059', '060',
             '061', '062', '063', '064', '065', '066', '067', '068', '069', '070',
             '071', '072', '073', '074', '075', '076', '077', '078', '079', '080',
             '081', '082', '083', '084', '085', '086', '087', '088', '089', '090',
             '091', '092', '093',
             '115', '119', '120',
             '121', '122', '123', '127',
             '131', '132', '133', '134', '135', '136', '137', '138', '139', '140',
             '141', '142', '143', '144', '145', '146', '147'],
        'MFP_LVS_WTH_FMLY_CD':
            ['0', '1', '2'],
        'MFP_QLFYD_INSTN_CD':
            ['00', '01', '02', '03', '04', '05', '0', '1', '2', '3', '4', '5'],
        'MFP_QLFYD_RSDNC_CD':
            ['00', '01', '02', '03', '04', '05', '0', '1', '2', '3', '4', '5'],
        'MFP_PRTCPTN_ENDD_RSN_CD':
            ['00', '01', '02', '03', '04', '05', '06', '07', '0', '1', '2', '3', '4', '5', '6', '7'],
        'MFP_RINSTLZD_RSN_CD':
            ['00', '01', '02', '03', '04', '05', '06', '07', '08', '0', '1', '2', '3', '4', '5', '6', '7', '8'],
        'STATE_PLAN_OPTN_TYPE_CD':
            ['00', '01', '02', '03', '04', '05', '06', '0', '1', '2', '3', '4', '5', '6'],
        'WVR_TYPE_CD':
            ['1', '2', '3', '4', '5', '6', '7', '8', '9', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
             '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33'],
        'LTSS_LVL_CARE_CD':
            ['1', '2', '3'],
        'MC_PLAN_TYPE_CD':
            ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '00', '01', '02', '03', '04', '05',
             '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '60', '70', '80'],
        'ETHNCTY_CD':
            ['0', '1', '2', '3', '4', '5'],
        'SECT_1115A_DEMO_IND':
            ['0', '1'],
        'TPL_INSRNC_CVRG_IND':
            ['0', '1'],
        'TPL_OTHR_CVRG_IND':
            ['0', '1'],
        'AMRCN_INDN_ALSK_NTV_IND':
            ['0', '1', '2'],
        'AMRCN_INDN_ALSKA_NTV_IND':
            ['0', '1', '2'],
        'ELGBLTY_EXTNSN_CD':
            ['1', '2', '3', '01', '02', '03', '001', '002', '003', '995'],
        'CNTNUS_ELGBLTY_CD':
            ['1', '2', '01', '02', '001', '002', '995'],
        'INCM_STD_CD':
            ['1', '2', '01', '02', '95'],    
    }

    prefrd_lang_cd = [
        'ABK', 'ACE', 'ACH', 'ADA', 'ADY', 'AAR', 'AFH', 'AFR', 'AFA', 'AIN', 'AKA', 'AKK', 'ALB', 'ALB', 'ALE', 'ALG', 'TUT',
        'AMH', 'ANP', 'APA', 'ARA', 'ARG', 'ARP', 'ARW', 'ARM', 'RUP', 'ART', 'ASM', 'AST', 'ATH', 'AUS', 'MAP', 'AVA', 'AVE',
        'AWA', 'AYM', 'AZE', 'BAN', 'BAT', 'BAL', 'BAM', 'BAI', 'BAD', 'BNT', 'BAS', 'BAK', 'BAQ', 'BTK', 'BEJ', 'BEL', 'BEM',
        'BEN', 'BER', 'BHO', 'BIH', 'BIK', 'BIN', 'BIS', 'BOD', 'BYN', 'ZBL', 'NOB', 'BOS', 'BRA', 'BRE', 'BUG', 'BUL', 'BUA', 'BUR',
        'CAD', 'CAT', 'CAU', 'CEB', 'CEL', 'CES', 'CAI', 'KHM', 'CHG', 'CMC', 'CNR', 'CHA', 'CHE', 'CHR', 'CHY', 'CHB', 'NYA', 'CHI', 'CHN',
        'CHP', 'CHO', 'CHU', 'CHK', 'CHV', 'CYM', 'NWC', 'SYC', 'COP', 'COR', 'COS', 'CRE', 'MUS', 'CRP', 'CPE', 'CPF', 'CPP', 'CRH',
        'HRV', 'CUS', 'CZE', 'DAK', 'DAN', 'DAR', 'DEL', 'DEU', 'DIN', 'DIV', 'DOI', 'DGR', 'DRA', 'DUA', 'DUM', 'DUT', 'DYU', 'DZO',
        'FRS', 'EFI', 'EGY', 'EKA', 'ELX', 'ENG', 'ENM', 'ANG', 'MYV', 'ELL', 'EPO', 'EST', 'EUS', 'EWE', 'EWO', 'FAN', 'FAS', 'FAT', 'FAO', 'FIJ',
        'FIL', 'FIN', 'FIU', 'FON', 'FRA', 'FRE', 'FRM', 'FRO', 'FUR', 'FUL', 'GAA', 'GLA', 'CAR', 'GLG', 'LUG', 'GAY', 'GBA', 'GEZ',
        'GEO', 'GER', 'GMH', 'GOH', 'GEM', 'GIL', 'GON', 'GOR', 'GOT', 'GRB', 'GRC', 'GRE', 'GRN', 'GUJ', 'GWI', 'HAI', 'HAT',
        'HAU', 'HAW', 'HEB', 'HER', 'HIL', 'HIM', 'HIN', 'HMO', 'HIT', 'HMN', 'HUN', 'HUP', 'HYE', 'IBA', 'ICE', 'IDO', 'IBO', 'IJO',
        'ILO', 'ISL', 'SMN', 'INC', 'INE', 'IND', 'INH', 'INA', 'ILE', 'IKU', 'IPK', 'IRA', 'GLE', 'MGA', 'SGA', 'IRO', 'ITA', 'JPN',
        'JAV', 'JRB', 'JPR', 'KBD', 'KAB', 'KAC', 'KAL', 'KAT', 'XAL', 'KAM', 'KAN', 'KAU', 'KRC', 'KAA', 'KRL', 'KAR', 'KAS', 'CSB',
        'KAW', 'KAZ', 'KHA', 'KHI', 'KHO', 'KIK', 'KMB', 'KIN', 'KIR', 'TLH', 'KOM', 'KON', 'KOK', 'KOR', 'KOS', 'KPE', 'KRO',
        'KUA', 'KUM', 'KUR', 'HSB', 'KRU', 'KUT', 'LAD', 'LAH', 'LAM', 'DAY', 'LAO', 'LAT', 'LAV', 'LEZ', 'LIM', 'LIN', 'LIT',
        'JBO', 'NDS', 'DSB', 'LOZ', 'LUB', 'LUA', 'LUI', 'SMJ', 'LUN', 'LUO', 'LUS', 'LTZ', 'MAC', 'MAD', 'MAG', 'MAI', 'MAK',
        'MLG', 'MAY', 'MAL', 'MLT', 'MNC', 'MDR', 'MAN', 'MNI', 'MNO', 'GLV', 'MAO', 'ARN', 'MAR', 'CHM', 'MAH', 'MWR', 'MAS',
        'MYN', 'MEN', 'MIC', 'MIN', 'MWL', 'MOH', 'MDF', 'LOL', 'MON', 'MKD', 'MKH', 'MOS', 'MRI', 'MSA', 'MUL', 'MUN', 'MYA', 'NAH', 'NAU', 'NAV', 'NDE',
        'NBL', 'NDO', 'NAP', 'NEW', 'NEP', 'NIA', 'NIC', 'NLD', 'SSA', 'NIU', 'NQO', 'NOG', 'NON', 'NAI', 'FRR', 'SME', 'NOR', 'NNO',
        'NUB', 'NYM', 'NYN', 'NYO', 'NZI', 'OCI', 'ARC', 'OJI', 'ORI', 'ORM', 'OSA', 'OSS', 'OTO', 'PAL', 'PAU', 'PLI', 'PAM',
        'PAG', 'PAN', 'PAP', 'PAA', 'NSO', 'PER', 'PEO', 'PHI', 'PHN', 'PON', 'POL', 'POR', 'PRA', 'PRO', 'PUS', 'QUE', 'RAJ',
        'RAP', 'RAR', 'ROA', 'RUM', 'ROH', 'ROM', 'RON', 'RUN', 'RUS', 'SAL', 'SAM', 'SMI', 'SMO', 'SAD', 'SAG', 'SAN', 'SAT', 'SRD',
        'SAS', 'SCO', 'SEL', 'SEM', 'SRP', 'SRR', 'SHN', 'SNA', 'III', 'SCN', 'SID', 'SGN', 'BLA', 'SND', 'SIN', 'SIT', 'SIO',
        'SMS', 'DEN', 'SLK', 'SLA', 'SLO', 'SLV', 'SOG', 'SOM', 'SON', 'SNK', 'WEN', 'SOT', 'SQI', 'SAI', 'ALT', 'SMA', 'SPA', 'SRN', 'SUK',
        'SUX', 'SUN', 'SUS', 'SWA', 'SSW', 'SWE', 'GSW', 'SYR', 'TGL', 'TAH', 'TAI', 'TGK', 'TMH', 'TAM', 'TAT', 'TEL', 'TER',
        'TET', 'THA', 'TIB', 'TIG', 'TIR', 'TEM', 'TIV', 'TLI', 'TPI', 'TKL', 'TOG', 'TON', 'TSI', 'TSO', 'TSN', 'TUM', 'TUP',
        'TUR', 'OTA', 'TUK', 'TVL', 'TYV', 'TWI', 'UDM', 'UGA', 'UIG', 'UKR', 'UMB', 'URD', 'MIS', 'UND', 'UZB', 'VAI', 'VEN', 'VIE',
        'VOL', 'VOT', 'WAK', 'WLN', 'WAR', 'WAS', 'WEL', 'FRY', 'WAL', 'WOL', 'XHO', 'SAH', 'YAO', 'YAP', 'YID', 'YOR', 'YPK',
        'ZND', 'ZAP', 'ZGH', 'ZHO', 'ZZA', 'ZEN', 'ZHA', 'ZUL', 'ZUN']

    output_columns = [
        'DA_RUN_ID',
        'BSF_FIL_DT',
        'BSF_VRSN',
        'MSIS_IDENT_NUM',
        'SSN_NUM',
        'SSN_IND',
        'SSN_VRFCTN_IND',
        'MDCR_BENE_ID',
        'MDCR_HICN_NUM',
        'TMSIS_RUN_ID',
        'SUBMTG_STATE_CD',
        'REGION',
        'MSIS_CASE_NUM',
        'SNGL_ENRLMT_FLAG',
        'MDCD_ENRLMT_EFF_DT_1',
        'MDCD_ENRLMT_END_DT_1',
        'MDCD_ENRLMT_EFF_DT_2',
        'MDCD_ENRLMT_END_DT_2',
        'MDCD_ENRLMT_EFF_DT_3',
        'MDCD_ENRLMT_END_DT_3',
        'MDCD_ENRLMT_EFF_DT_4',
        'MDCD_ENRLMT_END_DT_4',
        'MDCD_ENRLMT_EFF_DT_5',
        'MDCD_ENRLMT_END_DT_5',
        'MDCD_ENRLMT_EFF_DT_6',
        'MDCD_ENRLMT_END_DT_6',
        'MDCD_ENRLMT_EFF_DT_7',
        'MDCD_ENRLMT_END_DT_7',
        'MDCD_ENRLMT_EFF_DT_8',
        'MDCD_ENRLMT_END_DT_8',
        'MDCD_ENRLMT_EFF_DT_9',
        'MDCD_ENRLMT_END_DT_9',
        'MDCD_ENRLMT_EFF_DT_10',
        'MDCD_ENRLMT_END_DT_10',
        'MDCD_ENRLMT_EFF_DT_11',
        'MDCD_ENRLMT_END_DT_11',
        'MDCD_ENRLMT_EFF_DT_12',
        'MDCD_ENRLMT_END_DT_12',
        'MDCD_ENRLMT_EFF_DT_13',
        'MDCD_ENRLMT_END_DT_13',
        'MDCD_ENRLMT_EFF_DT_14',
        'MDCD_ENRLMT_END_DT_14',
        'MDCD_ENRLMT_EFF_DT_15',
        'MDCD_ENRLMT_END_DT_15',
        'MDCD_ENRLMT_EFF_DT_16',
        'MDCD_ENRLMT_END_DT_16',
        'CHIP_ENRLMT_EFF_DT_1',
        'CHIP_ENRLMT_END_DT_1',
        'CHIP_ENRLMT_EFF_DT_2',
        'CHIP_ENRLMT_END_DT_2',
        'CHIP_ENRLMT_EFF_DT_3',
        'CHIP_ENRLMT_END_DT_3',
        'CHIP_ENRLMT_EFF_DT_4',
        'CHIP_ENRLMT_END_DT_4',
        'CHIP_ENRLMT_EFF_DT_5',
        'CHIP_ENRLMT_END_DT_5',
        'CHIP_ENRLMT_EFF_DT_6',
        'CHIP_ENRLMT_END_DT_6',
        'CHIP_ENRLMT_EFF_DT_7',
        'CHIP_ENRLMT_END_DT_7',
        'CHIP_ENRLMT_EFF_DT_8',
        'CHIP_ENRLMT_END_DT_8',
        'CHIP_ENRLMT_EFF_DT_9',
        'CHIP_ENRLMT_END_DT_9',
        'CHIP_ENRLMT_EFF_DT_10',
        'CHIP_ENRLMT_END_DT_10',
        'CHIP_ENRLMT_EFF_DT_11',
        'CHIP_ENRLMT_END_DT_11',
        'CHIP_ENRLMT_EFF_DT_12',
        'CHIP_ENRLMT_END_DT_12',
        'CHIP_ENRLMT_EFF_DT_13',
        'CHIP_ENRLMT_END_DT_13',
        'CHIP_ENRLMT_EFF_DT_14',
        'CHIP_ENRLMT_END_DT_14',
        'CHIP_ENRLMT_EFF_DT_15',
        'CHIP_ENRLMT_END_DT_15',
        'CHIP_ENRLMT_EFF_DT_16',
        'CHIP_ENRLMT_END_DT_16',
        'ELGBL_1ST_NAME',
        'ELGBL_LAST_NAME',
        'ELGBL_MDL_INITL_NAME',
        'BIRTH_DT',
        'DEATH_DT',
        'cast(AGE_NUM as integer) as AGE_NUM',
        'AGE_GRP_FLAG',
        'DCSD_FLAG',
        'SEX_CD',
        'MRTL_STUS_CD',
        'INCM_CD',
        'VET_IND',
        'CTZNSHP_IND',
        'CTZNSHP_VRFCTN_IND',
        'IMGRTN_STUS_CD',
        'IMGRTN_VRFCTN_IND',
        'IMGRTN_STUS_5_YR_BAR_END_DT',
        'OTHR_LANG_HOME_CD',
        'PRMRY_LANG_FLAG',
        'ENGLSH_PRFCNCY_CD',
        'HSEHLD_SIZE_CD',
        'cast(NULL as string) as PRGNT_IND',
        'cast(NULL as integer) as PRGNCY_FLAG',
        'AMRCN_INDN_ALSKA_NTV_IND as AMRCN_INDN_ALSK_NTV_IND',
        'ETHNCTY_CD',
        'ELGBL_LINE_1_ADR_HOME',
        'ELGBL_LINE_2_ADR_HOME',
        'ELGBL_LINE_3_ADR_HOME',
        'ELGBL_CITY_NAME_HOME',
        'ELGBL_ZIP_CD_HOME',
        'ELGBL_CNTY_CD_HOME',
        'ELGBL_STATE_CD_HOME',
        'ELGBL_PHNE_NUM_HOME',
        'ELGBL_LINE_1_ADR_MAIL',
        'ELGBL_LINE_2_ADR_MAIL',
        'ELGBL_LINE_3_ADR_MAIL',
        'ELGBL_CITY_NAME_MAIL',
        'ELGBL_ZIP_CD_MAIL',
        'ELGBL_CNTY_CD_MAIL',
        'ELGBL_STATE_CD_MAIL',
        'CARE_LVL_STUS_CD',
        'DEAF_DSBL_FLAG',
        'BLND_DSBL_FLAG',
        'DFCLTY_CONC_DSBL_FLAG',
        'DFCLTY_WLKG_DSBL_FLAG',
        'DFCLTY_DRSNG_BATHG_DSBL_FLAG',
        'DFCLTY_ERRANDS_ALN_DSBL_FLAG',
        'OTHR_DSBL_FLAG',
        'HCBS_AGED_NON_HHCC_FLAG',
        'HCBS_PHYS_DSBL_NON_HHCC_FLAG',
        'HCBS_INTEL_DSBL_NON_HHCC_FLAG',
        'HCBS_AUTSM_NON_HHCC_FLAG',
        'HCBS_DD_NON_HHCC_FLAG',
        'HCBS_MI_SED_NON_HHCC_FLAG',
        'HCBS_BRN_INJ_NON_HHCC_FLAG',
        'HCBS_HIV_AIDS_NON_HHCC_FLAG',
        'HCBS_TECH_DEP_MF_NON_HHCC_FLAG',
        'HCBS_DSBL_OTHR_NON_HHCC_FLAG',
        'ENRL_TYPE_FLAG',
        'DAYS_ELIG_IN_MO_CNT',
        'ELGBL_ENTIR_MO_IND',
        'ELGBL_LAST_DAY_OF_MO_IND',
        'CHIP_CD',
        'ELGBLTY_GRP_CD',
        'PRMRY_ELGBLTY_GRP_IND',
        'ELGBLTY_GRP_CTGRY_FLAG',
        'cast(NULL as string) as MAS_CD',
        'cast(NULL as string) as ELGBLTY_MDCD_BASIS_CD',
        'cast(NULL as string) as MASBOE_CD',
        'STATE_SPEC_ELGBLTY_FCTR_TXT',
        'DUAL_ELGBL_CD',
        'DUAL_ELGBL_FLAG',
        'RSTRCTD_BNFTS_CD',
        'SSDI_IND',
        'SSI_IND',
        'SSI_STATE_SPLMT_STUS_CD',
        'SSI_STUS_CD',
        'BIRTH_CNCPTN_IND',
        'TANF_CASH_CD',
        'HH_PGM_PRTCPNT_FLAG',
        'HH_PRVDR_NUM',
        'HH_ENT_NAME',
        'MH_HH_CHRNC_COND_FLAG',
        'SA_HH_CHRNC_COND_FLAG',
        'ASTHMA_HH_CHRNC_COND_FLAG',
        'DBTS_HH_CHRNC_COND_FLAG',
        'HRT_DIS_HH_CHRNC_COND_FLAG',
        'OVRWT_HH_CHRNC_COND_FLAG',
        'HIV_AIDS_HH_CHRNC_COND_FLAG',
        'OTHR_HH_CHRNC_COND_FLAG',
        'LCKIN_PRVDR_NUM1',
        'LCKIN_PRVDR_TYPE_CD1',
        'LCKIN_PRVDR_NUM2',
        'LCKIN_PRVDR_TYPE_CD2',
        'LCKIN_PRVDR_NUM3',
        'LCKIN_PRVDR_TYPE_CD3',
        'LOCK_IN_FLAG',
        'LTSS_PRVDR_NUM1',
        'LTSS_LVL_CARE_CD1',
        'LTSS_PRVDR_NUM2',
        'LTSS_LVL_CARE_CD2',
        'LTSS_PRVDR_NUM3',
        'LTSS_LVL_CARE_CD3',
        'MC_PLAN_ID1',
        'MC_PLAN_TYPE_CD1',
        'MC_PLAN_ID2',
        'MC_PLAN_TYPE_CD2',
        'MC_PLAN_ID3',
        'MC_PLAN_TYPE_CD3',
        'MC_PLAN_ID4',
        'MC_PLAN_TYPE_CD4',
        'MC_PLAN_ID5',
        'MC_PLAN_TYPE_CD5',
        'MC_PLAN_ID6',
        'MC_PLAN_TYPE_CD6',
        'MC_PLAN_ID7',
        'MC_PLAN_TYPE_CD7',
        'MC_PLAN_ID8',
        'MC_PLAN_TYPE_CD8',
        'MC_PLAN_ID9',
        'MC_PLAN_TYPE_CD9',
        'MC_PLAN_ID10',
        'MC_PLAN_TYPE_CD10',
        'MC_PLAN_ID11',
        'MC_PLAN_TYPE_CD11',
        'MC_PLAN_ID12',
        'MC_PLAN_TYPE_CD12',
        'MC_PLAN_ID13',
        'MC_PLAN_TYPE_CD13',
        'MC_PLAN_ID14',
        'MC_PLAN_TYPE_CD14',
        'MC_PLAN_ID15',
        'MC_PLAN_TYPE_CD15',
        'MC_PLAN_ID16',
        'MC_PLAN_TYPE_CD16',
        'MFP_LVS_WTH_FMLY_CD',
        'MFP_QLFYD_INSTN_CD',
        'MFP_QLFYD_RSDNC_CD',
        'MFP_PRTCPTN_ENDD_RSN_CD',
        'MFP_RINSTLZD_RSN_CD',
        'MFP_PRTCPNT_FLAG',
        'CMNTY_1ST_CHS_SPO_FLAG',
        '_1915I_SPO_FLAG',
        '_1915J_SPO_FLAG',
        '_1932A_SPO_FLAG',
        '_1915A_SPO_FLAG',
        '_1937_ABP_SPO_FLAG',
        'cast(_1115A_PRTCPNT_FLAG as integer) as _1115A_PRTCPNT_FLAG',
        'WVR_ID1',
        'WVR_TYPE_CD1',
        'WVR_ID2',
        'WVR_TYPE_CD2',
        'WVR_ID3',
        'WVR_TYPE_CD3',
        'WVR_ID4',
        'WVR_TYPE_CD4',
        'WVR_ID5',
        'WVR_TYPE_CD5',
        'WVR_ID6',
        'WVR_TYPE_CD6',
        'WVR_ID7',
        'WVR_TYPE_CD7',
        'WVR_ID8',
        'WVR_TYPE_CD8',
        'WVR_ID9',
        'WVR_TYPE_CD9',
        'WVR_ID10',
        'WVR_TYPE_CD10',
        'TPL_INSRNC_CVRG_IND',
        'TPL_OTHR_CVRG_IND',
        'SECT_1115A_DEMO_IND',
        'NTV_HI_FLAG',
        'GUAM_CHAMORRO_FLAG',
        'SAMOAN_FLAG',
        'OTHR_PAC_ISLNDR_FLAG',
        'UNK_PAC_ISLNDR_FLAG',
        'ASN_INDN_FLAG',
        'CHINESE_FLAG',
        'FILIPINO_FLAG',
        'JAPANESE_FLAG',
        'KOREAN_FLAG',
        'VIETNAMESE_FLAG',
        'OTHER_ASIAN_FLAG',
        'UNKNOWN_ASIAN_FLAG',
        'WHITE_FLAG',
        'BLACK_AFRICAN_AMERICAN_FLAG',
        'AIAN_FLAG',
        'RACE_ETHNICITY_FLAG',
        'RACE_ETHNCTY_EXP_FLAG',
        'HISPANIC_ETHNICITY_FLAG',
        'REC_ADD_TS',
        'REC_UPDT_TS',
        'ELGBL_ID_ADDTNL',
        'ELGBL_ID_ADDTNL_ENT_ID',
        'ELGBL_ID_ADDTNL_RSN_CHG',
        'ELGBL_ID_MSIS_XWALK',
        'ELGBL_ID_MSIS_XWALK_ENT_ID',
        'ELGBL_ID_MSIS_XWALK_RSN_CHG',
        'ELGBLTY_TRMNTN_RSN_CD',
        'ELGBL_AFTR_EOM_IND',
        'LCKIN_SRVC1',
        'LCKIN_SRVC2',
        'LCKIN_SRVC3',
        'FED_PVT_LVL',
        'ELGBLTY_EXTNSN_CD',
        'CNTNUS_ELGBLTY_CD',
        'INCM_STD_CD',
        'ELGBLTY_RDTRMNTN_DT',
    ]

# -----------------------------------------------------------------------------
# CC0 1.0 Universal

# Statement of Purpose

# The laws of most jurisdictions throughout the world automatically confer
# exclusive Copyright and Related Rights (defined below) upon the creator and
# subsequent owner(s) (each and all, an "owner") of an original work of
# authorship and/or a database (each, a "Work").

# Certain owners wish to permanently relinquish those rights to a Work for the
# purpose of contributing to a commons of creative, cultural and scientific
# works ("Commons") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Commons to promote the ideal of a free culture and the
# further production of creative, cultural and scientific works, or to gain
# reputation or greater distribution for their Work in part through the use and
# efforts of others.

# For these and/or other purposes and motivations, and without any expectation
# of additional consideration or compensation, the person associating CC0 with a
# Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
# and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
# and publicly distribute the Work under its terms, with knowledge of his or her
# Copyright and Related Rights in the Work and the meaning and intended legal
# effect of CC0 on those rights.

# 1. Copyright and Related Rights. A Work made available under CC0 may be
# protected by copyright and related or neighboring rights ("Copyright and
# Related Rights"). Copyright and Related Rights include, but are not limited
# to, the following:

#   i. the right to reproduce, adapt, distribute, perform, display, communicate,
#   and translate a Work

#   ii. moral rights retained by the original author(s) and/or performer(s)

#   iii. publicity and privacy rights pertaining to a person's image or likeness
#   depicted in a Work

#   iv. rights protecting against unfair competition in regards to a Work,
#   subject to the limitations in paragraph 4(a), below

#   v. rights protecting the extraction, dissemination, use and reuse of data in
#   a Work

#   vi. database rights (such as those arising under Directive 96/9/EC of the
#   European Parliament and of the Council of 11 March 1996 on the legal
#   protection of databases, and under any national implementation thereof,
#   including any amended or successor version of such directive) and

#   vii. other similar, equivalent or corresponding rights throughout the world
#   based on applicable law or treaty, and any national implementations thereof.

# 2. Waiver. To the greatest extent permitted by, but not in contravention of,
# applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
# unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
# and Related Rights and associated claims and causes of action, whether now
# known or unknown (including existing as well as future claims and causes of
# action), in the Work (i) in all territories worldwide, (ii) for the maximum
# duration provided by applicable law or treaty (including future time
# extensions), (iii) in any current or future medium and for any number of
# copies, and (iv) for any purpose whatsoever, including without limitation
# commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
# the Waiver for the benefit of each member of the public at large and to the
# detriment of Affirmer's heirs and successors, fully intending that such Waiver
# shall not be subject to revocation, rescission, cancellation, termination, or
# any other legal or equitable action to disrupt the quiet enjoyment of the Work
# by the public as contemplated by Affirmer's express Statement of Purpose.

# 3. Public License Fallback. Should any part of the Waiver for any reason be
# judged legally invalid or ineffective under applicable law, then the Waiver
# shall be preserved to the maximum extent permitted taking into account
# Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
# is so judged Affirmer hereby grants to each affected person a royalty-free,
# non transferable, non sublicensable, non exclusive, irrevocable and
# unconditional license to exercise Affirmer's Copyright and Related Rights in
# the Work (i) in all territories worldwide, (ii) for the maximum duration
# provided by applicable law or treaty (including future time extensions), (iii)
# in any current or future medium and for any number of copies, and (iv) for any
# purpose whatsoever, including without limitation commercial, advertising or
# promotional purposes (the "License"). The License shall be deemed effective as
# of the date CC0 was applied by Affirmer to the Work. Should any part of the
# License for any reason be judged legally invalid or ineffective under
# applicable law, such partial invalidity or ineffectiveness shall not
# invalidate the remainder of the License, and in such case Affirmer hereby
# affirms that he or she will not (i) exercise any of his or her remaining
# Copyright and Related Rights in the Work or (ii) assert any associated claims
# and causes of action with respect to the Work, in either case contrary to
# Affirmer's express Statement of Purpose.

# 4. Limitations and Disclaimers.

#   a. No trademark or patent rights held by Affirmer are waived, abandoned,
#   surrendered, licensed or otherwise affected by this document.

#   b. Affirmer offers the Work as-is and makes no representations or warranties
#   of any kind concerning the Work, express, implied, statutory or otherwise,
#   including without limitation warranties of title, merchantability, fitness
#   for a particular purpose, non infringement, or the absence of latent or
#   other defects, accuracy, or the present or absence of errors, whether or not
#   discoverable, all to the greatest extent permissible under applicable law.

#   c. Affirmer disclaims responsibility for clearing rights of other persons
#   that may apply to the Work or any use thereof, including without limitation
#   any person's Copyright and Related Rights in the Work. Further, Affirmer
#   disclaims responsibility for obtaining any necessary consents, permissions
#   or other rights required for any use of the Work.

#   d. Affirmer understands and acknowledges that Creative Commons is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecommons.org/publicdomain/zero/1.0/>
