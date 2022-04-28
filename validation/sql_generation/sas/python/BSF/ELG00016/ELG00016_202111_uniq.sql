create
or replace temporary view ELG00016_202111_uniq as
select
    t1.*,
    t2.CRTFD_AMRCN_INDN_ALSKN_NTV_IND,
    case
        when (
            coalesce(ASIAN_INDIAN_FLG, 0) + coalesce(CHINESE_FLG, 0) + coalesce(FILIPINO_FLG, 0) + coalesce(JAPANESE_FLG, 0) + coalesce(KOREAN_FLG, 0) + coalesce(VIETNAMESE_FLG, 0) + coalesce(OTHER_ASIAN_FLG, 0) + coalesce(UNKNOWN_ASIAN_FLG, 0)
        ) >= 1 then 1
        else 0
    end as GLOBAL_ASIAN,
    case
        when (
            coalesce(ASIAN_INDIAN_FLG, 0) + coalesce(CHINESE_FLG, 0) + coalesce(FILIPINO_FLG, 0) + coalesce(JAPANESE_FLG, 0) + coalesce(KOREAN_FLG, 0) + coalesce(VIETNAMESE_FLG, 0) + coalesce(OTHER_ASIAN_FLG, 0) + coalesce(UNKNOWN_ASIAN_FLG, 0)
        ) > 1 then 1
        else 0
    end as MULTI_ASIAN,
    case
        when (
            coalesce(NATIVE_HI_FLG, 0) + coalesce(GUAM_CHAMORRO_FLG, 0) + coalesce(SAMOAN_FLG, 0) + coalesce(OTHER_PAC_ISLANDER_FLG, 0) + coalesce(UNK_PAC_ISLANDER_FLG, 0)
        ) >= 1 then 1
        else 0
    end as GLOBAL_ISLANDER,
    case
        when (
            coalesce(NATIVE_HI_FLG, 0) + coalesce(GUAM_CHAMORRO_FLG, 0) + coalesce(SAMOAN_FLG, 0) + coalesce(OTHER_PAC_ISLANDER_FLG, 0) + coalesce(UNK_PAC_ISLANDER_FLG, 0)
        ) > 1 then 1
        else 0
    end as MULTI_ISLANDER
from
    ELG00016_step3 t1
    left join ELG00016_step2 t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num