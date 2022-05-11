create
or replace temporary view ELG00016_step3 as
select
    submtg_state_cd,
    msis_ident_num,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '012' then 1
            else 0
        end
    ) as NATIVE_HI_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '013' then 1
            else 0
        end
    ) as GUAM_CHAMORRO_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '014' then 1
            else 0
        end
    ) as SAMOAN_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '015' then 1
            else 0
        end
    ) as OTHER_PAC_ISLANDER_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '016' then 1
            else 0
        end
    ) as UNK_PAC_ISLANDER_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '004' then 1
            else 0
        end
    ) as ASIAN_INDIAN_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '005' then 1
            else 0
        end
    ) as CHINESE_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '006' then 1
            else 0
        end
    ) as FILIPINO_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '007' then 1
            else 0
        end
    ) as JAPANESE_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '008' then 1
            else 0
        end
    ) as KOREAN_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '009' then 1
            else 0
        end
    ) as VIETNAMESE_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '010' then 1
            else 0
        end
    ) as OTHER_ASIAN_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '011' then 1
            else 0
        end
    ) as UNKNOWN_ASIAN_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '001' then 1
            else 0
        end
    ) as WHITE_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '002' then 1
            else 0
        end
    ) as BLACK_AFRICAN_AMERICAN_FLG,
    max(
        case
            when nullif(trim(RACE_CD), '') is null then null
            when trim(RACE_CD) = '003' then 1
            else 0
        end
    ) as AIAN_FLG
from
    ELG00016
group by
    submtg_state_cd,
    msis_ident_num