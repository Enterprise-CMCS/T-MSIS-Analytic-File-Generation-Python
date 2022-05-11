create temp table ELG00008_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    submtg_state_cd,
    msis_ident_num,
    max(
        case
            when trim(HH_CHRNC_CD) in('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H') then 1
            else 0
        end
    ) as ANY_VALID_HH_CC,
    max(
        case
            when nullif(trim(HH_CHRNC_CD), '') is null then null
            when trim(HH_CHRNC_CD) = 'A' then 1
            else 0
        end
    ) as MH_HH_CHRONIC_COND_FLG,
    max(
        case
            when nullif(trim(HH_CHRNC_CD), '') is null then null
            when trim(HH_CHRNC_CD) = 'B' then 1
            else 0
        end
    ) as SA_HH_CHRONIC_COND_FLG,
    max(
        case
            when nullif(trim(HH_CHRNC_CD), '') is null then null
            when trim(HH_CHRNC_CD) = 'C' then 1
            else 0
        end
    ) as ASTHMA_HH_CHRONIC_COND_FLG,
    max(
        case
            when nullif(trim(HH_CHRNC_CD), '') is null then null
            when trim(HH_CHRNC_CD) = 'D' then 1
            else 0
        end
    ) as DIABETES_HH_CHRONIC_COND_FLG,
    max(
        case
            when nullif(trim(HH_CHRNC_CD), '') is null then null
            when trim(HH_CHRNC_CD) = 'E' then 1
            else 0
        end
    ) as HEART_DIS_HH_CHRONIC_COND_FLG,
    max(
        case
            when nullif(trim(HH_CHRNC_CD), '') is null then null
            when trim(HH_CHRNC_CD) = 'F' then 1
            else 0
        end
    ) as OVERWEIGHT_HH_CHRONIC_COND_FLG,
    max(
        case
            when nullif(trim(HH_CHRNC_CD), '') is null then null
            when trim(HH_CHRNC_CD) = 'G' then 1
            else 0
        end
    ) as HIV_AIDS_HH_CHRONIC_COND_FLG,
    max(
        case
            when nullif(trim(HH_CHRNC_CD), '') is null then null
            when trim(HH_CHRNC_CD) = 'H' then 1
            else 0
        end
    ) as OTHER_HH_CHRONIC_COND_FLG
from
    ELG00008
group by
    submtg_state_cd,
    msis_ident_num