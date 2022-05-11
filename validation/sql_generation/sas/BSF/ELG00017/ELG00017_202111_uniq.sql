create temp table ELG00017_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    submtg_state_cd,
    msis_ident_num,
    max(
        case
            when nullif(trim(DSBLTY_TYPE_CD), '') is null then null
            when trim(DSBLTY_TYPE_CD) = '01' then 1
            else 0
        end
    ) as DEAF_DISAB_FLG,
    max(
        case
            when nullif(trim(DSBLTY_TYPE_CD), '') is null then null
            when trim(DSBLTY_TYPE_CD) = '02' then 1
            else 0
        end
    ) as BLIND_DISAB_FLG,
    max(
        case
            when nullif(trim(DSBLTY_TYPE_CD), '') is null then null
            when trim(DSBLTY_TYPE_CD) = '03' then 1
            else 0
        end
    ) as DIFF_CONC_DISAB_FLG,
    max(
        case
            when nullif(trim(DSBLTY_TYPE_CD), '') is null then null
            when trim(DSBLTY_TYPE_CD) = '04' then 1
            else 0
        end
    ) as DIFF_WALKING_DISAB_FLG,
    max(
        case
            when nullif(trim(DSBLTY_TYPE_CD), '') is null then null
            when trim(DSBLTY_TYPE_CD) = '05' then 1
            else 0
        end
    ) as DIFF_DRESSING_BATHING_DISAB_FLG,
    max(
        case
            when nullif(trim(DSBLTY_TYPE_CD), '') is null then null
            when trim(DSBLTY_TYPE_CD) = '06' then 1
            else 0
        end
    ) as DIFF_ERRANDS_ALONE_DISAB_FLG,
    max(
        case
            when nullif(trim(DSBLTY_TYPE_CD), '') is null then null
            when trim(DSBLTY_TYPE_CD) = '07' then 1
            else 0
        end
    ) as OTHER_DISAB_FLG
from
    ELG00017
group by
    submtg_state_cd,
    msis_ident_num