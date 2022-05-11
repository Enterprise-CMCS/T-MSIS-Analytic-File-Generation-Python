create temp table ELG00011_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    submtg_state_cd,
    msis_ident_num,
    max(
        case
            when STATE_PLAN_OPTN_TYPE_CD = '.' then null
            when lpad(trim(STATE_PLAN_OPTN_TYPE_CD), 2, '0') in('01') then 1
            else 0
        end
    ) as COMMUNITY_FIRST_CHOICE_SPO_FLG,
    max(
        case
            when STATE_PLAN_OPTN_TYPE_CD = '.' then null
            when lpad(trim(STATE_PLAN_OPTN_TYPE_CD), 2, '0') in('02') then 1
            else 0
        end
    ) as _1915I_SPO_FLG,
    max(
        case
            when STATE_PLAN_OPTN_TYPE_CD = '.' then null
            when lpad(trim(STATE_PLAN_OPTN_TYPE_CD), 2, '0') in('03') then 1
            else 0
        end
    ) as _1915J_SPO_FLG,
    max(
        case
            when STATE_PLAN_OPTN_TYPE_CD = '.' then null
            when lpad(trim(STATE_PLAN_OPTN_TYPE_CD), 2, '0') in('04') then 1
            else 0
        end
    ) as _1932A_SPO_FLG,
    max(
        case
            when STATE_PLAN_OPTN_TYPE_CD = '.' then null
            when lpad(trim(STATE_PLAN_OPTN_TYPE_CD), 2, '0') in('05') then 1
            else 0
        end
    ) as _1915A_SPO_FLG,
    max(
        case
            when STATE_PLAN_OPTN_TYPE_CD = '.' then null
            when lpad(trim(STATE_PLAN_OPTN_TYPE_CD), 2, '0') in('06') then 1
            else 0
        end
    ) as _1937_ABP_SPO_FLG
from
    ELG00011
group by
    submtg_state_cd,
    msis_ident_num