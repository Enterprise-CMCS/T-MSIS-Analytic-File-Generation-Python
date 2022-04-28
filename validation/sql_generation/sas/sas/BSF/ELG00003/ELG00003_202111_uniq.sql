create temp table ELG00003_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    comb.*,
    preg.PREGNANCY_FLAG as PREGNANCY_FLG
from
    (
        select
            *
        from
            ELG00003_uniq
        union
        all
        select
            *
        from
            ELG00003_multi
    ) as comb
    left join (
        select
            SUBMTG_STATE_CD,
            MSIS_IDENT_NUM,
            max(
                case
                    when trim(PRGNT_IND) in('0', '1') then cast(trim(PRGNT_IND) as integer)
                    else null
                end
            ) as PREGNANCY_FLAG
        from
            ELG00003A
        group by
            SUBMTG_STATE_CD,
            MSIS_IDENT_NUM
    ) preg on comb.SUBMTG_STATE_CD = preg.SUBMTG_STATE_CD
    and comb.msis_ident_num = preg.msis_ident_num