create temp table ELG00015_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    (
        select
            *
        from
            ELG00015_uniq
        union
        all
        select
            *
        from
            ELG00015_multi
    )