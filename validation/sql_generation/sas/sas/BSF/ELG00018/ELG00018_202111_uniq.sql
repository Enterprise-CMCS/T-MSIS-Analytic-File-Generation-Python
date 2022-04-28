create temp table ELG00018_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    (
        select
            *
        from
            ELG00018_uniq
        union
        all
        select
            *
        from
            ELG00018_multi
    )