create temp table ELG00006_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    (
        select
            *
        from
            ELG00006_uniq
        union
        all
        select
            *
        from
            ELG00006_multi
    )