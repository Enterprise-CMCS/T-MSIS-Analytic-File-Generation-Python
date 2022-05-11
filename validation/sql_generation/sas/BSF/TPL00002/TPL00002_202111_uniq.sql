create temp table TPL00002_202111_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    (
        select
            *
        from
            TPL00002_uniq
        union
        all
        select
            *
        from
            TPL00002_multi
    )