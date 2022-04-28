create temp table ELG00004_uniq_step1 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    ELG00004_uniq
union
all
select
    *
from
    ELG00004_multi