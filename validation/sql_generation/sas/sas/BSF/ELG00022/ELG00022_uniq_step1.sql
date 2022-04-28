create temp table ELG00022_uniq_step1 distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    *
from
    ELG00022_ADDTNL_uniq
union
all
select
    *
from
    ELG00022_ADDTNL_multi