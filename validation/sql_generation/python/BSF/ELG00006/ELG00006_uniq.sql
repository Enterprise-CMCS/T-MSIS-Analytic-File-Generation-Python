create
or replace temporary view ELG00006_uniq as
select
    t1.*,
    case
        when nullif(trim(HH_ENT_NAME), '') is null
        and nullif(trim(HH_SNTRN_NAME), '') is null then 2
        else 1
    end as HH_PROGRAM_PARTICIPANT_FLG,
    1 as KEEP_FLAG
from
    ELG00006 t1
    inner join ELG00006_recCt as t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt = 1