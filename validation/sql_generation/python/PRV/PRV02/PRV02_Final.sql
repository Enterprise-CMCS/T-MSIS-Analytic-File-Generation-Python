create
or replace temporary view PRV02_Final as
select
    submitting_state,
    count(*) as cnt_final
from
    Prov02_Main
group by
    submitting_state
order by
    submitting_state
