create
or replace temporary view PRV02_Active as
select
    submitting_state,
    count(*) as cnt_active
from
    Prov02_Main_Copy
group by
    submitting_state
order by
    submitting_state
