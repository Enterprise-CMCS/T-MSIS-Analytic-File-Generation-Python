create
or replace temporary view PRV02_Latest as
select
    submitting_state,
    count(*) as cnt_latest
from
    Prov02_Main_Latest1
group by
    submitting_state
order by
    submitting_state
