create
or replace temporary view PRV02_Date as
select
    submitting_state,
    count(*) as cnt_date
from
    Prov02_Main_Latest2
group by
    submitting_state
order by
    submitting_state
