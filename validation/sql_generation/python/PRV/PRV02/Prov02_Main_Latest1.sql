create
or replace temporary view Prov02_Main_Latest1 as
select
    T.*,
    R.SPCL
from
    Prov_Attributes_Main T
    inner join Prov01_Header R on T.tms_run_id = R.tms_run_id
    and T.submitting_state = R.submitting_state
order by
    T.tms_run_id,
    T.submitting_state
