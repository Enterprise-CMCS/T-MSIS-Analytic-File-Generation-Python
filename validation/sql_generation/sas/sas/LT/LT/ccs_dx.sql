create temp table ccs_dx distkey (icd_10_cm_cd) sortkey (icd_10_cm_cd) as
select
    icd_10_cm_cd,
    dflt_ccsr_ctgry_ip,
    dflt_ccsr_ctgry_ip as dflt_ccsr_ctgry_lt,
    dflt_ccsr_ctgry_op as dflt_ccsr_ctgry_ot
from
    data_anltcs_dm_prod.dxccsr_rfrnc