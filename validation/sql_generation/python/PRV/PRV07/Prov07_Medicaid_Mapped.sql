create
or replace temporary view Prov07_Medicaid_Mapped as
select
    [ 'tms_run_id',
    'submitting_state',
    'submitting_state_prov_id' ],
    1 as PRVDR_MDCD_ENRLMT_IND,
    max(MDCD_ENRLMT_IND) as MDCD_ENRLMT_IND,
    max(CHIP_ENRLMT_IND) as CHIP_ENRLMT_IND,
    max(MDCD_CHIP_ENRLMT_IND) as MDCD_CHIP_ENRLMT_IND,
    max(NOT_SP_AFLTD_IND) as NOT_SP_AFLTD_IND,
    max(PRVDR_ENRLMT_STUS_ACTV_IND) as PRVDR_ENRLMT_STUS_ACTV_IND,
    max(PRVDR_ENRLMT_STUS_DND_IND) as PRVDR_ENRLMT_STUS_DND_IND,
    max(PRVDR_ENRLMT_STUS_TRMNTD_IND) as PRVDR_ENRLMT_STUS_TRMNTD_IND,
    max(PRVDR_ENRLMT_STUS_PENDG_IND) as PRVDR_ENRLMT_STUS_PENDG_IND
from
    Prov07_Medicaid_CNST
group by
    [ 'tms_run_id',
    'submitting_state',
    'submitting_state_prov_id' ]
