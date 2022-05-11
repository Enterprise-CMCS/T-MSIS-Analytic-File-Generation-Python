create table Prov06_Taxonomies_CCD2 diststyle key distkey(submitting_state_prov_id) as
select
    B.*,
    case
        when B.PRVDR_CLSFCTN_TYPE_CD = '1' then B.PRVDR_CLSFCTN_CD_TAX
        when B.PRVDR_CLSFCTN_TYPE_CD = '2' then SPC.label
        when B.PRVDR_CLSFCTN_TYPE_CD = '3' then TYP.label
        when B.PRVDR_CLSFCTN_TYPE_CD = '4' then SRV.label
    end :: varchar(30) as PRVDR_CLSFCTN_CD
from
    #Prov06_Taxonomies_CCD B left join (select * from prv_formats_sm where fmtname='CLSPRSPC') SPC on B.PRVDR_CLSFCTN_CD_SPC=SPC.start left join (select * 
from
    prv_formats_sm
where
    fmtname = 'CLSPRTYP'
) TYP on B.PRVDR_CLSFCTN_CD_PTYP = TYP.start
left join (
    select
        *
    from
        prv_formats_sm
    where
        fmtname = 'CLSSRVCD'
) SRV on B.PRVDR_CLSFCTN_CD_CSRV = SRV.start
order by
    tms_run_id,
    submitting_state,
    submitting_state_prov_id;