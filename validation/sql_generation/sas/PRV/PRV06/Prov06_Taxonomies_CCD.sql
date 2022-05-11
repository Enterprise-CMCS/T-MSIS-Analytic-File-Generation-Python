create table Prov06_Taxonomies_CCD diststyle key distkey(submitting_state_prov_id) as
select
    *,
    case
        when PRVDR_CLSFCTN_TYPE_CD = '1' then prov_classification_code
    end :: varchar(30) as PRVDR_CLSFCTN_CD_TAX,
    case
        when PRVDR_CLSFCTN_TYPE_CD = '2' then trim(prov_classification_code)
    end :: varchar(30) as PRVDR_CLSFCTN_CD_SPC,
    case
        when PRVDR_CLSFCTN_TYPE_CD = '3' then trim(prov_classification_code)
    end :: varchar(30) as PRVDR_CLSFCTN_CD_PTYP,
    case
        when PRVDR_CLSFCTN_TYPE_CD = '4' then trim(prov_classification_code)
    end :: varchar(30) as PRVDR_CLSFCTN_CD_CSRV
from
    #Prov06_Taxonomies_TYP where PRVDR_CLSFCTN_TYPE_CD is not null order by tms_run_id, submitting_state, submitting_state_prov_id;