create
or replace temporary view ELG00003_v as
select
    t.*,
    case
        when v.LANG_CD is not null then v.LANG_CD
        else null
    end as PRMRY_LANG_CODE
from
    ELG00003 t
    left join prmry_lang_cd v on v.LANG_CD = t.PRMRY_LANG_CD