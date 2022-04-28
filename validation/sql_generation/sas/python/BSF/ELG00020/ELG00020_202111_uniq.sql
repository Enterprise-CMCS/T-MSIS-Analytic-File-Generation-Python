create
or replace temporary view ELG00020_202111_uniq as
select
    submtg_state_cd,
    msis_ident_num,
    max(
        case
            when nullif(NDC_UOM_CHRNC_NON_HH_CD, '') is null then null
            when NDC_UOM_CHRNC_NON_HH_CD = '001' then 1
            else 0
        end
    ) as HCBS_AGED_NON_HHCC_FLG,
    max(
        case
            when nullif(NDC_UOM_CHRNC_NON_HH_CD, '') is null then null
            when NDC_UOM_CHRNC_NON_HH_CD = '002' then 1
            else 0
        end
    ) as HCBS_PHYS_DISAB_NON_HHCC_FLG,
    max(
        case
            when nullif(NDC_UOM_CHRNC_NON_HH_CD, '') is null then null
            when NDC_UOM_CHRNC_NON_HH_CD = '003' then 1
            else 0
        end
    ) as HCBS_INTEL_DISAB_NON_HHCC_FLG,
    max(
        case
            when nullif(NDC_UOM_CHRNC_NON_HH_CD, '') is null then null
            when NDC_UOM_CHRNC_NON_HH_CD = '004' then 1
            else 0
        end
    ) as HCBS_AUTISM_SP_DIS_NON_HHCC_FLG,
    max(
        case
            when nullif(NDC_UOM_CHRNC_NON_HH_CD, '') is null then null
            when NDC_UOM_CHRNC_NON_HH_CD = '005' then 1
            else 0
        end
    ) as HCBS_DD_NON_HHCC_FLG,
    max(
        case
            when nullif(NDC_UOM_CHRNC_NON_HH_CD, '') is null then null
            when NDC_UOM_CHRNC_NON_HH_CD = '006' then 1
            else 0
        end
    ) as HCBS_MI_SED_NON_HHCC_FLG,
    max(
        case
            when nullif(NDC_UOM_CHRNC_NON_HH_CD, '') is null then null
            when NDC_UOM_CHRNC_NON_HH_CD = '007' then 1
            else 0
        end
    ) as HCBS_BRAIN_INJ_NON_HHCC_FLG,
    max(
        case
            when nullif(NDC_UOM_CHRNC_NON_HH_CD, '') is null then null
            when NDC_UOM_CHRNC_NON_HH_CD = '008' then 1
            else 0
        end
    ) as HCBS_HIV_AIDS_NON_HHCC_FLG,
    max(
        case
            when nullif(NDC_UOM_CHRNC_NON_HH_CD, '') is null then null
            when NDC_UOM_CHRNC_NON_HH_CD = '009' then 1
            else 0
        end
    ) as HCBS_TECH_DEP_MF_NON_HHCC_FLG,
    max(
        case
            when nullif(NDC_UOM_CHRNC_NON_HH_CD, '') is null then null
            when NDC_UOM_CHRNC_NON_HH_CD = '010' then 1
            else 0
        end
    ) as HCBS_DISAB_OTHER_NON_HHCC_FLG
from
    ELG00020
group by
    submtg_state_cd,
    msis_ident_num