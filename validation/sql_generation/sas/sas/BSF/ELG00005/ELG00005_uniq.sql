create temp table ELG00005_uniq distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    t1.*,
    lpad(trim(ELGBLTY_GRP_CD), 2, '0') as ELGBLTY_GRP_CODE,
    lpad(trim(DUAL_ELGBL_CD), 2, '0') as DUAL_ELGBL_CODE,
    lpad(trim(CARE_LVL_STUS_CD), 3, '0') as CARE_LVL_STUS_CODE,
    case
        when DUAL_ELGBL_CODE in('02', '04', '08') then 1
        when DUAL_ELGBL_CODE in('01', '03', '05', '06') then 2
        when DUAL_ELGBL_CODE in('09', '10') then 3
        when DUAL_ELGBL_CODE in('00') then 4
        else null
    end as DUAL_ELIGIBLE_FLG,
    case
        when (
            ELGBLTY_GRP_CODE between '01'
            and '09'
        )
        or (
            ELGBLTY_GRP_CODE between '72'
            and '75'
        ) then 1
        when (
            ELGBLTY_GRP_CODE between '11'
            and '19'
        )
        or (
            ELGBLTY_GRP_CODE between '20'
            and '26'
        ) then 2
        when (
            ELGBLTY_GRP_CODE between '27'
            and '29'
        )
        or (
            ELGBLTY_GRP_CODE between '30'
            and '36'
        )
        or (ELGBLTY_GRP_CODE = '76') then 3
        when (
            ELGBLTY_GRP_CODE between '37'
            and '39'
        )
        or (
            ELGBLTY_GRP_CODE between '40'
            and '49'
        )
        or (
            ELGBLTY_GRP_CODE between '50'
            and '52'
        ) then 4
        when (
            ELGBLTY_GRP_CODE between '53'
            and '56'
        ) then 5
        when (ELGBLTY_GRP_CODE in('59', '60')) then 6
        when (ELGBLTY_GRP_CODE in('61', '62', '63')) then 7
        when (ELGBLTY_GRP_CODE in('64', '65', '66')) then 8
        when (ELGBLTY_GRP_CODE in('67', '68')) then 9
        when (ELGBLTY_GRP_CODE in('69', '70', '71')) then 10
        else null
    end as ELIGIBILITY_GROUP_CATEGORY_FLG,
    case
        when MAS_CD = '.'
        or ELGBLTY_MDCD_BASIS_CD = '.' then '.'
        else (MAS_CD || ELGBLTY_MDCD_BASIS_CD)
    end as MASBOE,
    1 as KEEP_FLAG
from
    ELG00005 t1
    inner join ELG00005_recCt t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt = 1
where
    PRMRY_ELGBLTY_GRP_IND = '1'