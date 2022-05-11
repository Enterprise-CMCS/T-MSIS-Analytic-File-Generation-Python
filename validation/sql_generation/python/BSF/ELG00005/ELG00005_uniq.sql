create
or replace temporary view ELG00005_uniq as
select
    t1.*,
    case
        when length(trim(ELGBLTY_GRP_CD)) = 1
        and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
        else trim(ELGBLTY_GRP_CD)
    end as ELGBLTY_GRP_CODE,
    case
        when length(trim(DUAL_ELGBL_CD)) = 1
        and DUAL_ELGBL_CD <> '.' then lpad(trim(DUAL_ELGBL_CD), 2, '0')
        else trim(DUAL_ELGBL_CD)
    end as DUAL_ELGBL_CODE,
    case
        when LENGTH(trim(CARE_LVL_STUS_CD)) < 3
        and CARE_LVL_STUS_CD <> '.' then lpad(CARE_LVL_STUS_CD, 3, '00')
        else CARE_LVL_STUS_CD
    end as CARE_LVL_STUS_CODE,
    case
        when (
            case
                when length(trim(DUAL_ELGBL_CD)) = 1
                and DUAL_ELGBL_CD <> '.' then lpad(trim(DUAL_ELGBL_CD), 2, '0')
                else trim(DUAL_ELGBL_CD)
            end
        ) in ('02', '04', '08') then 1
        when (
            case
                when length(trim(DUAL_ELGBL_CD)) = 1
                and DUAL_ELGBL_CD <> '.' then lpad(trim(DUAL_ELGBL_CD), 2, '0')
                else trim(DUAL_ELGBL_CD)
            end
        ) in ('01', '03', '05', '06') then 2
        when (
            case
                when length(trim(DUAL_ELGBL_CD)) = 1
                and DUAL_ELGBL_CD <> '.' then lpad(trim(DUAL_ELGBL_CD), 2, '0')
                else trim(DUAL_ELGBL_CD)
            end
        ) in ('09', '10') then 3
        when (
            case
                when length(trim(DUAL_ELGBL_CD)) = 1
                and DUAL_ELGBL_CD <> '.' then lpad(trim(DUAL_ELGBL_CD), 2, '0')
                else trim(DUAL_ELGBL_CD)
            end
        ) in ('00') then 4
        else null
    end as DUAL_ELIGIBLE_FLG,
    case
        when (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) between '01'
            and '09'
        )
        or (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) between '72'
            and '75'
        ) then 1
        when (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) between '11'
            and '19'
        )
        or (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) between '20'
            and '26'
        ) then 2
        when (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) between '27'
            and '29'
        )
        or (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) between '30'
            and '36'
        )
        or (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) = '76'
        ) then 3
        when (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) between '37'
            and '39'
        )
        or (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) between '40'
            and '49'
        )
        or (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) between '50'
            and '52'
        ) then 4
        when (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) between '53'
            and '56'
        ) then 5
        when (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) in('59', '60')
        ) then 6
        when (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) in('61', '62', '63')
        ) then 7
        when (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) in('64', '65', '66')
        ) then 8
        when (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) in('67', '68')
        ) then 9
        when (
            (
                case
                    when length(trim(ELGBLTY_GRP_CD)) = 1
                    and ELGBLTY_GRP_CD <> '.' then lpad(trim(ELGBLTY_GRP_CD), 2, '0')
                    else trim(ELGBLTY_GRP_CD)
                end
            ) in('69', '70', '71')
        ) then 10
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
    inner join ELG00005_recCt as t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt = 1
where
    PRMRY_ELGBLTY_GRP_IND = '1'