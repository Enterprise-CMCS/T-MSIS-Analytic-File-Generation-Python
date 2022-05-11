create
or replace temporary view ELG00014_step1 as
select
    distinct submtg_state_cd,
    msis_ident_num,
    rec_num,
    MC_PLAN_ENRLMT_EFCTV_DT,
    MC_PLAN_ENRLMT_END_DT,
    tmsis_rptg_prd,
    case
        when trim(mc_plan_id) in (
            '0',
            '00',
            '000',
            '0000',
            '00000',
            '000000',
            '0000000',
            '00000000',
            '000000000',
            '0000000000',
            '00000000000',
            '000000000000',
            '8',
            '88',
            '888',
            '8888',
            '88888',
            '888888',
            '8888888',
            '88888888',
            '888888888',
            '8888888888',
            '88888888888',
            '888888888888',
            '9',
            '99',
            '999',
            '9999',
            '99999',
            '999999',
            '9999999',
            '99999999',
            '999999999',
            '9999999999',
            '99999999999',
            '999999999999',
            ''
        ) then null
        else trim(mc_plan_id)
    end as MC_PLAN_IDENTIFIER,
    case
        when (
            case
                when trim(mc_plan_id) in (
                    '0',
                    '00',
                    '000',
                    '0000',
                    '00000',
                    '000000',
                    '0000000',
                    '00000000',
                    '000000000',
                    '0000000000',
                    '00000000000',
                    '000000000000',
                    '8',
                    '88',
                    '888',
                    '8888',
                    '88888',
                    '888888',
                    '8888888',
                    '88888888',
                    '888888888',
                    '8888888888',
                    '88888888888',
                    '888888888888',
                    '9',
                    '99',
                    '999',
                    '9999',
                    '99999',
                    '999999',
                    '9999999',
                    '99999999',
                    '999999999',
                    '9999999999',
                    '99999999999',
                    '999999999999',
                    ''
                ) then null
                else trim(mc_plan_id)
            end
        ) is null
        and case
            when length(trim(enrld_mc_plan_type_cd)) = 1
            and trim(enrld_mc_plan_type_cd) <> '' then lpad(enrld_mc_plan_type_cd, 2, '0')
            else enrld_mc_plan_type_cd
        end = '00' then null
        else case
            when length(trim(enrld_mc_plan_type_cd)) = 1
            and trim(enrld_mc_plan_type_cd) <> '' then lpad(enrld_mc_plan_type_cd, 2, '0')
            else enrld_mc_plan_type_cd
        end
    end as ENRLD_MC_PLAN_TYPE_CODE,
    row_number() over (
        partition by submtg_state_cd,
        msis_ident_num,
        case
            when trim(mc_plan_id) in (
                '0',
                '00',
                '000',
                '0000',
                '00000',
                '000000',
                '0000000',
                '00000000',
                '000000000',
                '0000000000',
                '00000000000',
                '000000000000',
                '8',
                '88',
                '888',
                '8888',
                '88888',
                '888888',
                '8888888',
                '88888888',
                '888888888',
                '8888888888',
                '88888888888',
                '888888888888',
                '9',
                '99',
                '999',
                '9999',
                '99999',
                '999999',
                '9999999',
                '99999999',
                '999999999',
                '9999999999',
                '99999999999',
                '999999999999',
                ''
            ) then null
            else trim(mc_plan_id)
        end,
        (
            case
                when (
                    case
                        when trim(mc_plan_id) in (
                            '0',
                            '00',
                            '000',
                            '0000',
                            '00000',
                            '000000',
                            '0000000',
                            '00000000',
                            '000000000',
                            '0000000000',
                            '00000000000',
                            '000000000000',
                            '8',
                            '88',
                            '888',
                            '8888',
                            '88888',
                            '888888',
                            '8888888',
                            '88888888',
                            '888888888',
                            '8888888888',
                            '88888888888',
                            '888888888888',
                            '9',
                            '99',
                            '999',
                            '9999',
                            '99999',
                            '999999',
                            '9999999',
                            '99999999',
                            '999999999',
                            '9999999999',
                            '99999999999',
                            '999999999999',
                            ''
                        ) then null
                        else trim(mc_plan_id)
                    end
                ) is null
                and case
                    when length(trim(enrld_mc_plan_type_cd)) = 1
                    and trim(enrld_mc_plan_type_cd) <> '' then lpad(enrld_mc_plan_type_cd, 2, '0')
                    else enrld_mc_plan_type_cd
                end = '00' then null
                else case
                    when length(trim(enrld_mc_plan_type_cd)) = 1
                    and trim(enrld_mc_plan_type_cd) <> '' then lpad(enrld_mc_plan_type_cd, 2, '0')
                    else enrld_mc_plan_type_cd
                end
            end
        )
        order by
            submtg_state_cd,
            msis_ident_num,
            TMSIS_RPTG_PRD desc,
            MC_PLAN_ENRLMT_EFCTV_DT desc,
            MC_PLAN_ENRLMT_END_DT desc,
            REC_NUM desc,
            case
                when trim(mc_plan_id) in (
                    '0',
                    '00',
                    '000',
                    '0000',
                    '00000',
                    '000000',
                    '0000000',
                    '00000000',
                    '000000000',
                    '0000000000',
                    '00000000000',
                    '000000000000',
                    '8',
                    '88',
                    '888',
                    '8888',
                    '88888',
                    '888888',
                    '8888888',
                    '88888888',
                    '888888888',
                    '8888888888',
                    '88888888888',
                    '888888888888',
                    '9',
                    '99',
                    '999',
                    '9999',
                    '99999',
                    '999999',
                    '9999999',
                    '99999999',
                    '999999999',
                    '9999999999',
                    '99999999999',
                    '999999999999',
                    ''
                ) then null
                else trim(mc_plan_id)
            end,
            (
                case
                    when (
                        case
                            when trim(mc_plan_id) in (
                                '0',
                                '00',
                                '000',
                                '0000',
                                '00000',
                                '000000',
                                '0000000',
                                '00000000',
                                '000000000',
                                '0000000000',
                                '00000000000',
                                '000000000000',
                                '8',
                                '88',
                                '888',
                                '8888',
                                '88888',
                                '888888',
                                '8888888',
                                '88888888',
                                '888888888',
                                '8888888888',
                                '88888888888',
                                '888888888888',
                                '9',
                                '99',
                                '999',
                                '9999',
                                '99999',
                                '999999',
                                '9999999',
                                '99999999',
                                '999999999',
                                '9999999999',
                                '99999999999',
                                '999999999999',
                                ''
                            ) then null
                            else trim(mc_plan_id)
                        end
                    ) is null
                    and case
                        when length(trim(enrld_mc_plan_type_cd)) = 1
                        and trim(enrld_mc_plan_type_cd) <> '' then lpad(enrld_mc_plan_type_cd, 2, '0')
                        else enrld_mc_plan_type_cd
                    end = '00' then null
                    else case
                        when length(trim(enrld_mc_plan_type_cd)) = 1
                        and trim(enrld_mc_plan_type_cd) <> '' then lpad(enrld_mc_plan_type_cd, 2, '0')
                        else enrld_mc_plan_type_cd
                    end
                end
            )
    ) as mc_deduper
from
    (
        select
            *
        from
            ELG00014
        where
            enrld_mc_plan_type_cd is not null
            or mc_plan_id is not null
    ) t1