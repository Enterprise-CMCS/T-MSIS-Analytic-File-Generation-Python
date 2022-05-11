create
or replace temporary view ELG00021_buckets as
select
    b.submtg_state_cd,
    b.msis_ident_num,
    max(b.tmsis_run_id) as tmsis_run_id,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-01'))
            and (ENRLMT_END_DT >= to_date('2021-11-01')) then 1
            else 0
        end
    ) as DT_CHK_1,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-02'))
            and (ENRLMT_END_DT >= to_date('2021-11-02')) then 1
            else 0
        end
    ) as DT_CHK_2,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-03'))
            and (ENRLMT_END_DT >= to_date('2021-11-03')) then 1
            else 0
        end
    ) as DT_CHK_3,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-04'))
            and (ENRLMT_END_DT >= to_date('2021-11-04')) then 1
            else 0
        end
    ) as DT_CHK_4,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-05'))
            and (ENRLMT_END_DT >= to_date('2021-11-05')) then 1
            else 0
        end
    ) as DT_CHK_5,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-06'))
            and (ENRLMT_END_DT >= to_date('2021-11-06')) then 1
            else 0
        end
    ) as DT_CHK_6,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-07'))
            and (ENRLMT_END_DT >= to_date('2021-11-07')) then 1
            else 0
        end
    ) as DT_CHK_7,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-08'))
            and (ENRLMT_END_DT >= to_date('2021-11-08')) then 1
            else 0
        end
    ) as DT_CHK_8,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-09'))
            and (ENRLMT_END_DT >= to_date('2021-11-09')) then 1
            else 0
        end
    ) as DT_CHK_9,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-10'))
            and (ENRLMT_END_DT >= to_date('2021-11-10')) then 1
            else 0
        end
    ) as DT_CHK_10,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-11'))
            and (ENRLMT_END_DT >= to_date('2021-11-11')) then 1
            else 0
        end
    ) as DT_CHK_11,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-12'))
            and (ENRLMT_END_DT >= to_date('2021-11-12')) then 1
            else 0
        end
    ) as DT_CHK_12,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-13'))
            and (ENRLMT_END_DT >= to_date('2021-11-13')) then 1
            else 0
        end
    ) as DT_CHK_13,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-14'))
            and (ENRLMT_END_DT >= to_date('2021-11-14')) then 1
            else 0
        end
    ) as DT_CHK_14,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-15'))
            and (ENRLMT_END_DT >= to_date('2021-11-15')) then 1
            else 0
        end
    ) as DT_CHK_15,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-16'))
            and (ENRLMT_END_DT >= to_date('2021-11-16')) then 1
            else 0
        end
    ) as DT_CHK_16,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-17'))
            and (ENRLMT_END_DT >= to_date('2021-11-17')) then 1
            else 0
        end
    ) as DT_CHK_17,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-18'))
            and (ENRLMT_END_DT >= to_date('2021-11-18')) then 1
            else 0
        end
    ) as DT_CHK_18,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-19'))
            and (ENRLMT_END_DT >= to_date('2021-11-19')) then 1
            else 0
        end
    ) as DT_CHK_19,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-20'))
            and (ENRLMT_END_DT >= to_date('2021-11-20')) then 1
            else 0
        end
    ) as DT_CHK_20,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-21'))
            and (ENRLMT_END_DT >= to_date('2021-11-21')) then 1
            else 0
        end
    ) as DT_CHK_21,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-22'))
            and (ENRLMT_END_DT >= to_date('2021-11-22')) then 1
            else 0
        end
    ) as DT_CHK_22,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-23'))
            and (ENRLMT_END_DT >= to_date('2021-11-23')) then 1
            else 0
        end
    ) as DT_CHK_23,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-24'))
            and (ENRLMT_END_DT >= to_date('2021-11-24')) then 1
            else 0
        end
    ) as DT_CHK_24,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-25'))
            and (ENRLMT_END_DT >= to_date('2021-11-25')) then 1
            else 0
        end
    ) as DT_CHK_25,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-26'))
            and (ENRLMT_END_DT >= to_date('2021-11-26')) then 1
            else 0
        end
    ) as DT_CHK_26,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-27'))
            and (ENRLMT_END_DT >= to_date('2021-11-27')) then 1
            else 0
        end
    ) as DT_CHK_27,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-28'))
            and (ENRLMT_END_DT >= to_date('2021-11-28')) then 1
            else 0
        end
    ) as DT_CHK_28,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-29'))
            and (ENRLMT_END_DT >= to_date('2021-11-29')) then 1
            else 0
        end
    ) as DT_CHK_29,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in (1, 2)
            and (ENRLMT_EFCTV_DT <= to_date('2021-11-30'))
            and (ENRLMT_END_DT >= to_date('2021-11-30')) then 1
            else 0
        end
    ) as DT_CHK_30,
    sum(
        case
            when cast(ENRLMT_TYPE_CD as integer) not in(1, 2)
            or ENRLMT_TYPE_CD is null then 1
            else 0
        end
    ) as bucket_c
from
    ELG00021_v b
group by
    b.submtg_state_cd,
    b.msis_ident_num