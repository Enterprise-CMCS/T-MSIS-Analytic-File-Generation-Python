create temp table ELG00021_buckets distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    b.submtg_state_cd,
    b.msis_ident_num,
    max(b.tmsis_run_id) as tmsis_run_id,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '01NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '01NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_1,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '02NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '02NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_2,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '03NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '03NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_3,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '04NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '04NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_4,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '05NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '05NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_5,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '06NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '06NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_6,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '07NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '07NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_7,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '08NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '08NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_8,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '09NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '09NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_9,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '10NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '10NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_10,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '11NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '11NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_11,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '12NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '12NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_12,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '13NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '13NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_13,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '14NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '14NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_14,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '15NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '15NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_15,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '16NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '16NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_16,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '17NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '17NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_17,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '18NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '18NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_18,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '19NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '19NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_19,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '20NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '20NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_20,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '21NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '21NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_21,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '22NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '22NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_22,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '23NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '23NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_23,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '24NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '24NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_24,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '25NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '25NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_25,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '26NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '26NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_26,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '27NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '27NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_27,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '28NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '28NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_28,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '29NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '29NOV2021') in(1, 0) then 1
            else 0
        end
    ) as DT_CHK_29,
    max(
        case
            when cast(ENRLMT_TYPE_CD as integer) in(1, 2)
            and date_cmp(ENRLMT_EFCTV_DT, '30NOV2021') in(-1, 0)
            and date_cmp(ENRLMT_END_DT, '30NOV2021') in(1, 0) then 1
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