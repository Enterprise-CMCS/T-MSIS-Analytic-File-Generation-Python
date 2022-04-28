create temp table constructed_LT distkey (ORGNL_CLM_NUM_LINE) sortkey (
    NEW_SUBMTG_STATE_CD_LINE,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND
) as
select
    NEW_SUBMTG_STATE_CD_LINE,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND,
    max(RN) as NUM_CLL,
    sum (
        case
            when substring(lpad(REV_CD, 4, '0'), 1, 2) = '01'
            or substring(lpad(REV_CD, 4, '0'), 1, 3) in ('020', '021') then MDCD_PD_AMT
            when rev_cd is not null
            and mdcd_pd_amt is not null then 0
        end
    ) as ACCOMMODATION_PAID,
    sum (
        case
            when substring(lpad(REV_CD, 4, '0'), 1, 2) <> '01'
            and substring(lpad(REV_CD, 4, '0'), 1, 3) not in ('020', '021') then MDCD_PD_AMT
            when REV_CD is not null
            and MDCD_PD_AMT is not null then 0
        end
    ) as ANCILLARY_PAID,
    max (
        case
            when lpad(TOS_CD, 3, '0') in ('044', '045') then 1
            when TOS_CD is null then null
            else 0
        end
    ) as MH_DAYS_OVER_65,
    max(
        case
            when lpad(TOS_CD, 3, '0') = '048' then 1
            when TOS_CD is null then null
            else 0
        end
    ) as MH_DAYS_UNDER_21
from
    LT_LINE
group by
    NEW_SUBMTG_STATE_CD_LINE,
    ORGNL_CLM_NUM_LINE,
    ADJSTMT_CLM_NUM_LINE,
    ADJDCTN_DT_LINE,
    LINE_ADJSTMT_IND