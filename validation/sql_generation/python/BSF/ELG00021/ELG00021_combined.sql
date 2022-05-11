create
or replace temporary view ELG00021_combined as
select
    b.*,
    coalesce(m.MDCD_ENR, 0) as MDCD_ENR,
    coalesce(c.CHIP_ENR, 0) as CHIP_ENR,
    m.MDCD_ENRLMT_EFF_DT_1,
    m.MDCD_ENRLMT_END_DT_1,
    m.MDCD_ENRLMT_EFF_DT_2,
    m.MDCD_ENRLMT_END_DT_2,
    m.MDCD_ENRLMT_EFF_DT_3,
    m.MDCD_ENRLMT_END_DT_3,
    m.MDCD_ENRLMT_EFF_DT_4,
    m.MDCD_ENRLMT_END_DT_4,
    m.MDCD_ENRLMT_EFF_DT_5,
    m.MDCD_ENRLMT_END_DT_5,
    m.MDCD_ENRLMT_EFF_DT_6,
    m.MDCD_ENRLMT_END_DT_6,
    m.MDCD_ENRLMT_EFF_DT_7,
    m.MDCD_ENRLMT_END_DT_7,
    m.MDCD_ENRLMT_EFF_DT_8,
    m.MDCD_ENRLMT_END_DT_8,
    m.MDCD_ENRLMT_EFF_DT_9,
    m.MDCD_ENRLMT_END_DT_9,
    m.MDCD_ENRLMT_EFF_DT_10,
    m.MDCD_ENRLMT_END_DT_10,
    m.MDCD_ENRLMT_EFF_DT_11,
    m.MDCD_ENRLMT_END_DT_11,
    m.MDCD_ENRLMT_EFF_DT_12,
    m.MDCD_ENRLMT_END_DT_12,
    m.MDCD_ENRLMT_EFF_DT_13,
    m.MDCD_ENRLMT_END_DT_13,
    m.MDCD_ENRLMT_EFF_DT_14,
    m.MDCD_ENRLMT_END_DT_14,
    m.MDCD_ENRLMT_EFF_DT_15,
    m.MDCD_ENRLMT_END_DT_15,
    m.MDCD_ENRLMT_EFF_DT_16,
    m.MDCD_ENRLMT_END_DT_16,
    c.CHIP_ENRLMT_EFF_DT_1,
    c.CHIP_ENRLMT_END_DT_1,
    c.CHIP_ENRLMT_EFF_DT_2,
    c.CHIP_ENRLMT_END_DT_2,
    c.CHIP_ENRLMT_EFF_DT_3,
    c.CHIP_ENRLMT_END_DT_3,
    c.CHIP_ENRLMT_EFF_DT_4,
    c.CHIP_ENRLMT_END_DT_4,
    c.CHIP_ENRLMT_EFF_DT_5,
    c.CHIP_ENRLMT_END_DT_5,
    c.CHIP_ENRLMT_EFF_DT_6,
    c.CHIP_ENRLMT_END_DT_6,
    c.CHIP_ENRLMT_EFF_DT_7,
    c.CHIP_ENRLMT_END_DT_7,
    c.CHIP_ENRLMT_EFF_DT_8,
    c.CHIP_ENRLMT_END_DT_8,
    c.CHIP_ENRLMT_EFF_DT_9,
    c.CHIP_ENRLMT_END_DT_9,
    c.CHIP_ENRLMT_EFF_DT_10,
    c.CHIP_ENRLMT_END_DT_10,
    c.CHIP_ENRLMT_EFF_DT_11,
    c.CHIP_ENRLMT_END_DT_11,
    c.CHIP_ENRLMT_EFF_DT_12,
    c.CHIP_ENRLMT_END_DT_12,
    c.CHIP_ENRLMT_EFF_DT_13,
    c.CHIP_ENRLMT_END_DT_13,
    c.CHIP_ENRLMT_EFF_DT_14,
    c.CHIP_ENRLMT_END_DT_14,
    c.CHIP_ENRLMT_EFF_DT_15,
    c.CHIP_ENRLMT_END_DT_15,
    c.CHIP_ENRLMT_EFF_DT_16,
    c.CHIP_ENRLMT_END_DT_16,
    30 as DAYS_IN_MONTH,
    DT_CHK_1 + DT_CHK_2 + DT_CHK_3 + DT_CHK_4 + DT_CHK_5 + DT_CHK_6 + DT_CHK_7 + DT_CHK_8 + DT_CHK_9 + DT_CHK_10 + DT_CHK_11 + DT_CHK_12 + DT_CHK_13 + DT_CHK_14 + DT_CHK_15 + DT_CHK_16 + DT_CHK_17 + DT_CHK_18 + DT_CHK_19 + DT_CHK_20 + DT_CHK_21 + DT_CHK_22 + DT_CHK_23 + DT_CHK_24 + DT_CHK_25 + DT_CHK_26 + DT_CHK_27 + DT_CHK_28 + DT_CHK_29 + DT_CHK_30 as DAYS_ELIG_IN_MO_CNT,
    case
        when DT_CHK_1 = 1
        and DT_CHK_2 = 1
        and DT_CHK_3 = 1
        and DT_CHK_4 = 1
        and DT_CHK_5 = 1
        and DT_CHK_6 = 1
        and DT_CHK_7 = 1
        and DT_CHK_8 = 1
        and DT_CHK_9 = 1
        and DT_CHK_10 = 1
        and DT_CHK_11 = 1
        and DT_CHK_12 = 1
        and DT_CHK_13 = 1
        and DT_CHK_14 = 1
        and DT_CHK_15 = 1
        and DT_CHK_16 = 1
        and DT_CHK_17 = 1
        and DT_CHK_18 = 1
        and DT_CHK_19 = 1
        and DT_CHK_20 = 1
        and DT_CHK_21 = 1
        and DT_CHK_22 = 1
        and DT_CHK_23 = 1
        and DT_CHK_24 = 1
        and DT_CHK_25 = 1
        and DT_CHK_26 = 1
        and DT_CHK_27 = 1
        and DT_CHK_28 = 1
        and DT_CHK_29 = 1
        and DT_CHK_30 = 1 then 1
        else 0
    end as ELIGIBLE_ENTIRE_MONTH_IND,
    greatest(m.ELIG_LAST_DAY, c.ELIG_LAST_DAY, 0) as ELIGIBLE_LAST_DAY_OF_MONTH_IND,
    case
        when m.MDCD_ENR = 1 then 1
        when c.CHIP_ENR = 1 then 2
        else null
    end as ENROLLMENT_TYPE_FLAG,
    case
        when b.bucket_c = 1
        or (
            MDCD_ENR = 1
            and CHIP_ENR = 1
        )
        or (
            m.MDCD_ENRLMT_EFF_DT_1 is not null
            and m.MDCD_ENRLMT_EFF_DT_2 is not null
        )
        or (
            c.CHIP_ENRLMT_EFF_DT_1 is not null
            and c.CHIP_ENRLMT_EFF_DT_2 is not null
        ) then 0
        else 1
    end as SINGLE_ENR_FLAG,
    case
        when b.submtg_state_cd = '13' then 'GA'
        when b.submtg_state_cd = '50' then 'VT'
        when b.submtg_state_cd = '32' then 'NV'
        when b.submtg_state_cd = '17' then 'IL'
        when b.submtg_state_cd = '49' then 'UT'
        when b.submtg_state_cd = '30' then 'MT'
        when b.submtg_state_cd = '42' then 'PA'
        when b.submtg_state_cd = '24' then 'MD'
        when b.submtg_state_cd = '55' then 'WI'
        when b.submtg_state_cd = '23' then 'ME'
        when b.submtg_state_cd = '27' then 'MN'
        when b.submtg_state_cd = '38' then 'ND'
        when b.submtg_state_cd = '19' then 'IA'
        when b.submtg_state_cd = '47' then 'TN'
        when b.submtg_state_cd = '28' then 'MS'
        when b.submtg_state_cd = '78' then 'VI'
        when b.submtg_state_cd = '08' then 'CO'
        when b.submtg_state_cd = '51' then 'VA'
        when b.submtg_state_cd = '15' then 'HI'
        when b.submtg_state_cd = '33' then 'NH'
        when b.submtg_state_cd = '18' then 'IN'
        when b.submtg_state_cd = '31' then 'NE'
        when b.submtg_state_cd = '35' then 'NM'
        when b.submtg_state_cd = '25' then 'MA'
        when b.submtg_state_cd = '36' then 'NY'
        when b.submtg_state_cd = '45' then 'SC'
        when b.submtg_state_cd = '53' then 'WA'
        when b.submtg_state_cd = '26' then 'MI'
        when b.submtg_state_cd = '46' then 'SD'
        when b.submtg_state_cd = '12' then 'FL'
        when b.submtg_state_cd = '21' then 'KY'
        when b.submtg_state_cd = '41' then 'OR'
        when b.submtg_state_cd = '09' then 'CT'
        when b.submtg_state_cd = '39' then 'OH'
        when b.submtg_state_cd = '04' then 'AZ'
        when b.submtg_state_cd = '05' then 'AR'
        when b.submtg_state_cd = '72' then 'PR'
        when b.submtg_state_cd = '22' then 'LA'
        when b.submtg_state_cd = '44' then 'RI'
        when b.submtg_state_cd = '16' then 'ID'
        when b.submtg_state_cd = '40' then 'OK'
        when b.submtg_state_cd = '54' then 'WV'
        when b.submtg_state_cd = '34' then 'NJ'
        when b.submtg_state_cd = '01' then 'AL'
        when b.submtg_state_cd = '11' then 'DC'
        when b.submtg_state_cd = '20' then 'KS'
        when b.submtg_state_cd = '29' then 'MO'
        when b.submtg_state_cd = '10' then 'DE'
    end as ST_ABBREV
from
    ELG00021_buckets b
    left outer join MDCD_SPELLS m on b.submtg_state_cd = m.submtg_state_cd
    and b.msis_ident_num = m.msis_ident_num
    left outer join CHIP_SPELLS c on b.submtg_state_cd = c.submtg_state_cd
    and b.msis_ident_num = c.msis_ident_num