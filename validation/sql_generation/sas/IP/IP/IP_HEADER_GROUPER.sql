CREATE TEMP TABLE IP_HEADER_GROUPER distkey (ORGNL_CLM_NUM) sortkey(
    NEW_SUBMTG_STATE_CD,
    ORGNL_CLM_NUM,
    ADJSTMT_CLM_NUM,
    ADJDCTN_DT,
    ADJSTMT_IND
) as
select
    a.*,
    case
        when (
            (
                DGNS_1_CD_IND < '1'
                or DGNS_1_CD_IND > '2'
                or DGNS_1_CD_IND is NULL
                or DGNS_1_TEMP is NULL
            )
            and (
                DGNS_2_CD_IND < '1'
                or DGNS_2_CD_IND > '2'
                or DGNS_2_CD_IND is NULL
                or DGNS_2_TEMP is NULL
            )
        ) then null
        when (IPM91 + IPM92 + IPM01 + IPM02 > 0) then 1
        when (
            IPM91 + IPM92 = 0
            or IPM01 + IPM02 = 0
        ) then 0
    end as IP_MH_DX_IND,
    case
        when (
            (
                DGNS_1_CD_IND < '1'
                or DGNS_1_CD_IND > '2'
                or DGNS_1_CD_IND is NULL
                or DGNS_1_TEMP is NULL
            )
            and (
                DGNS_2_CD_IND < '1'
                or DGNS_2_CD_IND > '2'
                or DGNS_2_CD_IND is NULL
                or DGNS_2_TEMP is NULL
            )
        ) then null
        when (IPS91 + IPS92 + IPS01 + IPS02 > 0) then 1
        when (
            IPS91 + IPS92 = 0
            or IPS01 + IPS02 = 0
        ) then 0
    end as IP_SUD_DX_IND,
    case
        when a.IP_MH_TAXONOMY_IND_HDR is null
        and l.IP_MH_TAXONOMY_IND_LINE is null then null
        when (
            coalesce(a.IP_MH_TAXONOMY_IND_HDR, 0) + coalesce(l.IP_MH_TAXONOMY_IND_LINE, 0)
        ) = 2 then 1
        when a.IP_MH_TAXONOMY_IND_HDR = 1 then 2
        when l.IP_MH_TAXONOMY_IND_LINE = 1 then 3
        else 0
    end as IP_MH_TAXONOMY_IND,
    case
        when a.IP_SUD_TAXONOMY_IND_HDR is null
        and l.IP_SUD_TAXONOMY_IND_LINE is null then null
        when (
            coalesce(a.IP_SUD_TAXONOMY_IND_HDR, 0) + coalesce(l.IP_SUD_TAXONOMY_IND_LINE, 0)
        ) = 2 then 1
        when a.IP_SUD_TAXONOMY_IND_HDR = 1 then 2
        when l.IP_SUD_TAXONOMY_IND_LINE = 1 then 3
        else 0
    end as IP_SUD_TAXONOMY_IND
from
    IP_HEADER_STEP1 a
    left join IP_TAXONOMY l on l.NEW_SUBMTG_STATE_CD_LINE = a.NEW_SUBMTG_STATE_CD
    and l.ORGNL_CLM_NUM_LINE = a.ORGNL_CLM_NUM
    and l.ADJSTMT_CLM_NUM_LINE = a.ADJSTMT_CLM_NUM
    and l.ADJDCTN_DT_LINE = a.ADJDCTN_DT
    and l.LINE_ADJSTMT_IND = a.ADJSTMT_IND