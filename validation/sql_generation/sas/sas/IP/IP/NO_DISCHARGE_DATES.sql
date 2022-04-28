create temp TABLE NO_DISCHARGE_DATES distkey(ORGNL_CLM_NUM) sortkey(
    TMSIS_RUN_ID,
    SUBMTG_STATE_CD,
    ORGNL_CLM_NUM,
    ADJSTMT_CLM_NUM,
    ADJDCTN_DT,
    ADJSTMT_IND
) as with MAX_DATES as (
    select
        H.TMSIS_RUN_ID,
        H.ORGNL_CLM_NUM,
        H.ADJSTMT_CLM_NUM,
        H.SUBMTG_STATE_CD,
        H.ADJDCTN_DT,
        H.ADJSTMT_IND,
        MAX(L.SRVC_ENDG_DT) as SRVC_ENDG_DT,
        MAX(L.SRVC_BGNNG_DT) as SRVC_BGNNG_DT
    from
        HEADER2_IP H
        inner join cms_prod.TMSIS_CLL_REC_IP L on H.TMSIS_RUN_ID = L.TMSIS_RUN_ID
        and H.SUBMTG_STATE_CD = L.SUBMTG_STATE_CD
        and H.ORGNL_CLM_NUM = upper(coalesce(L.ORGNL_CLM_NUM, '~'))
        and H.ADJSTMT_CLM_NUM = upper(coalesce(L.ADJSTMT_CLM_NUM, '~'))
        and H.ADJDCTN_DT = coalesce(L.ADJDCTN_DT, '01JAN1960')
        and H.ADJSTMT_IND = upper(coalesce(L.LINE_ADJSTMT_IND, 'X'))
    where
        L.TMSIS_ACTV_IND = 1
        and coalesce(L.SRVC_ENDG_DT, L.SRVC_BGNNG_DT) is not NULL
        and H.NO_DISCH_DT = 1
        AND (L.SUBMTG_STATE_CD, L.TMSIS_RUN_ID) IN (
            ('01', 4516),
            ('02', 4530),
            ('04', 4491),
            ('05', 4492),
            ('06', 4518),
            ('08', 4500),
            ('09', 4485),
            ('10', 4493),
            ('11', 4490),
            ('12', 4502),
            ('13', 4494),
            ('15', 4503),
            ('16', 4519),
            ('17', 4520),
            ('18', 4497),
            ('19', 4511),
            ('20', 4482),
            ('21', 4486),
            ('22', 4496),
            ('23', 4499),
            ('24', 4531),
            ('25', 4504),
            ('26', 4528),
            ('27', 4481),
            ('28', 4479),
            ('29', 4524),
            ('30', 4529),
            ('31', 4505),
            ('32', 4512),
            ('33', 4532),
            ('34', 4477),
            ('35', 4483),
            ('36', 4527),
            ('37', 4517),
            ('38', 4510),
            ('39', 4506),
            ('40', 4480),
            ('41', 4513),
            ('42', 4495),
            ('44', 4534),
            ('45', 4488),
            ('46', 4521),
            ('47', 4525),
            ('48', 4507),
            ('49', 4484),
            ('50', 4522),
            ('51', 4508),
            ('53', 4526),
            ('54', 4515),
            ('55', 4514),
            ('56', 4423),
            ('72', 4501),
            ('78', 4523),
            ('93', 4447),
            ('97', 4487)
        )
    group by
        H.TMSIS_RUN_ID,
        H.ORGNL_CLM_NUM,
        H.ADJSTMT_CLM_NUM,
        H.SUBMTG_STATE_CD,
        H.ADJDCTN_DT,
        H.ADJSTMT_IND
)
select
    *
from
    MAX_DATES
where
    (
        date_part (year, coalesce(SRVC_ENDG_DT, SRVC_BGNNG_DT)) = 2021
        and date_part (month, coalesce(SRVC_ENDG_DT, SRVC_BGNNG_DT)) = 10
    )