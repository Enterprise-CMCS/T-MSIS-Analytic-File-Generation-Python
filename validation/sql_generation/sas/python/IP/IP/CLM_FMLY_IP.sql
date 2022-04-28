create
or replace temporary view CLM_FMLY_IP as
select
    TMSIS_RUN_ID,
    coalesce(upper(ORGNL_CLM_NUM), '~') as ORGNL_CLM_NUM,
    coalesce(upper(ADJSTMT_CLM_NUM), '~') as ADJSTMT_CLM_NUM,
    SUBMTG_STATE_CD,
    coalesce(ADJDCTN_DT, '01JAN1960') as ADJDCTN_DT,
    COALESCE(ADJSTMT_IND, 'X') as ADJSTMT_IND
from
    cms_prod.TMSIS_CLM_FMLY_IP F
where
    CLM_FMLY_FINL_ACTN_IND = 1
    and (F.submtg_state_cd, F.tmsis_run_id) in (
        concat('01', 4574),
        concat('02', 4567),
        concat('04', 4554),
        concat('05', 4550),
        concat('06', 4556),
        concat('08', 4578),
        concat('09', 4568),
        concat('10', 4557),
        concat('11', 4545),
        concat('12', 4555),
        concat('13', 4538),
        concat('15', 4558),
        concat('16', 4579),
        concat('17', 4584),
        concat('18', 4561),
        concat('19', 4548),
        concat('20', 4589),
        concat('21', 4553),
        concat('22', 4573),
        concat('23', 4540),
        concat('24', 4569),
        concat('25', 4559),
        concat('26', 4585),
        concat('27', 4539),
        concat('28', 4594),
        concat('29', 4562),
        concat('30', 4580),
        concat('31', 4546),
        concat('32', 4551),
        concat('33', 4570),
        concat('34', 4590),
        concat('35', 4552),
        concat('36', 4566),
        concat('37', 4517),
        concat('38', 4575),
        concat('39', 4571),
        concat('40', 4593),
        concat('41', 4541),
        concat('42', 4560),
        concat('44', 4592),
        concat('45', 4544),
        concat('46', 4549),
        concat('47', 4583),
        concat('48', 4507),
        concat('49', 4577),
        concat('50', 4581),
        concat('51', 4564),
        concat('53', 4587),
        concat('54', 4582),
        concat('55', 4565),
        concat('56', 4423),
        concat('72', 4547),
        concat('78', 4563),
        concat('93', 4447),
        concat('97', 4543)
    )
group by
    1,
    2,
    3,
    4,
    5,
    6
having
    count(TMSIS_RUN_ID) = 1
