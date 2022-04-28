create temp table ELG00004 distkey(msis_ident_num) sortkey(
    submtg_state_cd,
    msis_ident_num,
    ELGBL_ADR_TYPE_CD
) as
select
    a.TMSIS_RUN_ID,
    a.TMSIS_ACTV_IND,
    a.SUBMTG_STATE_CD,
    a.REC_NUM,
    a.ELGBL_LINE_1_ADR,
    a.ELGBL_LINE_2_ADR,
    a.ELGBL_LINE_3_ADR,
    a.ELGBL_ADR_TYPE_CD,
    a.ELGBL_CITY_NAME,
    a.ELGBL_CNTY_CD,
    a.ELGBL_PHNE_NUM,
    case
        when upper(trim(a.ELGBL_STATE_CD)) in(
            '1',
            '2',
            '4',
            '5',
            '6',
            '8',
            '9',
            '01',
            '02',
            '04',
            '05',
            '06',
            '08',
            '09',
            '10',
            '11',
            '12',
            '13',
            '15',
            '16',
            '17',
            '18',
            '19',
            '20',
            '21',
            '22',
            '23',
            '24',
            '25',
            '26',
            '27',
            '28',
            '29',
            '30',
            '31',
            '32',
            '33',
            '34',
            '35',
            '36',
            '37',
            '38',
            '39',
            '40',
            '41',
            '42',
            '44',
            '45',
            '46',
            '47',
            '48',
            '49',
            '50',
            '51',
            '53',
            '54',
            '55',
            '56',
            '60',
            '64',
            '66',
            '67',
            '68',
            '69',
            '70',
            '71',
            '72',
            '74',
            '76',
            '78',
            '79',
            '81',
            '84',
            '86',
            '89',
            '95'
        ) then upper(trim(a.ELGBL_STATE_CD))
        else null
    end as ELGBL_STATE_CD,
    a.ELGBL_ZIP_CD,
    a.ELGBL_ADR_EFCTV_DT,
    a.ELGBL_ADR_END_DT,
    a.MSIS_IDENT_NUM,
    a.TMSIS_RPTG_PRD
from
    cms_prod.TMSIS_ELGBL_CNTCT a
    left join data_anltcs_dm_prod.state_submsn_type s on a.submtg_state_cd = s.submtg_state_cd
    and upper(s.fil_type) = 'ELG'
where
    (
        a.TMSIS_ACTV_IND = 1
        and (
            date_cmp(ELGBL_ADR_EFCTV_DT, '30NOV2021') in(-1, 0)
            and (
                date_cmp(ELGBL_ADR_END_DT, '01NOV2021') in(0, 1)
                or ELGBL_ADR_END_DT is NULL
            )
        )
    )
    and (
        (
            upper(coalesce(s.submsn_type, 'X')) <> 'CSO'
            and date_cmp(a.TMSIS_RPTG_PRD, '01NOV2021') in(1, 0)
        )
        or (upper(coalesce(s.submsn_type, 'X')) = 'CSO')
    )
    and (a.submtg_state_cd, a.tmsis_run_id) in (
        ('01', 4574),
        ('02', 4567),
        ('04', 4554),
        ('05', 4550),
        ('06', 4556),
        ('08', 4578),
        ('09', 4568),
        ('10', 4557),
        ('11', 4545),
        ('12', 4555),
        ('13', 4538),
        ('15', 4558),
        ('16', 4579),
        ('17', 4584),
        ('18', 4561),
        ('19', 4548),
        ('20', 4589),
        ('21', 4553),
        ('22', 4573),
        ('23', 4540),
        ('24', 4569),
        ('25', 4559),
        ('26', 4585),
        ('27', 4539),
        ('28', 4594),
        ('29', 4562),
        ('30', 4580),
        ('31', 4546),
        ('32', 4551),
        ('33', 4570),
        ('34', 4590),
        ('35', 4552),
        ('36', 4566),
        ('37', 4517),
        ('38', 4575),
        ('39', 4571),
        ('40', 4593),
        ('41', 4541),
        ('42', 4560),
        ('44', 4592),
        ('45', 4544),
        ('46', 4549),
        ('47', 4583),
        ('48', 4507),
        ('49', 4577),
        ('50', 4581),
        ('51', 4564),
        ('53', 4587),
        ('54', 4582),
        ('55', 4565),
        ('56', 4423),
        ('72', 4547),
        ('78', 4563),
        ('93', 4447),
        ('97', 4543)
    )
    and a.msis_ident_num is not null