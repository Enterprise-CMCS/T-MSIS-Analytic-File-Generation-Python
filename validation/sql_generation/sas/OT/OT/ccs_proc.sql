create temp table ccs_proc distkey (cd_rng) sortkey (cd_rng) as
select
    cd_rng,
    ccs,
    case
        when ccs in ('205', '206', '233', '234', '235') then 'Lab       '
        when ccs in (
            '177',
            '178',
            '179',
            '180',
            '181',
            '182',
            '183',
            '184',
            '185',
            '186',
            '187',
            '189',
            '190',
            '191',
            '192',
            '193',
            '194',
            '195',
            '196',
            '197',
            '198',
            '207',
            '208',
            '209',
            '210',
            '226'
        ) then 'Rad       '
        when ccs in ('241', '242', '243') then 'DME       '
        when ccs in ('239') then 'Transprt  '
        else null
    end as code_cat
from
    data_anltcs_dm_prod.ccs_srvcs_prcdr_rfrnc