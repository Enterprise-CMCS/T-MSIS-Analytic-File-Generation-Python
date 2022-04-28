create temp table ELG00003_multi_all distkey(msis_ident_num) sortkey(submtg_state_cd, msis_ident_num) as
select
    t1.*,
case
        when trim(PRMRY_LANG_CODE) in('CHI') then 'C'
        when trim(PRMRY_LANG_CODE) in('GER', 'GMH', 'GOH', 'GSW', 'NDS') then 'D'
        when trim(PRMRY_LANG_CODE) in('ENG', 'ENM', 'ANG') then 'E'
        when trim(PRMRY_LANG_CODE) in('FRE', 'FRM', 'FRO') then 'F'
        when trim(PRMRY_LANG_CODE) in('GRC', 'GRE') then 'G'
        when trim(PRMRY_LANG_CODE) in('ITA', 'SCN') then 'I'
        when trim(PRMRY_LANG_CODE) in('JPN') then 'J'
        when trim(PRMRY_LANG_CODE) in('NOB', 'NNO', 'NOR') then 'N'
        when trim(PRMRY_LANG_CODE) in('POL') then 'P'
        when trim(PRMRY_LANG_CODE) in('RUS') then 'R'
        when trim(PRMRY_LANG_CODE) in('SPA') then 'S'
        when trim(PRMRY_LANG_CODE) in('SWE') then 'V'
        when trim(PRMRY_LANG_CODE) in('SRP', 'HRV') then 'W'
        when trim(PRMRY_LANG_CODE) in('UND', '', '.')
        or PRMRY_LANG_CODE is null then null
        else 'O'
    end as PRMRY_LANG_FLG
from
    ELG00003_v t1
    inner join ELG00003_recCt t2 on t1.submtg_state_cd = t2.submtg_state_cd
    and t1.msis_ident_num = t2.msis_ident_num
    and t2.recCt > 1