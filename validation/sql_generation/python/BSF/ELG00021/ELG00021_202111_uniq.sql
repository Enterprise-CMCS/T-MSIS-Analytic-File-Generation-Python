create or replace temporary view ELG00021_202111_uniq as
select
    c.*,
    s.ssn_ind,
    case
        when ST_ABBREV in('CT', 'MA', 'ME', 'NH', 'RI', 'VT') then '01'
        when ST_ABBREV in('NJ', 'NY', 'PR', 'VI') then '02'
        when ST_ABBREV in('DE', 'DC', 'MD', 'PA', 'VA', 'WV') then '03'
        when ST_ABBREV in('AL', 'FL', 'GA', 'KY', 'MS', 'NC', 'SC', 'TN') then '04'
        when ST_ABBREV in('IL', 'IN', 'MI', 'MN', 'OH', 'WI') then '05'
        when ST_ABBREV in('AR', 'LA', 'NM', 'OK', 'TX') then '06'
        when ST_ABBREV in('IA', 'KS', 'MO', 'NE') then '07'
        when ST_ABBREV in('CO', 'MT', 'ND', 'SD', 'UT', 'WY') then '08'
        when ST_ABBREV in('AZ', 'CA', 'HI', 'NV', 'AS', 'GU', 'MP') then '09'
        when ST_ABBREV in('AK', 'ID', 'OR', 'WA') then '10'
        else '11'
    end as REGION
from
    ELG00021_combined c
    left join ssn_ind s on c.submtg_state_cd = s.submtg_state_cd