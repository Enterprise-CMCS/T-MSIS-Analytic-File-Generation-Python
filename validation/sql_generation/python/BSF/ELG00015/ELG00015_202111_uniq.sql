create
or replace temporary view ELG00015_202111_uniq as
select
    *
from
    (
        select
            *
        from
            ELG00015_uniq
        union
        all
        select
            *
        from
            ELG00015_multi
    )