create
or replace temporary view ELG00006_202111_uniq as
select
    *
from
    (
        select
            *
        from
            ELG00006_uniq
        union
        all
        select
            *
        from
            ELG00006_multi
    )