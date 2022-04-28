create
or replace temporary view ELG00005_202111_uniq as
select
    *
from
    (
        select
            *
        from
            ELG00005_uniq
        union
        all
        select
            *
        from
            ELG00005_multi
    )