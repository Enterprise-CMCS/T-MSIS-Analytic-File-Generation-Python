create
or replace temporary view ELG00018_202111_uniq as
select
    *
from
    (
        select
            *
        from
            ELG00018_uniq
        union
        all
        select
            *
        from
            ELG00018_multi
    )