create
or replace temporary view ELG00010_202111_uniq as
select
    *
from
    (
        select
            *
        from
            ELG00010_uniq
        union
        all
        select
            *
        from
            ELG00010_multi
    )