create
or replace temporary view ELG00007_202111_uniq as
select
    *
from
    (
        select
            *
        from
            ELG00007_uniq
        union
        all
        select
            *
        from
            ELG00007_multi
    )