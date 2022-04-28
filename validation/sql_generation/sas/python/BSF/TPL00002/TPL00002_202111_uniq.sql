create
or replace temporary view TPL00002_202111_uniq as
select
    *
from
    (
        select
            *
        from
            TPL00002_uniq
        union
        all
        select
            *
        from
            TPL00002_multi
    )