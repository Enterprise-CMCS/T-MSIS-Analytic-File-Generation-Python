create temp table nppes_npi distkey (prvdr_npi) sortkey (prvdr_npi) as
select
    *
from
    (
        select
            cast(prvdr_npi as varchar(10)) as prvdr_npi,
            case
                when selected_txnmy_cdx is null then null
                else substring(json_serialize(selected_txnmy_cdx), 3, 10)
            end as selected_txnmy_cd,
            case
                when selected_txnmy_cd in ('315P00000X', '310500000X') then 1
                else 0
            end as prvdr_txnmy_icf,
            case
                when selected_txnmy_cd in (
                    '311500000X',
                    '313M00000X',
                    '314000000X',
                    '3140N1450X',
                    '275N00000X'
                ) then 1
                else 0
            end as prvdr_txnmy_nf,
            case
                when selected_txnmy_cd in (
                    '385H00000X',
                    '385HR2050X',
                    '385HR2055X',
                    '385HR2060X',
                    '385HR2065X',
                    '320900000X',
                    '320800000X',
                    '323P00000X',
                    '322D00000X',
                    '320600000X',
                    '320700000X',
                    '324500000X',
                    '3245S0500X',
                    '281P00000X',
                    '281PC2000X',
                    '282E00000X',
                    '283Q00000X',
                    '283X00000X',
                    '283XC2000X',
                    '273R00000X',
                    '273Y00000X',
                    '276400000X',
                    '310400000X',
                    '3104A0625X',
                    '3104A0630X',
                    '311Z00000X',
                    '311ZA0620X'
                ) then 1
                else 0
            end as prvdr_txnmy_othr_res,
            case
                when selected_txnmy_cd in (
                    '282N00000X',
                    '282NC2000X',
                    '282NC0060X',
                    '282NR1301X',
                    '282NW0100X',
                    '286500000X',
                    '2865M2000X',
                    '2865X1600X',
                    '282J00000X',
                    '284300000X',
                    '273100000X'
                ) then 1
                else 0
            end as prvdr_txnmy_IP
        from
            nppes_npi_step2
    ) a