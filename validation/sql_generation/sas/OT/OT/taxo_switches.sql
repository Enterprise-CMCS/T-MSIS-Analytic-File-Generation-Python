create temp table taxo_switches distkey (prvdr_npi) sortkey (prvdr_npi) as
select
    prvdr_npi,
    concat(
        nvl(hc_prvdr_prmry_txnmy_sw_1, ' ') :: char,
        concat(
            nvl(hc_prvdr_prmry_txnmy_sw_2, ' ') :: char,
            concat(
                nvl(hc_prvdr_prmry_txnmy_sw_3, ' ') :: char,
                concat(
                    nvl(hc_prvdr_prmry_txnmy_sw_4, ' ') :: char,
                    concat(
                        nvl(hc_prvdr_prmry_txnmy_sw_5, ' ') :: char,
                        concat(
                            nvl(hc_prvdr_prmry_txnmy_sw_6, ' ') :: char,
                            concat(
                                nvl(hc_prvdr_prmry_txnmy_sw_7, ' ') :: char,
                                concat(
                                    nvl(hc_prvdr_prmry_txnmy_sw_8, ' ') :: char,
                                    concat(
                                        nvl(hc_prvdr_prmry_txnmy_sw_9, ' ') :: char,
                                        concat(
                                            nvl(hc_prvdr_prmry_txnmy_sw_10, ' ') :: char,
                                            concat(
                                                nvl(hc_prvdr_prmry_txnmy_sw_11, ' ') :: char,
                                                concat(
                                                    nvl(hc_prvdr_prmry_txnmy_sw_12, ' ') :: char,
                                                    concat(
                                                        nvl(hc_prvdr_prmry_txnmy_sw_13, ' ') :: char,
                                                        concat(
                                                            nvl(hc_prvdr_prmry_txnmy_sw_14, ' ') :: char,
                                                            nvl(hc_prvdr_prmry_txnmy_sw_15, ' ') :: char
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    ) as sw_positions,
    regexp_count(sw_positions, 'Y') as taxo_switches,
    case
        when hc_prvdr_txnmy_cd_1 is not null
        and hc_prvdr_txnmy_cd_1 <> ' ' then 'X'
        else ' '
    end as taxopos1,
    case
        when hc_prvdr_txnmy_cd_2 is not null
        and hc_prvdr_txnmy_cd_2 <> ' ' then 'X'
        else ' '
    end as taxopos2,
    case
        when hc_prvdr_txnmy_cd_3 is not null
        and hc_prvdr_txnmy_cd_3 <> ' ' then 'X'
        else ' '
    end as taxopos3,
    case
        when hc_prvdr_txnmy_cd_4 is not null
        and hc_prvdr_txnmy_cd_4 <> ' ' then 'X'
        else ' '
    end as taxopos4,
    case
        when hc_prvdr_txnmy_cd_5 is not null
        and hc_prvdr_txnmy_cd_5 <> ' ' then 'X'
        else ' '
    end as taxopos5,
    case
        when hc_prvdr_txnmy_cd_6 is not null
        and hc_prvdr_txnmy_cd_6 <> ' ' then 'X'
        else ' '
    end as taxopos6,
    case
        when hc_prvdr_txnmy_cd_7 is not null
        and hc_prvdr_txnmy_cd_7 <> ' ' then 'X'
        else ' '
    end as taxopos7,
    case
        when hc_prvdr_txnmy_cd_8 is not null
        and hc_prvdr_txnmy_cd_8 <> ' ' then 'X'
        else ' '
    end as taxopos8,
    case
        when hc_prvdr_txnmy_cd_9 is not null
        and hc_prvdr_txnmy_cd_9 <> ' ' then 'X'
        else ' '
    end as taxopos9,
    case
        when hc_prvdr_txnmy_cd_10 is not null
        and hc_prvdr_txnmy_cd_10 <> ' ' then 'X'
        else ' '
    end as taxopos10,
    case
        when hc_prvdr_txnmy_cd_11 is not null
        and hc_prvdr_txnmy_cd_11 <> ' ' then 'X'
        else ' '
    end as taxopos11,
    case
        when hc_prvdr_txnmy_cd_12 is not null
        and hc_prvdr_txnmy_cd_12 <> ' ' then 'X'
        else ' '
    end as taxopos12,
    case
        when hc_prvdr_txnmy_cd_13 is not null
        and hc_prvdr_txnmy_cd_13 <> ' ' then 'X'
        else ' '
    end as taxopos13,
    case
        when hc_prvdr_txnmy_cd_14 is not null
        and hc_prvdr_txnmy_cd_14 <> ' ' then 'X'
        else ' '
    end as taxopos14,
    case
        when hc_prvdr_txnmy_cd_15 is not null
        and hc_prvdr_txnmy_cd_15 <> ' ' then 'X'
        else ' '
    end as taxopos15,
    concat(
        taxopos1,
        concat(
            taxopos2,
            concat(
                taxopos3,
                concat(
                    taxopos4,
                    concat(
                        taxopos5,
                        concat(
                            taxopos6,
                            concat(
                                taxopos7,
                                concat(
                                    taxopos8,
                                    concat(
                                        taxopos9,
                                        concat(
                                            taxopos10,
                                            concat(
                                                taxopos11,
                                                concat(
                                                    taxopos12,
                                                    concat(taxopos13, concat(taxopos14, taxopos15))
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        )
    ) as cd_positions,
    regexp_count(cd_positions, 'X') as taxo_cds,
    case
        when hc_prvdr_txnmy_cd_1 is not null
        and hc_prvdr_txnmy_cd_1 <> ' 
' then 1
        else null
    end as taxo1,
    case
        when hc_prvdr_txnmy_cd_2 is not null
        and hc_prvdr_txnmy_cd_2 <> ' ' then 2
        else null
    end as taxo2,
    case
        when hc_prvdr_txnmy_cd_3 is not null
        and hc_prvdr_txnmy_cd_3 <> ' ' then 3
        else null
    end as taxo3,
    case
        when hc_prvdr_txnmy_cd_4 is not null
        and hc_prvdr_txnmy_cd_4 <> ' ' then 4
        else null
    end as taxo4,
    case
        when hc_prvdr_txnmy_cd_5 is not null
        and hc_prvdr_txnmy_cd_5 <> ' ' then 5
        else null
    end as taxo5,
    case
        when hc_prvdr_txnmy_cd_6 is not null
        and hc_prvdr_txnmy_cd_6 <> ' ' then 6
        else null
    end as taxo6,
    case
        when hc_prvdr_txnmy_cd_7 is not null
        and hc_prvdr_txnmy_cd_7 <> ' ' then 7
        else null
    end as taxo7,
    case
        when hc_prvdr_txnmy_cd_8 is not null
        and hc_prvdr_txnmy_cd_8 <> ' ' then 8
        else null
    end as taxo8,
    case
        when hc_prvdr_txnmy_cd_9 is not null
        and hc_prvdr_txnmy_cd_9 <> ' ' then 9
        else null
    end as taxo9,
    case
        when hc_prvdr_txnmy_cd_10 is not null
        and hc_prvdr_txnmy_cd_10 <> ' ' then 10
        else null
    end as taxo10,
    case
        when hc_prvdr_txnmy_cd_11 is not null
        and hc_prvdr_txnmy_cd_11 <> ' ' then 11
        else null
    end as taxo11,
    case
        when hc_prvdr_txnmy_cd_12 is not null
        and hc_prvdr_txnmy_cd_12 <> ' ' then 12
        else null
    end as taxo12,
    case
        when hc_prvdr_txnmy_cd_13 is not null
        and hc_prvdr_txnmy_cd_13 <> ' ' then 13
        else null
    end as taxo13,
    case
        when hc_prvdr_txnmy_cd_14 is not null
        and hc_prvdr_txnmy_cd_14 <> ' ' then 14
        else null
    end as taxo14,
    case
        when hc_prvdr_txnmy_cd_15 is not null
        and hc_prvdr_txnmy_cd_15 <> ' ' then 15
        else null
    end as taxo15
from
    cms_prod.data_anltcs_prvdr_npi_data_vw