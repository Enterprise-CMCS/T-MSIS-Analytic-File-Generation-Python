insert into
    #nppes_tax0 (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select distinct prvdr_npi, prvdr_id ,case when taxo_switches = 1 and position('Y' in sw_positions)=1 then nvl(hc_prvdr_txnmy_cd_1,' ') when taxo_switches = 1 and position('Y' in 
    sw_positions
) = 2 then nvl(hc_prvdr_txnmy_cd_2, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 3 then nvl(hc_prvdr_txnmy_cd_3, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 4 then nvl(hc_prvdr_txnmy_cd_4, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 5 then nvl(hc_prvdr_txnmy_cd_5, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 6 then nvl(hc_prvdr_txnmy_cd_6, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 7 then nvl(hc_prvdr_txnmy_cd_7, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 8 then nvl(hc_prvdr_txnmy_cd_8, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 9 then nvl(hc_prvdr_txnmy_cd_9, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 10 then nvl(hc_prvdr_txnmy_cd_10, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 11 then nvl(hc_prvdr_txnmy_cd_11, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 12 then nvl(hc_prvdr_txnmy_cd_12, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 13 then nvl(hc_prvdr_txnmy_cd_13, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 14 then nvl(hc_prvdr_txnmy_cd_14, ' ')
when taxo_switches = 1
and position('Y' in sw_positions) = 15 then nvl(hc_prvdr_txnmy_cd_15, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 1 then nvl(hc_prvdr_txnmy_cd_1, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 2 then nvl(hc_prvdr_txnmy_cd_2, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 3 then nvl(hc_prvdr_txnmy_cd_3, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 4 then nvl(hc_prvdr_txnmy_cd_4, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 5 then nvl(hc_prvdr_txnmy_cd_5, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 6 then nvl(hc_prvdr_txnmy_cd_6, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 7 then nvl(hc_prvdr_txnmy_cd_7, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 8 then nvl(hc_prvdr_txnmy_cd_8, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 9 then nvl(hc_prvdr_txnmy_cd_9, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 10 then nvl(hc_prvdr_txnmy_cd_10, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 11 then nvl(hc_prvdr_txnmy_cd_11, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 12 then nvl(hc_prvdr_txnmy_cd_12, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 13 then nvl(hc_prvdr_txnmy_cd_13, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 14 then nvl(hc_prvdr_txnmy_cd_14, ' ')
when taxo_switches = 0
and taxo_cnt = 1
and position('X' in cd_positions) = 15 then nvl(hc_prvdr_txnmy_cd_15, ' ')
else null
end as selected_txnmy_cd
from
    #nppes_tax_flags where taxo_switches = 1 or 
    (
        taxo_switches = 0
        and taxo_cnt = 1
    )