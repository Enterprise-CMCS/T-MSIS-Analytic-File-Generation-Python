insert into
    nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd)
select
    prvdr_npi,
    prvdr_id,
    hc_prvdr_txnmy_cd_1
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_1,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_1='Y'; insert into 
    #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select prvdr_npi, prvdr_id, hc_prvdr_txnmy_cd_2 from #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_2,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_2='Y'; insert into #nppes_tax0b 
    (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd)
select
    prvdr_npi,
    prvdr_id,
    hc_prvdr_txnmy_cd_3
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_3,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_3='Y'; insert into #nppes_tax0b (prvdr_npi, 
    prvdr_id_chr,
    prvdr_clsfctn_cd
)
select
    prvdr_npi,
    prvdr_id,
    hc_prvdr_txnmy_cd_4
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_4,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_4='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, 
    prvdr_clsfctn_cd
)
select
    prvdr_npi,
    prvdr_id,
    hc_prvdr_txnmy_cd_5
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_5,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_5='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) 
select
    prvdr_npi,
    prvdr_id,
    hc_prvdr_txnmy_cd_6
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_6,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_6='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select prvdr_npi, 
    prvdr_id,
    hc_prvdr_txnmy_cd_7
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_7,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_7='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select prvdr_npi, prvdr_id, 
    hc_prvdr_txnmy_cd_8
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_8,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_8='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select prvdr_npi, prvdr_id, 
    hc_prvdr_txnmy_cd_9
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_9,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_9='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select prvdr_npi, prvdr_id, 
    hc_prvdr_txnmy_cd_10
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_10,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_10='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select prvdr_npi, prvdr_id, 
    hc_prvdr_txnmy_cd_11
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_11,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_11='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select prvdr_npi, prvdr_id, 
    hc_prvdr_txnmy_cd_12
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_12,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_12='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select prvdr_npi, prvdr_id, 
    hc_prvdr_txnmy_cd_13
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_13,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_13='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select prvdr_npi, prvdr_id, 
    hc_prvdr_txnmy_cd_14
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_14,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_14='Y'; insert into #nppes_tax0b (prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd) select prvdr_npi, prvdr_id, 
    hc_prvdr_txnmy_cd_15
from
    #nppes_tax_flags where taxo_switches > 1 and nvl(hc_prvdr_txnmy_cd_15,' ') <> ' ' and hc_prvdr_prmry_txnmy_sw_15='Y';