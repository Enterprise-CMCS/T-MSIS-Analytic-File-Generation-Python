create table nppes_tax1 distkey(prvdr_id_chr) sortkey(prvdr_id_chr) as (
    select
        prvdr_npi,
        prvdr_id_chr,
        prvdr_clsfctn_cd
    from
        #nppes_tax0) union (select prvdr_npi, prvdr_id_chr, prvdr_clsfctn_cd from #nppes_tax0b) order by prvdr_npi;