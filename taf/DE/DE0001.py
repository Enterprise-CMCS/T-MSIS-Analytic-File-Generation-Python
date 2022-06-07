from taf.DE.DE import DE
from taf.DE.DE_Runner import DE_Runner


class DE0001(DE):

    def __init__(self, de: DE_Runner):
        DE.__init__(self, DE, 'DE00002', 'TMSIS_PRMRY_DMGRPHC_ELGBLTY', 'PRMRY_DMGRPHC_ELE_EFCTV_DT', 'PRMRY_DMGRPHC_ELE_END_DT')

    def create(self, base_nondemo):
        z = """
        base_nondemo,
		          subcols=%nrbquote( %last_best(CTZNSHP_VRFCTN_IND)
								     %last_best(IMGRTN_STUS_CD)
								     %last_best(IMGRTN_VRFCTN_IND) 

									 ,null :: smallint as PRGNCY_FLAG_01
									 ,null :: smallint as PRGNCY_FLAG_02
									 ,null :: smallint as PRGNCY_FLAG_03
									 ,null :: smallint as PRGNCY_FLAG_04
									 ,null :: smallint as PRGNCY_FLAG_05
									 ,null :: smallint as PRGNCY_FLAG_06
									 ,null :: smallint as PRGNCY_FLAG_07
									 ,null :: smallint as PRGNCY_FLAG_08
									 ,null :: smallint as PRGNCY_FLAG_09
									 ,null :: smallint as PRGNCY_FLAG_10
									 ,null :: smallint as PRGNCY_FLAG_11
									 ,null :: smallint as PRGNCY_FLAG_12

									 ,null :: smallint as PRGNCY_FLAG_EVR

									 %monthly_array(ELGBLTY_GRP_CD)
									 %last_best(ELGBLTY_GRP_CD,outcol=ELGBLTY_GRP_CD_LTST)
									 %monthly_array(MASBOE_CD)
									 %last_best(MASBOE_CD,outcol=MASBOE_CD_LTST)
									 %last_best(CARE_LVL_STUS_CD)
									 %ever_year(DEAF_DSBL_FLAG)
									 %ever_year(BLND_DSBL_FLAG)
									 %ever_year(DFCLTY_CONC_DSBL_FLAG,outcol=DFCLTY_CNCNTRTNG_DSBL_FLAG_EVR)
									 %ever_year(DFCLTY_WLKG_DSBL_FLAG)
									 %ever_year(DFCLTY_DRSNG_BATHG_DSBL_FLAG,outcol=DFCLTY_DRSNG_BTH_DSBL_FLAG_EVR)
									 %ever_year(DFCLTY_ERRANDS_ALN_DSBL_FLAG,outcol=DFCLTY_ERNDS_ALN_DSBL_FLAG_EVR)
									 %ever_year(OTHR_DSBL_FLAG)

									 %monthly_array(CHIP_CD)
									 %last_best(CHIP_CD,outcol=CHIP_CD_LTST)
     
									 %monthly_array(STATE_SPEC_ELGBLTY_FCTR_TXT,outcol=STATE_SPEC_ELGBLTY_GRP)
									 %last_best(STATE_SPEC_ELGBLTY_FCTR_TXT,outcol=STATE_SPEC_ELGBLTY_GRP_LTST)
									 %monthly_array(DUAL_ELGBL_CD)
									 %last_best(DUAL_ELGBL_CD,outcol=DUAL_ELGBL_CD_LTST)
									                     
									 %mc_type_rank(smonth=1,emonth=2)
									                  
									 %monthly_array(RSTRCTD_BNFTS_CD)
									 %last_best(RSTRCTD_BNFTS_CD,outcol=RSTRCTD_BNFTS_CD_LTST)
									 %last_best(SSDI_IND)
									 %last_best(SSI_IND)
									 %last_best(SSI_STATE_SPLMT_STUS_CD)
									 %last_best(SSI_STUS_CD)
									 %last_best(BIRTH_CNCPTN_IND)
									 %last_best(TANF_CASH_CD)
									 %last_best(TPL_INSRNC_CVRG_IND)
									 %last_best(TPL_OTHR_CVRG_IND)	

									 %misg_enrlmt_type
									 

		              ),
                      subcols2=%nrbquote(
                                     %mc_type_rank(smonth=3,emonth=4)
                     ),
                      subcols3=%nrbquote(
                                     %mc_type_rank(smonth=5,emonth=6)
                     ),
                      subcols4=%nrbquote(
                                     %mc_type_rank(smonth=7,emonth=8)
                     ),
                      subcols5=%nrbquote(
                                     %mc_type_rank(smonth=9,emonth=10)
                     ),
                      subcols6=%nrbquote(
                                     %mc_type_rank(smonth=11,emonth=12)

                     ) );
        """

    def demographics():
        pass

    