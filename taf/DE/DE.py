from taf.DE.DE_Runner import DE_Runner
from taf.TAF import TAF


class DE(TAF):

    def __init__(self, de: DE_Runner, dtype, dval, year):

        self.de = de

        self.dtype = dtype
        self.dval = dval
        self.year = year
        self.st_fil_type = 'DE'

        self.create_initial_table()

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create(self):
        print('')

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    def create_initial_table(self):

        z = f"""
                create or replace temporary view {self.tab_no} as
                select
                    { BSF_Metadata.selectDataElements(self.tab_no, 'a') }
                    , upper(a.MSIS_IDENT_NUM) as MSIS_IDENT_NUM
                    , a.TMSIS_RPTG_PRD
                from
                    tmsis.{self._2x_segment} as a

                -- left join
                --     data_anltcs_dm_prod.state_submsn_type s
                --     on
                --         a.submtg_state_cd = s.submtg_state_cd
                --         and upper(s.fil_type) = 'ELG'

                where
                    (
                    a.TMSIS_ACTV_IND = 1
                    and (
                        {self.eff_date} <= to_date('{self.bsf.RPT_PRD}')
                        and (
                            {self.end_date} >= to_date('{self.bsf.st_dt}')
                            or {self.end_date} is NULL
                            )
                        )
                    )
                    and a.TMSIS_RPTG_PRD >= to_date('{self.bsf.st_dt}')
                    -- and (
                    --     (
                    --         upper(coalesce(s.submsn_type, 'X')) <> 'CSO'
                    --         and a.TMSIS_RPTG_PRD >= to_date('{self.bsf.st_dt}')
                    --     )
                    --     or (upper(coalesce(s.submsn_type, 'X')) = 'CSO')
                    -- )
                    and concat(a.submtg_state_cd, a.tmsis_run_id) in (
                        {self.get_combined_list()}
                    )
                    and a.msis_ident_num is not null

            """
        # limit 1000
        self.bsf.append(type(self).__name__, z)