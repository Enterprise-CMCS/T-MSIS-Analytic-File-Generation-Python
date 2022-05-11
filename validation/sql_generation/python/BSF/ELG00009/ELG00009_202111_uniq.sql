
            create or replace temporary view ELG00009_202111_uniq as

            select
                t1.submtg_state_cd
                ,t1.msis_ident_num

                
                    , t1.LCKIN_PRVDR_NUM as LCKIN_PRVDR_NUM1
                    , t1.LCKIN_PRVDR_TYPE_CODE as LCKIN_PRVDR_TYPE_CD1
                
			
                    , t2.LCKIN_PRVDR_NUM as LCKIN_PRVDR_NUM2
                    , t2.LCKIN_PRVDR_TYPE_CODE as LCKIN_PRVDR_TYPE_CD2
                
			
                    , cast(null as varchar(30)) as LCKIN_PRVDR_NUM3
                    , cast(null as varchar(2)) as LCKIN_PRVDR_TYPE_CD3
                

                , case when (t1.LCKIN_PRVDR_NUM is not null or t2.LCKIN_PRVDR_NUM is not null)
                  then 1 else 2 end as LOCK_IN_FLG

                from (select * from ELG00009_step2 where keeper=1) t1

                
                    left join (select * from ELG00009_step2 where keeper=2) t2
                         on t1.submtg_state_cd = t2.submtg_state_cd
                        and t1.msis_ident_num  = t2.msis_ident_num
                

                