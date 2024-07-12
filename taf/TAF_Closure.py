class TAF_Closure:
    """
    Contains helper functions to facilitate TAF analysis.
    """

    def coalesce_date(colname: str, alias: str):
        """
        Helper function to coalesce null dates to a default date of 1960-01-01.
        """

        return f"coalesce({alias}.{colname}, to_date('1960-01-01')) as {colname}"

    def cleanADJSTMT_IND(colname: str, alias: str):

        return f"COALESCE(upper({alias}.{colname}), 'X') AS {colname}"

    def cleanXIX_SRVC_CTGRY_CD(colname: str, alias: str):

        return f"upper(lpad(trim({alias}.{colname}), 4, '0')) as {colname}"

    def cleanXXI_SRVC_CTGRY_CD(colname: str, alias: str):

        return f"upper(lpad(trim({alias}.{colname}), 3, '0')) as {colname}"

    def compress_dots(colname: str, alias: str):
        """
        Renames columns by removing dots from the string.
        """

        return (
            f"trim(translate(upper({alias}.{colname}), '.', '')) as {colname.lower()}"
        )

    def coalesce_tilda(colname: str, alias: str):

        return f"coalesce(upper({alias}.{colname}), '~') as {colname}"

    def cast_as_dollar(colname: str, alias: str):

        return f"cast({alias}.{colname} as decimal(13, 2)) as {colname}"

    def set_as_null(colname: str, alias: str):
        #This function requires alias parameter but does not use it.  All functions called by cleanser need 2 parameters.    
        return f"NULL as {colname}"

    def var_set_type1(var: str, upper: bool = False, lpad: int = 0, new: str = "NO"):
        """
        Helper function that acts as a template to standardize how name strings are manipulated.
        """

        result = []
        if lpad == 0:
            if upper:
                result.append(f"trim(upper({var}))")
            else:
                result.append(f"trim({var})")
        else:
            result.append(f"case when {var} is NOT NULL then")
            if upper:
                result.append(f"upper(lpad(trim({var}),{lpad},'0'))")
            else:
                result.append(f"lpad(trim({var}),{lpad},'0')")
            result.append("else NULL end")
        if new == "NO":
            result.append(f"as {var}")
        else:
            result.append(f"as {new}")
        return "\n    ".join(result)

    def var_set_type2(var: str,
                      lpad: int = 0,
                      cond1: str = "@",
                      cond2: str = "@",
                      cond3: str = "@",
                      cond4: str = "@",
                      cond5: str = "@",
                      cond6: str = "@",
                      cond7: str = "@",
                      cond8: str = "@",
                      cond9: str = "@",
                      cond10: str = "@",
                      upper: bool = False
                      ):
        """
        Helper function that acts as a template to standardize how name strings are manipulated.
        """

        if upper:
            var = var.upper()

        result = []
        result.append(f"case when {var} is NOT NULL and")
        if lpad == 0:
            result.append(f"trim({var})")
        else:
            result.append(f"lpad(trim({var}), {lpad}, '0')")

        result.append(f"in ('{cond1}'")

        if cond2 != "@":
            result.append(f", '{cond2}'")
        if cond3 != "@":
            result.append(f", '{cond3}'")
        if cond4 != "@":
            result.append(f", '{cond4}'")
        if cond5 != "@":
            result.append(f", '{cond5}'")
        if cond6 != "@":
            result.append(f", '{cond6}'")
        if cond7 != "@":
            result.append(f", '{cond7}'")
        if cond8 != "@":
            result.append(f", '{cond8}'")
        if cond9 != "@":
            result.append(f", '{cond9}'")
        if cond10 != "@":
            result.append(f", '{cond10}'")

        result.append(") then")

        if lpad > 0:
            result.append(f"lpad(trim({var}), {lpad}, '0')")
        else:
            result.append(f"trim({var})")

        result.append(f"else NULL end as {var}")

        return "\n    ".join(result)

    def var_set_type3(
        var: str,
        cond1: str = "@",
        cond2: str = "@",
        cond3: str = "@",
        cond4: str = "@",
        cond5: str = "@",
        cond6: str = "@",
        spaces: bool = True,
        new: str = "NO",
    ):
        """
        Helper function that acts as a template to standardize how name strings are manipulated.
        """

        result = []
        result.append(f"case when {var} in ('{cond1}'")

        if cond2 != "@":
            result.append(f", '{cond2}'")
        if cond3 != "@":
            result.append(f", '{cond3}'")
        if cond4 != "@":
            result.append(f", '{cond4}'")
        if cond5 != "@":
            result.append(f", '{cond5}'")
        if cond6 != "@":
            result.append(f", '{cond6}'")

        result.append(")")

        if spaces:
            result.append(f"or nullif(trim('{cond1}'),'') is NULL")

            if cond2 != "@":
                result.append(f"or nullif(trim('{cond2}'),'') is NULL")
            if cond3 != "@":
                result.append(f"or nullif(trim('{cond3}'),'') is NULL")
            if cond4 != "@":
                result.append(f"or nullif(trim('{cond4}'),'') is NULL")
            if cond5 != "@":
                result.append(f"or nullif(trim('{cond5}'),'') is NULL")
            if cond6 != "@":
                result.append(f"or nullif(trim('{cond6}'),'') is NULL")

        result.append(f"then NULL else {var} end")

        if new == "NO":
            result.append(f"as {var}")
        else:
            result.append(f"as {new}")

        return "\n    ".join(result)

    def var_set_type4(
        var: str,
        upper: bool = False,
        cond1: str = "@",
        cond2: str = "@",
        cond3: str = "@",
        cond4: str = "@",
        cond5: str = "@",
        cond6: str = "@",
        cond7: str = "@",
        cond8: str = "@",
        cond9: str = "@",
        cond10: str = "@",
    ):
        """
        Helper function that acts as a template to standardize how name strings are manipulated.
        """

        result = []

        result.append("case when")

        if upper:
            result.append(f"trim(upper({var}))")
        else:
            result.append(f"trim({var})")

        result.append(f"in ('{cond1}'")

        if cond2 != "@":
            result.append(f", '{cond2}'")
        if cond3 != "@":
            result.append(f", '{cond3}'")
        if cond4 != "@":
            result.append(f", '{cond4}'")
        if cond5 != "@":
            result.append(f", '{cond5}'")
        if cond6 != "@":
            result.append(f", '{cond6}'")
        if cond7 != "@":
            result.append(f", '{cond7}'")
        if cond8 != "@":
            result.append(f", '{cond8}'")
        if cond9 != "@":
            result.append(f", '{cond9}'")
        if cond10 != "@":
            result.append(f", '{cond10}'")

        result.append(") then")

        if upper:
            result.append(f"trim(upper({var}))")
        else:
            result.append(f"trim({var})")

        result.append(f"else NULL end as {var}")

        return "\n    ".join(result)

    def var_set_type5(
        var: str,
        lpad: int = 2,
        lowerbound: int = 1,
        upperbound: int = 10,
        multiple_condition: bool = False,
        upper: bool = False
    ):
        """
        Helper function that acts as a template to standardize how name strings are manipulated.
        """

        if upper:
            var = var.upper()

        result = []

        result.append(
            f"case when ({var} is not NULL and ((length(lpad({var},{lpad}, '0')) - coalesce(length(regexp_replace(lpad({var},{lpad}, '0'), '[0-9]{{{lpad}}}', '')), 0))) > 0) then"
        )
        result.append(
            f"   case when ({var} >= {lowerbound} and {var} <= {upperbound}) then lpad({var}, {lpad}, '0')"
        )
        result.append("    else NULL end")
        result.append("else NULL end")

        if multiple_condition:
            result.append(f"end as {var}")
        else:
            result.append(f"as {var}")

        return "\n    ".join(result)

    def var_set_type6(
        var: str,
        cond1: str = "@",
        cond2: str = "@",
        cond3: str = "@",
        cond4: str = "@",
        cond5: str = "@",
        cond6: str = "@",
        new: str = "NO",
    ):
        """
        Helper function that acts as a template to standardize how name strings are manipulated.
        """

        result = []

        result.append(f"case when {var} in ({cond1}")

        if cond2 != "@":
            result.append(f", {cond2}")
        if cond3 != "@":
            result.append(f", {cond3}")
        if cond4 != "@":
            result.append(f", {cond4}")
        if cond5 != "@":
            result.append(f", {cond5}")
        if cond6 != "@":
            result.append(f", {cond6}")

        result.append(f") then NULL else {var} end as")

        if new == "NO":
            result.append(f"{var}")
        else:
            result.append(f"{new}")

        return "\n    ".join(result)

    def var_set_proc(var: str, upper: bool = False):
        """
        Helper function for left paddiing the procedure code indicators.
        """

        if upper:
            var = var.upper()

        return f"""
            case when lpad({var},2,'0')
             in('01','02','06','07','10','11','12','13','14','15','16','17','18','19',
                '20','21','22','23','24','25','26','27','28','29','30','31','32','33',
                '34','35','36','37','38','39','40','41','42','43','44','45','46','47',
                '48','49','50','51','52','53','54','55','56','57','58','59','60','61',
                '62','63','64','65','66','67','68','69','70','71','72','73','74','75',
                '76','77','78','79','80','81','82','83','84','85','86','87')
                then lpad({var},2,'0')
            else NULL end as {var}
        """

    def var_set_ptstatus(var):
        """
        Helper function for left padding patient status.
        """

        return f"""
            case when lpad({var}, 2, '0')
             in('01','02','03','04','05','06','07','08','09',
                '20','21','30','40','41','42','43','50','51',
                '61','62','63','64','65','66','69','70','71',
                '72','81','82','83','84','85','86','87','88',
                '89','90','91','92','93','94','95')
                then lpad({var}, 2, '0')
            else NULL end as {var}
        """

    def var_set_tos(var):
        """
        Helper function for left padding TOS.
        """

        return f"""
            case when (length(lpad({var},3,'0')) - coalesce(length(regexp_replace(lpad({var},3,'0'), '[0-9]{{3}}', '')), 0)) > 0 then
                case when (
                    ({var} >= 1 and {var} <= 93) or
                    ({var} in (115, 119, 120, 121, 122, 123, 127, 131, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146)))
                then lpad({var}, 3, '0')
            end
            else NULL end as {var}
        """

    def var_set_prtype(var):
        """
        Helper function for left padding provider type.
        """

        return f"""
            case when (length(lpad({var},2,'0')) - coalesce(length(regexp_replace(lpad({var},2,'0'), '[0-9]{{2}}', '')), 0)) > 0 then
                case when ({var} >= 1 and {var} <= 57) then lpad({var}, 2, '0')
                else NULL end
            else NULL end as {var}
        """

    def var_set_spclty(var):
        """
        Helper function for left padding specialty.
        """

        return f"""
            case when (length(lpad({var},2,'0')) - coalesce(length(regexp_replace(lpad({var},2,'0'), "[0-9]{{2}}", "")), 0)) > 0 then
                case when ({var} >= 1 and {var} <= 98) then upper(lpad({var}, 2, '0'))
                    else NULL
                end
                else case when upper({var}) in ('A0','A1','A2','A3','A4','A5','A6','A7','A8','A9','B1','B2','B3','B4','B5') then upper(lpad({var}, 2, '0'))
                        else NULL
                end
            end  as {var}
        """

    def var_set_poa(var):
        """
        Helper function for setting POA.
        """

        return f"""
            case when (upper({var}) in ('Y','N','U','W','1')) then upper({var})
                else NULL
            end as {var}
        """

    def var_set_fills(
        var: str,
        cond1: str = "@",
        cond2: str = "@",
        cond3: str = "@",
        cond4: str = "@",
        cond5: str = "@",
        cond6: str = "@",
        spaces: bool = True,
        new: str = "NO",
    ):
        """
        Helper function typically used for manipulating diagnostic codes.
        """

        result = []

        result.append("case when")
        result.append(
            f"((length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond1}]+', '')), 0)) = 0"
        )

        if cond2 != "@":
            result.append(
                f"or (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond2}]+', '')), 0)) = 0"
            )
        if cond3 != "@":
            result.append(
                f"or (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond3}]+', '')), 0)) = 0"
            )
        if cond4 != "@":
            result.append(
                f"or (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond4}]+', '')), 0)) = 0"
            )
        if cond5 != "@":
            result.append(
                f"or (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond5}]+', '')), 0)) = 0"
            )
        if cond6 != "@":
            result.append(
                f"or (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond6}]+', '')), 0)) = 0"
            )

        result.append(")")
        if spaces:
            result.append(f"or nullif(trim({var}),'') is null")

        result.append("then NULL")

        result.append(f"else {var} end as")

        if new == "NO":
            result.append(f"{var}")
        else:
            result.append(f"{new}")

        return "\n    ".join(result)

    def var_set_fillpr(
        var: str,
        cond1: str = "@",
        cond2: str = "@",
        cond3: str = "@",
        cond4: str = "@",
        cond5: str = "@",
        cond6: str = "@",
        spaces: bool = True,
        new: str = "NO",
    ):
        """
        Helper function for filling procedures.
        """


        result = []

        result.append(f"case when {var} = '0.00' or")
        result.append(
            f"((length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond1}]+', '')), 0)) = 0"
        )

        if cond2 != "@":
            result.append(
                f"or (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond2}]+', '')), 0)) = 0"
            )
        if cond3 != "@":
            result.append(
                f"or (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond3}]+', '')), 0)) = 0"
            )
        if cond4 != "@":
            result.append(
                f"or (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond4}]+', '')), 0)) = 0"
            )
        if cond5 != "@":
            result.append(
                f"or (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond5}]+', '')), 0)) = 0"
            )
        if cond6 != "@":
            result.append(
                f"or (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^{cond6}]+', '')), 0)) = 0"
            )

        result.append(")")
        if spaces:
            result.append(f"or nullif(trim({var}),'') is null")

        result.append("then NULL")

        result.append(f"else {var} end as")

        if new == "NO":
            result.append(f"{var}")
        else:
            result.append(f"{new}")

        return "\n    ".join(result)

    def var_set_rsn(var):
        """
        Helper function for manipulating adjustment reason.
        """

        return f"""
            case when (length(lpad({var},3,'0')) - coalesce(length(regexp_replace(lpad({var},3,'0'), '[0-9]{{3}}', '')), 0)) > 0 then
                case when ({var} >= 1 and {var} <= 99) then upper(lpad({var},3,'0'))
                    else {var}
                end
                else {var}
            end as {var}
        """

    def var_set_taxo(
        var: str,
        cond1: str = "@",
        cond2: str = "@",
        cond3: str = "@",
        cond4: str = "@",
        cond5: str = "@",
        cond6: str = "@",
        cond7: str = "@",
        spaces: bool = True,
        new: str = "NO",
        upper: bool = False
    ):
        """
        Helper function for manipulating taxonomy.

        """

        if upper:
            var = var.upper()

        result = []

        result.append(
            f"case when (length(trim({var})) - coalesce(length(regexp_replace(trim({var}), '[^0]+', '')), 0) = 0)"
        )

        result.append(f"or {var} in ('{cond1}',")

        if cond2 != "@":
            result.append(f"'{cond2}',")
        if cond3 != "@":
            result.append(f"'{cond3}',")
        if cond4 != "@":
            result.append(f"'{cond4}',")
        if cond5 != "@":
            result.append(f"'{cond5}',")
        if cond6 != "@":
            result.append(f"'{cond6}',")
        if cond7 != "@":
            result.append(f"'{cond7}'")

        result.append(")")

        if spaces:

            result.append(f"or nullif(trim('{cond1}'),'') is NULL")

            if cond2 != "@":
                result.append(f"or nullif(trim('{cond2}'),'') is NULL")
            if cond3 != "@":
                result.append(f"or nullif(trim('{cond3}'),'') is NULL")
            if cond4 != "@":
                result.append(f"or nullif(trim('{cond4}'),'') is NULL")
            if cond5 != "@":
                result.append(f"or nullif(trim('{cond5}'),'') is NULL")
            if cond6 != "@":
                result.append(f"or nullif(trim('{cond6}'),'') is NULL")
            if cond6 != "@":
                result.append(f"or nullif(trim('{cond7}'),'') is NULL")

        result.append(f"then NULL else {var} end")

        if new == "NO":
            result.append(f"as {var}")
        else:
            result.append(f"as {new}")

        return "\n    ".join(result)

    def fix_old_dates(date_var):
        """
        For dates older than 1600-01-01, default the dates to 1599-12-31.
        """

        return f"case when ({date_var} < to_date('1600-01-01')) then to_date('1599-12-31') else {date_var} end as {date_var}"

    def set_end_dt(enddt):
        """
        For dates older than 1600-01-01, default the dates to 1599-12-31.
        For null dates, set dates to 9999-12-31.
        """

        return f"""
                    case
                        when {enddt} is null then to_date('9999-12-31')
                        when {enddt} < to_date('1600-01-01') then to_date('1599-12-31')
                        else to_date( {enddt} )
                    end
        """

    def upper_case(textst):
        """
        Uppercase the name.
        """

        return f"nullif(trim(upper({textst})),'')"

    zpad = {"XIX_SRVC_CTGRY_CD": 4, "XXI_SRVC_CTGRY_CD": 3}

    def zpad(col):
        """
        Zero pad the column name.
        """

        return "lpad(trim(col), 4, '0')"

    def zero_pad(var_cd, var_len):
        """
        Another zero pad function.
        """

        return f"""case
                     when length(trim({var_cd}))<{var_len} and length(trim({var_cd}))>0 and {var_cd} is not null
                     then lpad(trim(upper({var_cd})),{var_len},'0')
                     else nullif(trim(upper({var_cd})),'')
                   end as {var_cd}
        """

    typecast = {
        # 'NCVRD_CHRGS_AMT': decimal(13, 2)
    }


    def last_best(incol, outcol=""):
        """
        Function last_best to take the last best value (go backwards in time from month
        12 to month 1, taking the first non-missing/null value).

        Function parms:
            incol=input monthly column
            outcol=name of column to be output, where default is the same name of the
            incol
        """

        if outcol == "":
            outcol = incol

        return f"""
                coalesce(m12.{incol}, m11.{incol}, m10.{incol}, m09.{incol},
                       m08.{incol}, m07.{incol}, m06.{incol}, m05.{incol},
                       m04.{incol}, m03.{incol}, m02.{incol}, m01.{incol})

        as {outcol}"""

    def monthly_array(self, incol, outcol="", nslots="1"):
        """
        Function monthly_array to take the raw monthly columns and array into columns with _MO suffixes.

        Function parms:
            incol=input monthly column
            outcol=name of column to be output, where default is the name of the incol with _MO for each month appended as a suffix
            nslots=# of slots (used for columns like MC or waiver where we have multiple slots) - default is 1, and for those with
                    slots>1, we will add slot # before _MO suffix
        """

        z = ""
        if outcol == "":
            outcol = incol

        if nslots == "1":
            return f"""
              m01.{incol} as {outcol}_01
            , m02.{incol} as {outcol}_02
            , m03.{incol} as {outcol}_03
            , m04.{incol} as {outcol}_04
            , m05.{incol} as {outcol}_05
            , m06.{incol} as {outcol}_06
            , m07.{incol} as {outcol}_07
            , m08.{incol} as {outcol}_08
            , m09.{incol} as {outcol}_09
            , m10.{incol} as {outcol}_10
            , m11.{incol} as {outcol}_11
            , m12.{incol} as {outcol}_12"""
        else:
            for s in range(1, int(nslots) + 1):
                if nslots == "1":
                    snum = ""
                else:
                    snum = s

                if s == 1:
                    z += f"""m01.{incol}{snum} as {outcol}{snum}_01"""
                else:
                    z += f""", m01.{incol}{snum} as {outcol}{snum}_01"""

                z += f"""
                , m02.{incol}{snum} as {outcol}{snum}_02
                , m03.{incol}{snum} as {outcol}{snum}_03
                , m04.{incol}{snum} as {outcol}{snum}_04
                , m05.{incol}{snum} as {outcol}{snum}_05
                , m06.{incol}{snum} as {outcol}{snum}_06
                , m07.{incol}{snum} as {outcol}{snum}_07
                , m08.{incol}{snum} as {outcol}{snum}_08
                , m09.{incol}{snum} as {outcol}{snum}_09
                , m10.{incol}{snum} as {outcol}{snum}_10
                , m11.{incol}{snum} as {outcol}{snum}_11
                , m12.{incol}{snum} as {outcol}{snum}_12"""
            return z

    def ever_year(incol, condition="=1", raw=1, outcol="", usenulls=0, nullcond=""):
        """
        Function ever_year to look across all monthly columns and create an indicator for whether ANY of the monthly
        columns meet the given condition. The default condition is = 1.

        Function parms:
            incol=input monthly column
            condition=monthly condition to be evaulated, where default is = 1
            raw=indicator for whether the monthly variables are raw (must come from the 12 monthly files) or were created
                in an earlier subquery and will therefore have the _MO suffixes, where default = 1
            outcol=name of column to be output, where default is the name of the incol
            usenulls=indicator to determine whether to use the nullif function to compare both nulls AND another value,
                    where default is = 0
            nullcond=additional value to look for when usenulls=1
        """

        if outcol == "":
            outcol = incol

        if usenulls == 0:

            if raw == 1:

                z = f"""case when
                    m01.{incol} {condition} or
                    m02.{incol} {condition} or
                    m03.{incol} {condition} or
                    m04.{incol} {condition} or
                    m05.{incol} {condition} or
                    m06.{incol} {condition} or
                    m07.{incol} {condition} or
                    m08.{incol} {condition} or
                    m09.{incol} {condition} or
                    m10.{incol} {condition} or
                    m11.{incol} {condition} or
                    m12.{incol} {condition}"""

            if raw == 0:

                z = f"""case when
                    {incol}_01 {condition} or
                    {incol}_02 {condition} or
                    {incol}_03 {condition} or
                    {incol}_04 {condition} or
                    {incol}_05 {condition} or
                    {incol}_06 {condition} or
                    {incol}_07 {condition} or
                    {incol}_08 {condition} or
                    {incol}_09 {condition} or
                    {incol}_10 {condition} or
                    {incol}_11 {condition} or
                    {incol}_12"""

        if usenulls == 1:

            if raw == 1:

                z = f"""case when
                    nullif(m01. {incol}, {nullcond}) {condition} or
                    nullif(m02. {incol}, {nullcond}) {condition} or
                    nullif(m03. {incol}, {nullcond}) {condition} or
                    nullif(m04. {incol}, {nullcond}) {condition} or
                    nullif(m05. {incol}, {nullcond}) {condition} or
                    nullif(m06. {incol}, {nullcond}) {condition} or
                    nullif(m07. {incol}, {nullcond}) {condition} or
                    nullif(m08. {incol}, {nullcond}) {condition} or
                    nullif(m09. {incol}, {nullcond}) {condition} or
                    nullif(m10. {incol}, {nullcond}) {condition} or
                    nullif(m11. {incol}, {nullcond}) {condition} or
                    nullif(m12. {incol}, {nullcond}) {condition}"""

            if raw == 0:

                z = f"""case when
                    nullif( {incol}_01, {nullcond}) {condition} or
                    nullif( {incol}_02, {nullcond}) {condition} or
                    nullif( {incol}_03, {nullcond}) {condition} or
                    nullif( {incol}_04, {nullcond}) {condition} or
                    nullif( {incol}_05, {nullcond}) {condition} or
                    nullif( {incol}_06, {nullcond}) {condition} or
                    nullif( {incol}_07, {nullcond}) {condition} or
                    nullif( {incol}_08, {nullcond}) {condition} or
                    nullif( {incol}_09, {nullcond}) {condition} or
                    nullif( {incol}_10, {nullcond}) {condition} or
                    nullif( {incol}_11, {nullcond}) {condition} or
                    nullif( {incol}_12, {nullcond}) {condition}"""

        return f"{z} then 1 else 0 end as {outcol}"

    def any_month(incols: str, outcol, condition="=1"):
        """
        Function any_month to look at multiple columns within a month to evaluate whether ANY
        of the months meets a certain condition, and output monthly indicators.

        Function parms:
            incols=list of input columns to be evaluated, separated by spaces
            outcol=name of the output col with the indicators, with the _MO suffix appended
            condition=monthly condition to be evaulated, where default is = 1
        """

        cases = []

        for m in range(1, 13):
            colCount = 1
            mm = "{:02d}".format(m)

            z = "CASE WHEN" + " "

            for col in incols.split():
                if colCount > 1:
                    z += " " + "OR" + " "

                z += f"""m{mm}.{col}""" + " " + f"""{condition}"""

                colCount += 1

            z += " " + f"""THEN 1 ELSE 0 END AS {outcol}_{mm}"""

            cases.append(z)

        return ",".join(cases)


    def getmax(incol: str, outcol="") -> str:
        """
        Get the max of the incol (which will be a 0/1 indicator)
        """

        if not outcol:
            _outcol = incol
        else:
            _outcol = outcol

        return f"""max({incol}) as {_outcol}"""

    def sumrecs(incol: str, outcol="") -> str:
        """
        Get the sum of the incol
        """

        if not outcol:
            _outcol = incol
        else:
            _outcol = outcol

        return f"""sum({incol}) as {_outcol}"""

    def count_rec(
        condcol1="",
        cond1="=1",
        condcol2="",
        cond2="=1",
        condcol3="",
        cond3="=1",
        condcol4="",
        cond4="=1",
        outcol="",
    ):

        """
        Count the number of recs where the given column equals the given condition.
        """

        z = f"""SUM(CASE WHEN {condcol1} {cond1}
        """

        if condcol2:
            z += " " + "AND" + " "
            z += f"""{condcol2} {cond2}"""

        if condcol3:
            z += " " + "AND" + " "
            z += f"""{condcol3} {cond3}"""

        if condcol4:
            z += " " + "AND" + " "
            z += f"""{condcol4} {cond4}"""

        z += f"""
             THEN 1 ELSE 0 END) AS {outcol}
        """

        return z

    def any_rec(
        condcol1="",
        cond1="=1",
        condcol2="",
        cond2="=1",
        condcol3="",
        cond3="=1",
        condcol4="",
        cond4="=1",
        outcol="",
    ):
        """
        Create an indicator for ANY rec where the given column equals the given condition.
        """


        z = f"""MAX(CASE WHEN {condcol1} {cond1}
        """

        if condcol2:
            z += " " + " AND " + " "
            z += f"""{condcol2} {cond2}"""

        if condcol3:
            z += " " + " AND " + " "
            z += f"""{condcol3} {cond3}"""

        if condcol4:
            z += " " + " AND " + " "
            z += f"""{condcol4} {cond4}"""

        z += f"""
             THEN 1 ELSE 0 END) AS {outcol}
        """

        return z

    def sum_paid(
        condcol1="",
        cond1="=1",
        condcol2="",
        cond2="=1",
        condcol3="",
        cond3="=1",
        condcol4="",
        cond4="=1",
        paidcol="tot_mdcd_pd_amt",
        outcol="",
    ):
        """
        Sum tot_mdcd_pd_amt on the headers OR mdcd_pd_amt on the lines where the given column equals the given condition.
        """

        if not outcol:
            outcol = condcol1

        z = f"""
            SUM(CASE WHEN {condcol1} {cond1}
        """

        if condcol2:
            z += f"""
                 AND {condcol2} {cond2}
            """

        if condcol3:
            z += f"""
                 AND {condcol3} {cond3}
            """

        if condcol4:
            z += f"""
                 AND {condcol4} {cond4}
            """

        z += f"""THEN {paidcol} ELSE NULL END) as {outcol}
        """

        return z

    def misslogic(var, length):
        """
        Helper function containing msis logic.
        """

        return f"""
               {var} LIKE '8{{{length}}}'
               OR {var} LIKE '9{{{length}}}'
               OR {var} LIKE '0{{{length}}}'
               OR {var} not rlike '[(a-z)|(A-Z)|(0-9)]'
               OR {var} = '&'
               OR {var} IS NULL
        """

    passthrough = {
        "%any_month": any_month,
        "%any_rec": any_rec,
        "%count_rec": count_rec,
        "%ever_year": ever_year,
        "%fix_old_dates": fix_old_dates,
        "%getmax": getmax,
        "%last_best": last_best,
        "%monthly_array": monthly_array,
        "%set_end_dt": set_end_dt,
        "%sum_paid": sum_paid,
        "%sumrecs": sumrecs,
        "%upper_case": upper_case,
        "%zero_pad": zero_pad,
        }

    @staticmethod
    def parse(var):
        """
        Lexical Analysis for a Closure
        """

        oplen = len(var)
        i = 0
        pos = [0]
        tokens = []
        while i >= 0:
            i = var.find("%", i, oplen)
            if i >= 0:
                pos.extend([i])
                i += 1
        pos.extend([oplen])
        i = 0
        for p in pos[:-1]:
            s = var[p : pos[i + 1]]
            tokens.extend([s])
            i += 1
        conditions = []
        for t in tokens:
            t1 = t[0 : t.find("(")]
            t2 = t[t.find("(") : len(t)]
            macro = [t1, t2]
            if len(macro[0]) > 1:
                if macro[0].strip() in TAF_Closure.passthrough.keys():
                    predicate = macro[1]
                    k_pos = predicate.find(")")
                    params = predicate[1:k_pos]
                    trail = predicate[k_pos + 1 : len(predicate)]
                    args = params.split(",")
                    if len(args) == 1:
                        conditions.append(
                            TAF_Closure.passthrough[macro[0].strip()](args[0])
                        )
                    elif len(args) == 2:
                        conditions.append(
                            TAF_Closure.passthrough[macro[0].strip()](args[0], args[1])
                        )
                    elif len(args) == 3:
                        conditions.append(
                            TAF_Closure.passthrough[macro[0].strip()](
                                args[0], args[1], args[2]
                            )
                        )
                    conditions.append(trail)
                    m = 2
                    while m < len(macro):
                        conditions.append(
                            str(macro[m]).format(**TAF_Closure.passthrough)
                        )
                        m += 1
                else:
                    conditions.append(str(t).format(**TAF_Closure.passthrough))
            else:
                conditions.append(str(t).format(**TAF_Closure.passthrough))

        return "\n".join(conditions)


# -----------------------------------------------------------------------------
# CC0 1.0 Universal

# Statement of Purpose

# The laws of most jurisdictions throughout the world automatically confer
# exclusive Copyright and Related Rights (defined below) upon the creator and
# subsequent owner(s) (each and all, an "owner") of an original work of
# authorship and/or a database (each, a "Work").

# Certain owners wish to permanently relinquish those rights to a Work for the
# purpose of contributing to a commons of creative, cultural and scientific
# works ("Commons") that the public can reliably and without fear of later
# claims of infringement build upon, modify, incorporate in other works, reuse
# and redistribute as freely as possible in any form whatsoever and for any
# purposes, including without limitation commercial purposes. These owners may
# contribute to the Commons to promote the ideal of a free culture and the
# further production of creative, cultural and scientific works, or to gain
# reputation or greater distribution for their Work in part through the use and
# efforts of others.

# For these and/or other purposes and motivations, and without any expectation
# of additional consideration or compensation, the person associating CC0 with a
# Work (the "Affirmer"), to the extent that he or she is an owner of Copyright
# and Related Rights in the Work, voluntarily elects to apply CC0 to the Work
# and publicly distribute the Work under its terms, with knowledge of his or her
# Copyright and Related Rights in the Work and the meaning and intended legal
# effect of CC0 on those rights.

# 1. Copyright and Related Rights. A Work made available under CC0 may be
# protected by copyright and related or neighboring rights ("Copyright and
# Related Rights"). Copyright and Related Rights include, but are not limited
# to, the following:

#   i. the right to reproduce, adapt, distribute, perform, display, communicate,
#   and translate a Work

#   ii. moral rights retained by the original author(s) and/or performer(s)

#   iii. publicity and privacy rights pertaining to a person's image or likeness
#   depicted in a Work

#   iv. rights protecting against unfair competition in regards to a Work,
#   subject to the limitations in paragraph 4(a), below

#   v. rights protecting the extraction, dissemination, use and reuse of data in
#   a Work

#   vi. database rights (such as those arising under Directive 96/9/EC of the
#   European Parliament and of the Council of 11 March 1996 on the legal
#   protection of databases, and under any national implementation thereof,
#   including any amended or successor version of such directive) and

#   vii. other similar, equivalent or corresponding rights throughout the world
#   based on applicable law or treaty, and any national implementations thereof.

# 2. Waiver. To the greatest extent permitted by, but not in contravention of,
# applicable law, Affirmer hereby overtly, fully, permanently, irrevocably and
# unconditionally waives, abandons, and surrenders all of Affirmer's Copyright
# and Related Rights and associated claims and causes of action, whether now
# known or unknown (including existing as well as future claims and causes of
# action), in the Work (i) in all territories worldwide, (ii) for the maximum
# duration provided by applicable law or treaty (including future time
# extensions), (iii) in any current or future medium and for any number of
# copies, and (iv) for any purpose whatsoever, including without limitation
# commercial, advertising or promotional purposes (the "Waiver"). Affirmer makes
# the Waiver for the benefit of each member of the public at large and to the
# detriment of Affirmer's heirs and successors, fully intending that such Waiver
# shall not be subject to revocation, rescission, cancellation, termination, or
# any other legal or equitable action to disrupt the quiet enjoyment of the Work
# by the public as contemplated by Affirmer's express Statement of Purpose.

# 3. Public License Fallback. Should any part of the Waiver for any reason be
# judged legally invalid or ineffective under applicable law, then the Waiver
# shall be preserved to the maximum extent permitted taking into account
# Affirmer's express Statement of Purpose. In addition, to the extent the Waiver
# is so judged Affirmer hereby grants to each affected person a royalty-free,
# non transferable, non sublicensable, non exclusive, irrevocable and
# unconditional license to exercise Affirmer's Copyright and Related Rights in
# the Work (i) in all territories worldwide, (ii) for the maximum duration
# provided by applicable law or treaty (including future time extensions), (iii)
# in any current or future medium and for any number of copies, and (iv) for any
# purpose whatsoever, including without limitation commercial, advertising or
# promotional purposes (the "License"). The License shall be deemed effective as
# of the date CC0 was applied by Affirmer to the Work. Should any part of the
# License for any reason be judged legally invalid or ineffective under
# applicable law, such partial invalidity or ineffectiveness shall not
# invalidate the remainder of the License, and in such case Affirmer hereby
# affirms that he or she will not (i) exercise any of his or her remaining
# Copyright and Related Rights in the Work or (ii) assert any associated claims
# and causes of action with respect to the Work, in either case contrary to
# Affirmer's express Statement of Purpose.

# 4. Limitations and Disclaimers.

#   a. No trademark or patent rights held by Affirmer are waived, abandoned,
#   surrendered, licensed or otherwise affected by this document.

#   b. Affirmer offers the Work as-is and makes no representations or warranties
#   of any kind concerning the Work, express, implied, statutory or otherwise,
#   including without limitation warranties of title, merchantability, fitness
#   for a particular purpose, non infringement, or the absence of latent or
#   other defects, accuracy, or the present or absence of errors, whether or not
#   discoverable, all to the greatest extent permissible under applicable law.

#   c. Affirmer disclaims responsibility for clearing rights of other persons
#   that may apply to the Work or any use thereof, including without limitation
#   any person's Copyright and Related Rights in the Work. Further, Affirmer
#   disclaims responsibility for obtaining any necessary consents, permissions
#   or other rights required for any use of the Work.

#   d. Affirmer understands and acknowledges that Creative Commons is not a
#   party to this document and has no duty or obligation with respect to this
#   CC0 or use of the Work.

# For more information, please see
# <http://creativecommons.org/publicdomain/zero/1.0/>
