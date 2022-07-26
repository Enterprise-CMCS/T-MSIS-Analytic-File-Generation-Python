# ---------------------------------------------------------------------------------
#
#
#
#
# ---------------------------------------------------------------------------------
class TAF_Metadata:

    # ---------------------------------------------------------------------------------
    #
    #
    #
    #
    # ---------------------------------------------------------------------------------
    XIX_SRVC_CTGRY_CD_values = [
        "001A",
        "001B",
        "001C",
        "001D",
        "002A",
        "002B",
        "002C",
        "003A",
        "003B",
        "004A",
        "004B",
        "004C",
        "005A",
        "005B",
        "005C",
        "005D",
        "006A",
        "006B",
        "0007",
        "07A1",
        "07A2",
        "07A3",
        "07A4",
        "07A5",
        "07A6",
        "0008",
        "009A",
        "009B",
        "0010",
        "0011",
        "0012",
        "0013",
        "0014",
        "0015",
        "0016",
        "017A",
        "017B",
        "17C1",
        "017D",
        "018A",
        "18A1",
        "18A2",
        "18A3",
        "18A4",
        "18A5",
        "18B1",
        "18B2",
        "018C",
        "018D",
        "018E",
        "019A",
        "019B",
        "019C",
        "019D",
        "0022",
        "023A",
        "023B",
        "024A",
        "024B",
        "0025",
        "0026",
        "0027",
        "0028",
        "0029",
        "0030",
        "0031",
        "0032",
        "0033",
        "0034",
        "034A",
        "0035",
        "0036",
        "0037",
        "0038",
        "0039",
        "0040",
        "0041",
        "0042",
        "0043",
        "0044",
        "0045",
        "0046",
        "46A1",
        "46A2",
        "46A3",
        "46A4",
        "46A5",
        "46A6",
        "046B",
        "0049",
        "0050",
    ]

    XXI_SRVC_CTGRY_CD_values = [
        "01A",
        "01B",
        "01C",
        "01D",
        "002",
        "003",
        "004",
        "005",
        "006",
        "007",
        "008",
        "08A",
        "009",
        "010",
        "011",
        "012",
        "013",
        "014",
        "015",
        "016",
        "017",
        "018",
        "019",
        "020",
        "021",
        "022",
        "023",
        "024",
        "025",
        "031",
        "032",
        "32A",
        "32B",
        "033",
        "034",
        "035",
        "35A",
        "35B",
        "048",
        "049",
        "050",
    ]

    TMSIS_cutover_dates = {
        "01": "201401",
        "02": "201310",
        "04": "201410",
        "05": "201504",
        "06": "201510",
        "08": "201110",
        "09": "201504",
        "10": "201401",
        "11": "201401",
        "12": "201310",
        "13": "201510",
        "15": "201410",
        "16": "201510",
        "17": "201401",
        "18": "201410",
        "19": "201510",
        "20": "201301",
        "21": "201407",
        "22": "201510",
        "23": "201401",
        "24": "201401",
        "25": "201410",
        "26": "201510",
        "27": "201510",
        "28": "201510",
        "29": "201510",
        "30": "201401",
        "31": "201401",
        "32": "201401",
        "33": "201401",
        "34": "201510",
        "35": "201401",
        "36": "201507",
        "37": "201307",
        "38": "201401",
        "39": "201410",
        "40": "201410",
        "41": "201507",
        "42": "201510",
        "44": "201210",
        "45": "201407",
        "46": "201510",
        "47": "201510",
        "48": "201407",
        "49": "201510",
        "50": "201510",
        "51": "201404",
        "53": "201501",
        "54": "201510",
        "55": "201401",
        "56": "201510",
        "72": "201501",
        "78": "201701",
        "93": "201510",
        "94": "201601",
        "96": "201401",
        "97": "201401",
    }

    vs_IP_Taxo = [
        "282N00000X",
        "282NC2000X",
        "282NC0060X",
        "282NR1301X",
        "282NW0100X",
        "286500000X",
        "2865M2000X",
        "2865X1600X",
        "282J00000X",
        "284300000X",
        "273100000X",
    ]

    vs_NF_Taxo = ["311500000X", "313M00000X", "314000000X", "3140N1450X", "275N00000X"]

    vs_ICF_Taxo = ["315P00000X", "310500000X"]

    vs_Othr_Res_Taxo = [
        "385H00000X",
        "385HR2050X",
        "385HR2055X",
        "385HR2060X",
        "385HR2065X",
        "320900000X",
        "320800000X",
        "323P00000X",
        "322D00000X",
        "320600000X",
        "320700000X",
        "324500000X",
        "3245S0500X",
        "281P00000X",
        "281PC2000X",
        "282E00000X",
        "283Q00000X",
        "283X00000X",
        "283XC2000X",
        "273R00000X",
        "273Y00000X",
        "276400000X",
        "310400000X",
        "3104A0625X",
        "3104A0630X",
        "311Z00000X",
        "311ZA0620X",
    ]

    vs_Othr_HCBS_Proc_cd = [
        "T1019",
        "T1020",
        "S5125",
        "S5126",
        "T0005",
        "T1028",
        "S5100",
        "S5101",
        "S5102",
        "S5105",
        "S5120",
        "S5121",
        "S5130",
        "S5131",
        "S5135",
        "S5136",
        "S5150",
        "S5151",
        "S5170",
    ]

    vs_Othr_HCBS_Taxo = [
        "04050",
        "04060",
        "06010",
        "07010",
        "08030",
        "08040",
        "08050",
        "08060",
        "09012",
    ]

    vs_HH_Proc_cd = [
        "99503",
        "99504",
        "99505",
        "99506",
        "99507",
        "99509",
        "99511",
        "99512",
        "99600",
        "99601",
        "99602",
        "G0068",
        "G0069",
        "G0070",
        "G0088",
        "G0089",
        "G0090",
        "G0151",
        "G0152",
        "G0153",
        "G0154",
        "G0155",
        "G0156",
        "G0157",
        "G0158",
        "G0159",
        "G0160",
        "G0161",
        "G0162",
        "G0163",
        "G0164",
        "G0299",
        "G0300",
        "G0490",
        "G0493",
        "G0494",
        "G0495",
        "G0496",
        "S5108",
        "S5109",
        "S5110",
        "S5111",
        "S5115",
        "S5116",
        "S5180",
        "S5181",
        "S5522",
        "S5523",
        "S9097",
        "S9098",
        "S9122",
        "S9123",
        "S9124",
        "S9127",
        "S9128",
        "S9129",
        "S9131",
        "S9474",
        "T1000",
        "T1001",
        "T1002",
        "T1003",
        "T1004",
        "T1021",
        "T1022",
        "T1030",
        "T1031",
        "T1502",
        "T1503",
    ]

    vs_HH_Rev_cd = [
        "0023",
        "056",
        "056 ",
        "0560",
        "0561",
        "0562",
        "0563",
        "0564",
        "0565",
        "0566",
        "0567",
        "0568",
        "0569",
        "057",
        "057 ",
        "0570",
        "0571",
        "0572",
        "0573",
        "0574",
        "0575",
        "0576",
        "0577",
        "0578",
        "0579",
        "058",
        "058 ",
        "0580",
        "0581",
        "0582",
        "0583",
        "0584",
        "0585",
        "0586",
        "0587",
        "0588",
        "0589",
        "059",
        "059 ",
        "0590",
        "0591",
        "0592",
        "0593",
        "0594",
        "0595",
        "0596",
        "0597",
        "0598",
        "0599",
    ]

    vs_Rad_CCS_Cat = [
        "177",
        "178",
        "179",
        "180",
        "181",
        "182",
        "183",
        "184",
        "185",
        "186",
        "187",
        "189",
        "190",
        "191",
        "192",
        "193",
        "194",
        "195",
        "196",
        "197",
        "198",
        "207",
        "208",
        "209",
        "210",
        "226",
    ]

    vs_Lab_CCS_Cat = ["205", "206", "233", "234", "235"]

    vs_DME_CCS_Cat = ["241", "242", "243"]

    vs_transp_CCS_Cat = ["239"]


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
