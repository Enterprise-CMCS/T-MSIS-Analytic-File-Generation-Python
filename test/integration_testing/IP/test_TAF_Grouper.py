from taf.IP.IP_Runner import IP_Runner
from taf.TAF_Grouper import TAF_Grouper

ip = IP_Runner('2021-10-31')

grouper = TAF_Grouper(ip, False)

grouper.AWS_Assign_Grouper_Data_Conv('IP', 'IP_HEADER', 'IP_LINE', 'IP', 'DSCHRG_DT')

# ip.view_plan()
ip.write('IP')
