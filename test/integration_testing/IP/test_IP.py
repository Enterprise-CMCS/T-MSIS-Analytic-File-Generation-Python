from taf.IP.IP import IP
from taf.IP.IP_Runner import IP_Runner

ipr = IP_Runner('2021-10-31')

ip = IP(ipr, False)

ip.AWS_Extract_Line('tmsis', 'IP', 'IP', 'CIP00003', 'TMSIS_CLL_REC_IP')

# ip.view_plan()
ipr.write('IP')
