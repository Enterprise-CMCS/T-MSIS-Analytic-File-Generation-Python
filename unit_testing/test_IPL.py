from taf.IP.IP_Runner import IP_Runner
from taf.IP.IPL import IPL

ip = IP_Runner('2021-10-31')

ipl = IPL(ip)

ipl.create()

# ip.view_plan()
ip.write('IP')
