import sys
sys.path.append('../../..')

from taf.IP.IP_Runner import IP_Runner
from taf.IP.IPH import IPH

ip = IP_Runner('2021-10-31')

iph = IPH(ip)

iph.create()

# ip.view_plan()
ip.write('IP')
