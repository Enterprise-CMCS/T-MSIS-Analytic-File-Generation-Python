import sys
sys.path.append('../../..')

from taf.IP.IP_Runner import IP_Runner

ipr = IP_Runner('2021-10-31')

ipr.init()

# ipr.audit()
ipr.write('IP')
