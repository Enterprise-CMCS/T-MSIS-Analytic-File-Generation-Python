import sys
sys.path.append('../../..')

from taf.IP.IP_Runner import IP_Runner
from taf.TAF_Claims import TAF_Claims


ip = IP_Runner('2021-10-31')

claims = TAF_Claims(ip, False)

claims.AWS_Claims_Family_Table_Link('tmsis', 'CIP00002', 'TMSIS_CLH_REC_IP', 'IP', 'DSCHRG_DT')

# ip.view_plan()
ip.write('IP')
