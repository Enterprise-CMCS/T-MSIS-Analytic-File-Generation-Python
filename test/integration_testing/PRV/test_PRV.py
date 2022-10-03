import sys
sys.path.append('../../..')

from taf.PRV.PRV_Runner import PRV_Runner

prv = PRV_Runner('2021-10-31', '56', '00123')
prv.init()


# prv.view_plan()
prv.write('PRV')
