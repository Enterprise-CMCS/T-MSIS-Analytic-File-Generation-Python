import sys
sys.path.append('../../..')

from taf.APR.APR_Runner import APR_Runner

APR = APR_Runner('2021-10-31', '56', '00123')
APR.init()


APR.view_plan()
# APR.write('APR')
