import sys
sys.path.append('../../..')

from taf.LT.LT_Runner import LT_Runner
from taf.LT.LTH import LTH

lt = LT_Runner('2021-10-31')

iph = LTH(lt)

iph.create()

# lt.view_plan()
lt.write('LT')
