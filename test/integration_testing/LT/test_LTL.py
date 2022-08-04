import sys
sys.path.append('../../..')

from taf.LT.LT_Runner import LT_Runner
from taf.LT.LTL import LTL

lt = LT_Runner('2021-10-31')

ltl = LTL(lt)

ltl.create()

# lt.view_plan()
lt.write('LT')
