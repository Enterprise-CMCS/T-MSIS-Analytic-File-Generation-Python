import sys
sys.path.append('../../..')

from taf.RX.RX_Runner import RX_Runner
from taf.RX.RXL import RXL

rx = RX_Runner('2021-10-31')

RXL = RXL(rx)

RXL.create()

# rx.view_plan()
rx.write('RX')
