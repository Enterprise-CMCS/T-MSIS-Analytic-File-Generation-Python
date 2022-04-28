import sys
sys.path.append('../../..')

from taf.RX.RX_Runner import RX_Runner
from taf.RX.RXH import RXH

rx = RX_Runner('2021-10-31')

RXH = RXH(rx)

RXH.create()

# rx.view_plan()
rx.write('RX')
