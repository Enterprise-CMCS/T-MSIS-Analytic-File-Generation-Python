import sys
sys.path.append('../../..')

# import os
import subprocess

from taf.OT.OT_Runner import OT_Runner
from taf.OT.OTH import OTH

ot = OT_Runner('2021-10-31')

oth = OTH(ot)

oth.create()

# ot.view_plan()
ot.write('OT')

# subprocess.run('npx sql-formatter --indent 4 --output C:/code/tmsis/TAF/test/sql/python/OT/OTH/OTH.sql C:/code/tmsis/TAF/test/sql/python/OT/OTH/OTH.sql')
