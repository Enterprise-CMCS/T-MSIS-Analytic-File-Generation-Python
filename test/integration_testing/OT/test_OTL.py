import sys
sys.path.append('../../..')

# import os
import subprocess

from taf.OT.OT_Runner import OT_Runner
from taf.OT.OTL import OTL

ot = OT_Runner('2021-10-31')

OTL = OTL(ot)

OTL.create()

# ot.view_plan()
ot.write('OT')

# subprocess.run('npx sql-formatter --indent 4 --output C:/code/tmsis/TAF/test/sql/python/OT/OTL/OTL.sql C:/code/tmsis/TAF/test/sql/python/OT/OTL/OTL.sql')
