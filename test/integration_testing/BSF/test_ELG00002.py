from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.ELG00002 import ELG00002

bsf = BSF_Runner('2021-11-30')
e = ELG00002(bsf)

e.create()

bsf.view_plan()
# bsf.write('BSF')
