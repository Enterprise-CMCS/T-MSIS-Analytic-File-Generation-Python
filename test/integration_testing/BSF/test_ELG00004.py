from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.ELG00004 import ELG00004

bsf = BSF_Runner('2021-11-30', False)
e = ELG00004(bsf)

e.create()

# bsf.view_plan()
bsf.write('BSF')
