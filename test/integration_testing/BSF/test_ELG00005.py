from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.ELG00005 import ELG00005

bsf = BSF_Runner('2021-11-30', False)
e = ELG00005(bsf)

e.create()

# bsf.view_plan()
bsf.write('BSF')
