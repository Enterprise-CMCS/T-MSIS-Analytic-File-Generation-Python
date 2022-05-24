from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.ELG00016 import ELG00016

bsf = BSF_Runner('2021-11-30', False)
e = ELG00016(bsf)

e.create()

# bsf.view_plan()
bsf.write('BSF')
