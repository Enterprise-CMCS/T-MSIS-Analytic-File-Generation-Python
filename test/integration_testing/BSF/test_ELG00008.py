from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.ELG00008 import ELG00008

bsf = BSF_Runner('2021-11-30', False)
e = ELG00008(bsf)

e.create()

# bsf.view_plan()
bsf.write('BSF')
