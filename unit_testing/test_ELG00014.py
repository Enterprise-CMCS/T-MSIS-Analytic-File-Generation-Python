from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.ELG00014 import ELG00014

bsf = BSF_Runner('2021-11-30', False)
e = ELG00014(bsf)

e.create()

# bsf.view_plan()
bsf.write('BSF')
