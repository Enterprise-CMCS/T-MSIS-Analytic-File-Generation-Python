from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.ELG00022 import ELG00022

bsf = BSF_Runner('2021-11-30', False)
e = ELG00022(bsf)

e.create()

# bsf.view_plan()
bsf.write('BSF')
