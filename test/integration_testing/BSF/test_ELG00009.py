from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.ELG00009 import ELG00009

bsf = BSF_Runner('2021-11-30', False)
e = ELG00009(bsf)

e.create()

# bsf.view_plan()
bsf.write('BSF')
