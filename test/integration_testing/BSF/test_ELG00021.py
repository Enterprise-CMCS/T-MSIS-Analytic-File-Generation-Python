from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.ELG00021 import ELG00021

bsf = BSF_Runner('2021-11-30', False)
e = ELG00021(bsf)

e.create()

# bsf.view_plan()
bsf.write('BSF')
