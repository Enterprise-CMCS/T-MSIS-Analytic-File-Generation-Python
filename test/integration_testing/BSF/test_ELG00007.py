from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.ELG00007 import ELG00007

bsf = BSF_Runner('2021-11-30', False)
e = ELG00007(bsf)

e.create()

# bsf.view_plan()
bsf.write('BSF')
