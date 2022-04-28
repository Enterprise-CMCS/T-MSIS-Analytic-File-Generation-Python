from taf.BSF.BSF_Runner import BSF_Runner
from taf.BSF.TPL00002 import TPL00002

bsf = BSF_Runner('2021-11-30', False)
e = TPL00002(bsf)

e.create()

# bsf.view_plan()
bsf.write('BSF')
