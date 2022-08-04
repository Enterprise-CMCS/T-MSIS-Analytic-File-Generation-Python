from taf.BSF.BSF_Metadata import BSF_Metadata
from taf.BSF.BSF_Runner import BSF_Runner

bsf = BSF_Runner('2021-11-30', False)

print(BSF_Metadata.unifySelect(bsf))
