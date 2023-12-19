
from taf.TAF_Run_Stack import TAF_Run_Stack

bsf = TAF_Run_Stack(file_type="BSF", state_cds=None, reporting_period="2023-09-01")
bsf.init_stack()

entry = bsf.get_next_run_id_tuple()
print(entry)
entry = bsf.get_next_run_id_tuple()
print(entry)

dict = bsf.get_run_id_dict()
print(dict)
