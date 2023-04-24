import csv
import re


def get_fields_list(curr_row: list):
    rows = []
    exp = []
    padzero = False
    r = re.findall('\\d+', curr_row[0])
    for num in range(int(r[0]), int(r[1]) + 1):
        if re.match(r'0\d+', r[0]) is not None:
            padzero = True
        val = repack_code(curr_row[0], str(num), padzero)
        exp.append(val)
        for field in curr_row:
            exp.append(field)
        rows.append(exp)
        exp = []

    return rows


def repack_code(code: str, str_num: str, padzero: bool):
    dps = code.replace("'", '').split("-")[0]
    strmatch = re.search('[a-zA-Z]', dps)
    if padzero:
        str_num = str('0') + str_num
    if strmatch is None:
        return str_num
    else:
        start = strmatch.start(0)
        if not len(dps[:start]) == 0 and isinstance(int(dps[:start]), int):
            repack = str(str_num) + dps[start:]
        else:
            repack = dps[:start + 1] + str(str_num)

        return repack


with open('C:\\Users\\MattFury\\Projects\\TAF\\T-MSIS-Analytic-File-Generation-Python\\CCS_services_procedures_v2021-1.csv', 'r', newline='') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(reader)
    next(reader)
    rows = []
    for row in reader:
        rows.extend(get_fields_list(row))

print(rows)
# for row in rows:
#     print(row)
