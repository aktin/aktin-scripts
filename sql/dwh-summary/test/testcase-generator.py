import datetime
import random

# Generates a datetime string in format of the datetime of the aktin db
# if an attribute of the datetime is not set, it will be randomly generated
def generate_datetime(year=None, month=None, day=None) -> str:
    _format = "%Y-%m-%d %H:%M:%S"
    if year is None:
        year = random.randint(2016, datetime.datetime.now().year-1)
    if month is None:
        month = random.randint(1, 12)
    if day is None:
        day = random.randint(1, 28)
    date = datetime.datetime(year, month, day)
    return date.strftime(_format)

def generate_case(enc_num: int, date: str, p21=False) -> str:
    aktin_case_id = "@"
    p21_case_id = "P21"
    case_str = f"INSERT INTO i2b2crcdata.observation_fact (encounter_num, provider_id, start_date) VALUES ({enc_num}, \'{aktin_case_id}\', \'{date}\');"
    if p21:
        case_str += f"\nINSERT INTO i2b2crcdata.observation_fact (encounter_num, provider_id, start_date) VALUES ({enc_num}, \'{p21_case_id}\', \'{date}\');"
    return case_str

def print_test_cases_c1():
    year = 2024
    enc_num = 1
    for m in range(1, 12+1):
        for i in range(6):
            print(generate_case(enc_num, generate_datetime(year, m)))
            enc_num += 1

        for i in range(4):
            print(generate_case(enc_num, generate_datetime(year, m), p21=True))
            enc_num += 1

def print_test_cases_c2():
    year = 2024
    enc_num = 1
    for m in range(1, 12 + 1):
        for i in range(10):
            print(generate_case(enc_num, generate_datetime(year, m)))
            enc_num += 1

print_test_cases_c2()