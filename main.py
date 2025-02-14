import pandas as pd
import datetime as dt
import time
import os

# ================================================================================= STARTING
print('🏁 Starting ... \n\n')
start = time.time() 
# ================================================================================= FOLDERS & FILES PATHS
FOLDER_FILES = '//content//drive//MyDrive//Projects//special_cases'
FOLDER_RAW_FILES = os.path.join(FOLDER_FILES, 'raw_data')
FILE_PATH_SPECIAL = os.path.join(FOLDER_RAW_FILES, 'SpecialCases.xlsx')
FILE_PATH_GETINVOICE = os.path.join(FOLDER_RAW_FILES, 'getinvoice.xlsx')
FILE_PATH_INSURANCE = os.path.join(FOLDER_RAW_FILES, 'SpecialCases.xlsx')
FINAL_PATH_SPECIAL = os.path.join(FOLDER_FILES, 'clean_data//special_cases.xlsx')
FINAL_PATH_MANUAL = os.path.join(FOLDER_FILES, 'clean_data//special_cases_manual.xlsx')

# ================================================================================= FUNCTIONS
def read_excelFiles(folder: str, raw_file: str, sheet_name: str):
  FULL_PATH_FILE = os.path.join(folder, raw_file)
  df = pd.read_excel(FULL_PATH_FILE, sheet_name=sheet_name)

  return df

def fix_str_contract(contract):
    # Garantir que o valor seja uma string
    contract = str(contract).strip()  # Remove espaços extras
    # Verificar se o contrato contém o hífen
    if '-' in contract:
        partes = contract.split('-')
        # Preencher a segunda parte com zeros, garantindo 8 caracteres no total
        partes[1] = partes[1].zfill(8 - len(partes[0]))
        return ''.join(partes)
    else:
        # Caso não tenha hífen ou seja vazio, preencher com zeros
        return contract.zfill(8)

# ================================================================================= ETL PROCESS BEGINS
print('🧹 ETL PROCESS BEGINS ... \n\n')

# SPECIAL CASES SHEETS
print(f'>> 🆙 Loading the following file: \n {FILE_PATH_SPECIAL} \n')

df_special_cases = read_excelFiles(
    folder=FOLDER_RAW_FILES,
    raw_file='SpecialCases.xlsx',
    sheet_name='Alteração de Vencimento'
)

actual_year = dt.date.today().year
rescheduling_year = str(actual_year)[-2:]
actual_month = dt.date.today().month
rescheduling_month = str(actual_month + 1)
rescheduling_month = rescheduling_month.zfill(2)

df_special_cases['new_date'] = df_special_cases['DATA'].astype(str).apply(lambda x: x.zfill(2)) + rescheduling_month + rescheduling_year
df_special_cases['contract'] = df_special_cases['C+B'].apply(fix_str_contract)
df_special_cases = df_special_cases[['contract', 'new_date','C+B']].dropna()


# GET INVOICE SHEETS
print(f'>> 🆙 Loading the following file: \n {FILE_PATH_GETINVOICE} \n')

df_getinvoice = read_excelFiles(
    folder=FOLDER_RAW_FILES,
    raw_file='getinvoice.xlsx',
    sheet_name='Planilha1'
)
df_getinvoice = df_getinvoice[(df_getinvoice['Contract Code'].str.startswith('J') | df_getinvoice['Contract Code'].str.startswith('E'))]
df_getinvoice = df_getinvoice.loc[:,['B+C','Insurance Key']]

# DIMENSIONS INSURANCE CATEGORY KEYS
print(f'>> 🆙 Loading the following file: \n {FILE_PATH_INSURANCE} \n')

dim_insurance_key = read_excelFiles(
    folder=FOLDER_RAW_FILES,
    raw_file='dim_insurance_keys.xlsx',
    sheet_name='Insurance-Category-Insured'
)

df_special_cases = df_special_cases.merge(
    right=df_getinvoice,
    how = 'left',
    right_on='B+C',
    left_on='C+B',
)

df_special_cases = df_special_cases.merge(
    right=dim_insurance_key,
    how = 'left',
    right_on='Number',
    left_on='Insurance Key'
)

df_special_cases_with_pd  = df_special_cases[df_special_cases['Number'].isnull() == False]
df_special_cases = df_special_cases[df_special_cases['Number'].isna() == True]


df_special_cases.to_excel(FINAL_PATH_SPECIAL, sheet_name='data', index=False)
df_special_cases_with_pd.to_excel(FINAL_PATH_MANUAL, sheet_name='data', index=False)

# ==================================================================================== ENDING
end = time.time()  # Marca o fim do tempo

print(f"⏰ Tempo de execução: {end - start:.6f} segundos. \n\n")

print(
    "🔥 OS SEGUINTES DOCUMENTOS FORAM CRIADOS EM SEUS DEVIDOS DIRETÓRIOS:",
    f"\n• Arquivo 01: {FINAL_PATH_SPECIAL} ",
    f"\n• Arquivo 02: {FINAL_PATH_MANUAL} ",
    "\n\n End of Process!"
    )
