# Special Cases ETL

## Overview
The **Special Cases ETL** project automates the extraction, transformation, and loading (ETL) of contract data for customers with special conditions for due date modifications. The processed data is structured and cleaned to support further automation that updates contract due dates according to company policies.

## Features
- **Extracts** data from multiple Excel files.
- **Transforms** contract codes into a standardized format.
- **Generates new due dates** based on company rules.
- **Joins datasets** to enrich contract information.
- **Exports cleaned data** for automation processing.

## Requirements
Ensure you have the following installed before running the script:
- Python 3.x
- Required libraries:
  ```bash
  pip install pandas openpyxl
  ```

## Folder Structure
```plaintext
Special Cases ETL/
│-- raw_data/                     # Folder containing source Excel files
│   │-- SpecialCases.xlsx         # Contract data
│   │-- getinvoice.xlsx           # Invoice data
│   │-- dim_insurance_keys.xlsx   # Insurance category keys
│-- clean_data/                   # Folder for processed data
│-- etl_script.py                 # Main ETL script
│-- README.md                     # Project documentation
```

## How to Run
1. Ensure the required Excel files are placed in the `raw_data/` directory.
2. Execute the script:
   ```bash
   python etl_script.py
   ```
3. The cleaned data will be saved in the `clean_data/` directory.

## Code Breakdown

### 1️⃣ Setting Up
The script initializes by recording the start time and defining paths for input and output files.
```python
start = time.time()
FOLDER_FILES = '//content//drive//MyDrive//Projects//special_cases'
FOLDER_RAW_FILES = os.path.join(FOLDER_FILES, 'raw_data')
```

### 2️⃣ Function: Reading Excel Files
This function reads an Excel file given a folder path, file name, and sheet name.
```python
def read_excelFiles(folder: str, raw_file: str, sheet_name: str):
    FULL_PATH_FILE = os.path.join(folder, raw_file)
    df = pd.read_excel(FULL_PATH_FILE, sheet_name=sheet_name)
    return df
```

### 3️⃣ Function: Standardizing Contract Codes
This function ensures contract numbers are properly formatted.
```python
def fix_str_contract(contract):
    contract = str(contract).strip()
    if '-' in contract:
        partes = contract.split('-')
        partes[1] = partes[1].zfill(8 - len(partes[0]))
        return ''.join(partes)
    return contract.zfill(8)
```

### 4️⃣ Extracting & Transforming Data
The script loads the **Special Cases** dataset and formats the due date based on the next month.
```python
df_special_cases = read_excelFiles(FOLDER_RAW_FILES, 'SpecialCases.xlsx', 'Alteração de Vencimento')
actual_year = dt.date.today().year
rescheduling_month = str(dt.date.today().month + 1).zfill(2)
df_special_cases['new_date'] = df_special_cases['DATA'].astype(str).apply(lambda x: x.zfill(2)) + rescheduling_month + str(actual_year)[-2:]
df_special_cases['contract'] = df_special_cases['C+B'].apply(fix_str_contract)
```

### 5️⃣ Enriching Data
Merging the **invoice dataset** and **insurance key dataset** with the special cases data.
```python
df_getinvoice = read_excelFiles(FOLDER_RAW_FILES, 'getinvoice.xlsx', 'Planilha1')
df_special_cases = df_special_cases.merge(df_getinvoice, how='left', right_on='B+C', left_on='C+B')
dim_insurance_key = read_excelFiles(FOLDER_RAW_FILES, 'dim_insurance_keys.xlsx', 'Insurance-Category-Insured')
df_special_cases = df_special_cases.merge(dim_insurance_key, how='left', right_on='Number', left_on='Insurance Key')
```

### 6️⃣ Exporting Data
Filtered datasets are saved to the `clean_data/` folder for automation.
```python
df_special_cases.to_excel(FINAL_PATH_SPECIAL, sheet_name='data', index=False)
df_special_cases_with_pd.to_excel(FINAL_PATH_MANUAL, sheet_name='data', index=False)
```

### 7️⃣ Execution Time Tracking
The script logs execution time and confirms output file generation.
```python
end = time.time()
print(f'⏰ Execution Time: {end - start:.6f} seconds.')
```

## Expected Output
Two cleaned Excel files:
- `special_cases.xlsx`: Primary dataset for automation.
- `special_cases_manual.xlsx`: Cases requiring manual review.

## Next Steps
- Implement error handling for missing or incorrect data.
- Optimize performance for large datasets.
- Automate execution using a scheduled task or cloud function.

---
✅ **Developed for automating special contract due date updates efficiently.**

