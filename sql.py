import pyodbc
import pandas as pd
from sqlalchemy import create_engine

# Connection details
server = r'INSPIRON-5518\MSQLSERVER'   # your server name
database = 'vaccination'                    # replace with your database name
username = 'sa'                        # your SQL username
password = '7809'             # your SQL password

# Create the connection
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={server};'
    f'DATABASE={database};'
    'Trusted_Connection=yes;'
)

# Now create cursor from the connection
cursor = conn.cursor()
print("connected to the sql server")


# Create SQLAlchemy engine for bulk insert
engine = create_engine(
    f"mssql+pyodbc://{server}/{database}?driver=ODBC%20Driver%2017%20for%20SQL%20Server&trusted_connection=yes"
)

reported_cases_data_df = pd.read_excel(r"E:\vaccine_data\reported-cases-data.xlsx")
coverage_data_df = pd.read_excel(r"E:\vaccine_data\coverage-data.xlsx")
incidence_rate_df = pd.read_excel(r"E:\vaccine_data\incidence-rate-data.xlsx")
vaccine_introduction_df = pd.read_excel(r"E:\vaccine_data\vaccine-introduction-data.xlsx")
vaccine_schedule_df = pd.read_excel(r"E:\vaccine_data\vaccine-schedule-data.xlsx")

# Insert into SQL Server with error handling
tables_and_dfs = [
    ("reported_cases_data", reported_cases_data_df),
    ("coverage_data", coverage_data_df),
    ("incidence_rate_data", incidence_rate_df),
    ("vaccine_introduction", vaccine_introduction_df),
    ("vaccine_schedule", vaccine_schedule_df),
]

for table_name, df in tables_and_dfs:
    # Truncate long string columns to prevent truncation errors
    if 'Sourcecomment' in df.columns:
        df['Sourcecomment'] = df['Sourcecomment'].astype(str).str[:255]

    # Add similar for other potential long columns if needed
    try:
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f"Successfully inserted data into {table_name}")
    except Exception as e:
        print(f"Error inserting into {table_name}: {e}")



# Load data from SQL Server into DataFrames
print("Loading data from SQL Server into DataFrames...")

reported_cases_df = pd.read_sql("SELECT * FROM reported_cases_data", con=engine)
coverage_df = pd.read_sql("SELECT * FROM coverage_data", con=engine)
incidence_rate_df = pd.read_sql("SELECT * FROM incidence_rate_data", con=engine)
vaccine_introduction_df_loaded = pd.read_sql("SELECT * FROM vaccine_introduction", con=engine)
vaccine_schedule_df_loaded = pd.read_sql("SELECT * FROM vaccine_schedule", con=engine)

# Save DataFrames to CSV for later use
reported_cases_df.to_csv('reported_cases_data_loaded.csv', index=False)
coverage_df.to_csv('coverage_data_loaded.csv', index=False)
incidence_rate_df.to_csv('incidence_rate_data_loaded.csv', index=False)
vaccine_introduction_df_loaded.to_csv('vaccine_introduction_loaded.csv', index=False)
vaccine_schedule_df_loaded.to_csv('vaccine_schedule_loaded.csv', index=False)


print("DataFrames saved to CSV files.")

import pandas as pd

reported_cases_df = pd.read_csv('reported_cases_data_loaded.csv')
coverage_df = pd.read_csv('coverage_data_loaded.csv')
incidence_rate_df = pd.read_csv('incidence_rate_data_loaded.csv')
vaccine_introduction_df_loaded = pd.read_csv('vaccine_introduction_loaded.csv')
vaccine_schedule_df_loaded = pd.read_csv('vaccine_schedule_loaded.csv')


print(f"reported_cases_df shape: {reported_cases_df.shape}")
print("reported_cases_df head:")
print(reported_cases_df.head())

print(f"coverage_df shape: {coverage_df.shape}")
print("coverage_df head:")
print(coverage_df.head())

print(f"incidence_rate_df shape: {incidence_rate_df.shape}")
print("incidence_rate_df head:")
print(incidence_rate_df.head())

print(f"vaccine_introduction_df_loaded shape: {vaccine_introduction_df_loaded.shape}")
print("vaccine_introduction_df_loaded head:")
print(vaccine_introduction_df_loaded.head())

print(f"vaccine_schedule_df_loaded shape: {vaccine_schedule_df_loaded.shape}")
print("vaccine_schedule_df_loaded head:")
print(vaccine_schedule_df_loaded.head())


## Handle Missing Data
print('handling missing values')
# Drop rows missing key identifiers
coverage_df = coverage_df.dropna(subset=["Year", "Code", "Name"])
reported_cases_df = reported_cases_df.dropna(subset=["Year", "Code", "Disease"])
incidence_rate_df = incidence_rate_df.dropna(subset=["Year", "Code", "Disease"])
print("done handling")

# Fill numeric columns
coverage_df["Coverage"] = coverage_df["Coverage"].fillna(0)
reported_cases_df["Cases"] = reported_cases_df["Cases"].fillna(0)
incidence_rate_df["Incidence_rate"] = incidence_rate_df["Incidence_rate"].fillna(0)

print("done filling numeric values ")


# 🔹 2. Normalize Units

# Ensure coverage is always percentage (0–100)
coverage_df["Coverage"] = coverage_df["Coverage"].apply(
    lambda x: x*100 if 0 < x < 1 else x
)

# Standardize incidence rate per 100,000
incidence_rate_df["Incidence_rate"] = incidence_rate_df["Incidence_rate"].apply(
    lambda x: x*100000 if x < 1 else x
)
print("completed the normalization")

## Date Consistency

for df in [coverage_df, reported_cases_df, incidence_rate_df, vaccine_schedule_df_loaded, vaccine_introduction_df_loaded]:
    if "Year" in df.columns:
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce").fillna(0).astype(int)
        df = df[df["Year"].between(1900, 2100)]

print("completed the data consistency")

# Example Summary

print("✅ Coverage cleaned:", coverage_df.shape)
print(coverage_df.describe())

print("✅ Reported Cases cleaned:", reported_cases_df.shape)
print(reported_cases_df.describe())

print("✅ Incidence cleaned:", incidence_rate_df.shape)
print(incidence_rate_df.describe())

# Close connections
cursor.close()
conn.close()
