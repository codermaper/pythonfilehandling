import pandas as pd


# # Sample DataFrame
# data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Age': [24, 30, 22]}
# df = pd.DataFrame(data)

# Sample DataFrame with date column
data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Date': ['2023-01-01', '9998-12-31', '2022-05-15'], 
        'DateNew': ['2023-01-01', '9999-12-31', '2022-05-15']}
df = pd.DataFrame(data)
print(df)

# Convert the 'Date' column to datetime, with errors='coerce' to handle invalid dates
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df['DateNew'] = pd.to_datetime(df['DateNew'], errors='coerce')

# Compare the 'Date' and 'DateNew' columns
df['Dates_Equal'] = df['Date'].astype(str) != df['DateNew'].astype(str)



print(df)


