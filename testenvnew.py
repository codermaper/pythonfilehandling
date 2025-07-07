import pandas as pd

# Sample DataFrame with date column
data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Date': ['2023-01-01', '9991-12-31', '7777-05-14'], 
        'DateNew': ['2023-01-01', '9999-12-31', '7777-05-14']}
df = pd.DataFrame(data)
print(df)

# Compare the 'Date' and 'DateNew' columns
df['Dates_Equal'] = df['Date'].astype(str) != df['DateNew'].astype(str)
print(df)

# A lambda function that adds 10 to the input
add_ten = lambda x: x + 10
print(add_ten(5))  # Output: 15


# Sample DataFrame with date columns
data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Date': ['2023-01-01', '9991-12-31', '7777-05-14'], 
        'DateNew': ['2023-01-01', '9999-12-31', '7777-05-14']}
df = pd.DataFrame(data)

# Convert the 'Date' and 'DateNew' columns to datetime
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df['DateNew'] = pd.to_datetime(df['DateNew'], errors='coerce')

# Use a lambda function to compare the 'Date' and 'DateNew' columns
df['Dates_Equal'] = df.apply(lambda row: row['Date'] == row['DateNew'], axis=1)

print(df)


# Lambda function for addition
add = lambda x, y: x + y
print(add(5, 3))  # Output: 8

# Lambda function for multiplication
multiply = lambda x, y: x * y
print(multiply(5, 3))  # Output: 15


numbers = [1, 2, 3, 4, 5]
squared = list(map(lambda x: x ** 2, numbers))
print(squared)  # Output: [1, 4, 9, 16, 25]

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
even_numbers = list(filter(lambda x: x % 2 == 0, numbers))
print(even_numbers)  # Output: [2, 4, 6, 8, 10]

points = [(2, 3), (1, 2), (4, 1), (3, 3)]
sorted_points = sorted(points, key=lambda point: point[1])
print(sorted_points)  # Output: [(4, 1), (1, 2), (2, 3), (3, 3)]

import pandas as pd

# Sample DataFrame
data = {'Name': ['Alice', 'Bob', 'Charlie'], 'Age': [24, 30, 22]}
df = pd.DataFrame(data)

# Use a lambda function to create a new column 'Age_Group'
df['Age_Group'] = df['Age'].apply(lambda x: 'Adult' if x >= 18 else 'Minor')
print(df)
# Output:
#       Name  Age Age_Group
# 0    Alice   24     Adult
# 1      Bob   30     Adult
# 2  Charlie   22     Adult

import pandas as pd

# Sample DataFrame with date columns
data = {'Name': ['Alice', 'Bob', 'Charlie'], 
        'Date': ['2023-01-01', '9991-12-31', '7777-05-14'], 
        'DateNew': ['2023-01-01', '9999-12-31', '7777-05-14']}
df = pd.DataFrame(data)

# Convert the 'Date' and 'DateNew' columns to datetime
df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
df['DateNew'] = pd.to_datetime(df['DateNew'], errors='coerce')

# Use a lambda function to compare the 'Date' and 'DateNew' columns
df['Dates_Equal'] = df.apply(lambda row: row['Date'] == row['DateNew'], axis=1)

print(df)
# Output:
#       Name       Date    DateNew  Dates_Equal
# 0    Alice 2023-01-01 2023-01-01         True
# 1      Bob 9991-12-31 9999-12-31        False
# 2  Charlie 7777-05-14 7777-05-14         True

