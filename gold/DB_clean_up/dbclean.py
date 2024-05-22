import pandas as pd

# Read the CSV file
df = pd.read_csv('cleaned_data.csv')


# Remove special symbols from column names
df.columns = df.columns.str.replace('[^\w\s]', '')

# Remove special symbols from all data
df = df.replace({'\$': '', ',': ''}, regex=True)

# Replace "AWAIT" with 0
df = df.replace('AWAIT', 0)

# Convert date format to yy-MM-dd
df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%y-%m-%d')

# Save cleaned data to a new CSV file
df.to_csv('cleaned_data.csv', index=False)


