# Import the pandas library, used for data processing and analysis. '# type: ignore' is used to ignore type checking warnings.
import pandas as pd  # type: ignore
# Import the sqlite3 library, used to interact with SQLite databases.
import sqlite3

# Read customer data from the 'customer.csv' file and store it in a DataFrame object named 'customers_df'.
customers_df = pd.read_csv('customer.csv')
# Read order data from the 'orders.csv' file and store it in a DataFrame object named 'orders_df'.
orders_df = pd.read_csv('orders.csv')

# Use the 'pd.merge' function to perform an inner join on 'orders_df' and 'customers_df' based on the 'CustomerID' column.
# That is, only keep the rows where the 'CustomerID' values exist in both DataFrames. The result is stored in'merged_df'.
merged_df = pd.merge(orders_df, customers_df, on='CustomerID', how='inner')

# Add a new column named 'TotalAmount' to'merged_df'. The values in this column are the result of multiplying the corresponding elements of the 'Quantity' and 'Price' columns,
# representing the total amount of each order.
merged_df['TotalAmount'] = merged_df['Quantity'] * merged_df['Price']

# Add a new column named 'Status' to'merged_df'. Determine the status by applying a lambda function to each element of the 'OrderDate' column.
# If the order date starts with '2025-03', the status is 'New'; otherwise, it is 'Old'.
merged_df['Status'] = merged_df['OrderDate'].apply(lambda d: 'New' if d.startswith('2025-03') else 'Old')

# Filter the rows in'merged_df' where the 'TotalAmount' is greater than 5000, and store these high - value orders in 'high_value_orders'.
high_value_orders = merged_df[merged_df['TotalAmount'] > 5000]

# Connect to the SQLite database named 'ecommerce.db'. If the database does not exist, a new one will be created.
conn = sqlite3.connect('ecommerce.db')

# Define an SQL query statement to create a table named 'HighValueOrders' (if the table does not exist).
# This table contains multiple columns to store order ID, customer ID, customer name, customer email, product name, quantity, price, order date, total amount, and order status respectively.
create_table_query = '''
CREATE TABLE IF NOT EXISTS HighValueOrders (
    OrderID INTEGER,
    CustomerID INTEGER,
    Name TEXT,
    Email TEXT,
    Product TEXT,
    Quantity INTEGER,
    Price REAL,
    OrderDate TEXT,
    TotalAmount REAL,
    Status TEXT
)
'''
# Execute the above SQL query statement to create the table.
conn.execute(create_table_query)

# Write the data in 'high_value_orders' to the 'HighValueOrders' table.
# 'if_exists='replace'' means that if the table already exists, delete the original table first, then create a new table and insert the data.
# 'index=False' means not to write the index column of the DataFrame to the database table.
high_value_orders.to_sql('HighValueOrders', conn, if_exists='replace', index=False)

# Execute an SQL query statement to select all rows and columns from the 'HighValueOrders' table. The result is stored in'result'.
result = conn.execute('SELECT * FROM HighValueOrders')
# Iterate through each row of the query result and print it.
for row in result.fetchall():
    print(row)

# Close the connection to the SQLite database.
conn.close()

# Print a prompt message indicating that the ETL (Extract, Transform, Load) process has been successfully completed.
print("ETL process completed successfully!")