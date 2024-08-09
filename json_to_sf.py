import snowflake.connector as sf
import pandas as pd
import numpy as np
import snowflake
import os


# Establish a connection to Snowflake
conn = sf.connect(
    user='Type Your Credential',
    password='Type Your Credential@2000',
    account='Type Your Credential',
    warehouse='Type Your Credential',
    database='Type Your Credential',
    schema='Type Your Credential'
)

print("sf.connect Completed")

cursor = conn.cursor()
cursor.execute("SELECT current_version()")
sfResults = cursor.fetchall()
print('Snowflake Version: ' + sfResults[0][0])

# Define the table name and stage name
table_name = 'Py2'
stage_name = 'my_stage1'

# Define the JSON file path (here we need to ask the file path and file name)
json_file_path = "C:/Users/krabi/Desktop/New_Python_files/test2_file.json"

# Read the JSON file into a DataFrame
try:
    df = pd.read_json(json_file_path)
    df.replace({np.nan: None}, inplace=True)
    print(f"JSON file {json_file_path} read successfully.")
except ValueError as e:
    print(f"Error reading JSON file: {e}")
    exit(1)

# Create a Snowflake cursor object
cur = conn.cursor()

# Check if the table exists
check_table_query = f"SHOW TABLES LIKE '{table_name}'"
cur.execute(check_table_query)
table_exists = cur.fetchone() is not None

# Ask user for append or overwrite
mode = input("Enter 'append' to add data to the existing table or 'overwrite' to replace it: ").strip().lower()

# Create table if it doesn't exist
if not table_exists:
    columns = ", ".join([f'"{col}" STRING' for col in df.columns])
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    )
    """
    try:
        cur.execute(create_table_query)
        print("Table created successfully.")
    except sf.errors.ProgrammingError as e:
        print(f"Error during table creation: {e}")

# Create or replace the stage
try:
    cur.execute(f"CREATE OR REPLACE STAGE {stage_name}")
    print("Stage created/replaced successfully.")
except sf.errors.ProgrammingError as e:
    print(f"Error during stage creation: {e}")

# Save DataFrame to a temporary CSV file
csv_temp_path = 'temp_data.csv'
df.to_csv(csv_temp_path, index=False)

# Adjust the file path for the PUT command
csv_file_path = os.path.abspath(csv_temp_path).replace('\\', '/')
put_command = f"PUT 'file://{csv_file_path}' @{stage_name}"

# Execute the PUT command
try:
    cur.execute(put_command)
    print("PUT command executed successfully.")
except sf.errors.ProgrammingError as e:
    print(f"Error during PUT command: {e}")

# Execute COPY INTO command based on user choice
try:
    if mode == 'append':
        copy_into_query = f"""
        COPY INTO {table_name}
        FROM @{stage_name}
        FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"' ERROR_ON_COLUMN_COUNT_MISMATCH=false)
        ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_into_query)
        print(f"COPY INTO command executed successfully. Data loaded into Snowflake table {table_name}.")
    elif mode == 'overwrite':
        truncate_table_query = f"TRUNCATE TABLE {table_name}"
        cur.execute(truncate_table_query)
        print(f"Table {table_name} truncated successfully.")
        
        copy_into_query = f"""
        COPY INTO {table_name}
        FROM @{stage_name}
        FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY='"' ERROR_ON_COLUMN_COUNT_MISMATCH=false)
        ON_ERROR = 'CONTINUE'
        """
        cur.execute(copy_into_query)
        print(f"COPY INTO command executed successfully. Data loaded into Snowflake table {table_name}.")
    else:
        print("Invalid mode. Please enter either 'append' or 'overwrite'.")
except sf.errors.ProgrammingError as e:
    print(f"Error during COPY INTO command: {e}")

# Commit the transaction (optional)
conn.commit()

# Close cursor and connection
cur.close()
conn.close()

# Remove the temporary CSV file
os.remove(csv_temp_path)

