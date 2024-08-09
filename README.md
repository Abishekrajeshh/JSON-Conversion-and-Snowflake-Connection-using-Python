# Data Conversion and Database Connections with Python

This repository provides Python scripts to convert JSON data into a DataFrame, export it as a CSV file, and upload it to a Snowflake database. The scripts are designed to be flexible, allowing for user input regarding file paths, and can handle both data manipulation and database interactions efficiently.

## Features

- **JSON to DataFrame Conversion:** Converts nested JSON files into a flattened pandas DataFrame.
- **CSV Export:** Saves the DataFrame as a CSV file with customizable file paths.
- **Duplicate Removal:** Automatically removes duplicate entries from the DataFrame to ensure data integrity.
- **Snowflake Integration:** Uploads the resulting CSV file to a Snowflake database, with options to append or overwrite existing tables.

## Prerequisites

Before using the code, make sure you have the following installed:

- **Python 3.7+**: Ensure you have Python installed. [Download Python](https://www.python.org/downloads/)
- **Pandas**: For data manipulation.
  ```bash
  pip install pandas
  ```
- **NumPy**: For numerical operations.
  ```bash
  pip install numpy
  ```
- **Snowflake Connector**: To connect and upload data to Snowflake.
  ```bash
  pip install snowflake-connector-python
  ```
- **Snowflake SQLAlchemy** (optional): If you plan to use SQLAlchemy with Snowflake.
  ```bash
  pip install snowflake-sqlalchemy
  ```

## Usage

1. Clone the repository:
   ```bash
   git clone https://github.com/Abishekrajeshh/Data-Conversion-and-Database-Connections-with-Python.git
   cd Data-Conversion-and-Database-Connections-with-Python
   ```

2. Run the main script:
   ```bash
   python main_script.py
   ```
   - Follow the on-screen prompts to enter file paths and select your desired mode (append/overwrite) for uploading data to Snowflake.

## Contributing

Contributions are welcome! Please fork the repository and create a pull request with your changes.

## License

This project is licensed under the MIT License.
