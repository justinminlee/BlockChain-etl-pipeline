# Blockchain ETL Pipeline Using Bitquery API

## Overview

This project implements an **Extract, Transform, Load (ETL) pipeline** to process blockchain data using the [Bitquery API](https://graphql.bitquery.io/). The pipeline extracts Ethereum transaction data, transforms it into a structured format, and loads it into a PostgreSQL database for analysis.

## Features

- **Data Extraction**: Retrieves Ethereum transaction data via Bitquery's GraphQL API.
- **Data Transformation**: Processes and structures the extracted data for consistency and usability.
- **Data Loading**: Inserts the transformed data into a PostgreSQL database.
- **Environment Management**: Utilises environment variables for secure configuration management.

## Prerequisites

- **Python 3.8+**: Ensure Python is installed. [Download Python](https://www.python.org/downloads/)
- **PostgreSQL**: A running PostgreSQL instance. [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- **Bitquery API Key**: Obtain an API key by registering at [Bitquery](https://bitquery.io/).

## Installation

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/your-username/blockchain-etl.git
   cd blockchain-etl
    ```
2. **Set Up a Virtual Environment**:
   ```bash
   python -m venv env
   source env/bin/activate  # On Windows: env\Scripts\activate
    ```
3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
     ```
4. **Configure Environment Variables: Create a .env file in the project root**:
   ```bash
   BITQUERY_API_KEY=your_bitquery_api_key
   DB_USER=your_db_username
   DB_PASSWORD=your_db_password
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=your_db_name
    ```
   Replace placeholder values with your actual credentials.
   
6.  **Initialise the Database: Execute the SQL script init_db.sql to create the necessary tables**:
    ```bash
    psql -U your_db_username -d your_db_name -f init_db.sql
     ```

## Usage
1. **Activate the Virtual Environment**:
   ```bash
   source env/bin/activate  # On Windows: env\Scripts\activate
    ```
2. **Run the ETL Pipeline**:
   ```bash
   python main.py
    ```
The script will extract data from the Bitquery API, transform it, and load it into the PostgreSQL database.

## Project Structure
   ```bash
    blockchain-etl/
     │
     ├── main.py                 # Main ETL script
     ├── requirements.txt        # Python dependencies
     ├── init_db.sql             # SQL script to initialise the database
     ├── .env.example            # Example environment variables file
     └── README.md               # Project documentation
   ```

## Data Model

### The ethereum_transactions table schema:

| Column Name      | Data Type  | Description                          |
|-----------------|-----------|--------------------------------------|
| `tx_hash`       | VARCHAR   | Transaction hash (Primary Key)       |
| `from_address`  | VARCHAR   | Sender's address                     |
| `to_address`    | VARCHAR   | Receiver's address                   |
| `value`        | NUMERIC   | Transaction value in Ether           |
| `gas`          | NUMERIC   | Gas used for the transaction         |
| `gas_price`    | NUMERIC   | Gas price in Wei                     |
| `block_height` | INTEGER   | Block number containing the transaction |
| `timestamp`    | TIMESTAMP | Transaction timestamp                 |

## Error Handling
The ETL pipeline includes basic error handling:
<li><strong>API Error</strong>: Logs HTTP status codes and error messages from the Bitquery API.</li>
<li><strong>Data Transformation Errors</strong>: Validates and cleans data during transformation.</li>
<li><strong>Database Errors</strong>: Catches exceptions during database operations and logs error messages.</li>

## Logging
The pipeline uses Python's built-in logging module to record events and errors. Logs are output to the console and can be configured to write to a file for persistent logging.

## Security Considerations
<li><strong>Environment Variables</strong>: Sensitive information such as API keys and database credentials are stored in environment variables loaded from a .env file using the python-dotenv package.</li>
<li><strong>.env File</strong>: Ensure the .env file is included in .gitignore to prevent sensitive information from being committed to version control.</li>

## Future Enhancements
<li><strong>Automated Scheduling</strong>: Integrate with tools like Apache Airflow to schedule regular ETL runs.
<li><strong>Data Validation</strong>: Implement data validation frameworks such as Great Expectations to ensure data quality.
<li><strong>Scalability</strong>: Optimise the ETL process to handle larger datasets and improve performance.
  
## Contributing
Contributions are welcome! Please fork the repository and create a pull request with your changes. Ensure that your code adheres to the project's coding standards and includes appropriate tests.

## License
This project is licensed under the MIT License. See the [LICENSE]() file for details.

## Acknowledgments
1. [Bitquery](https://bitquery.io/) for providing the blockchain data API.
2. [SQLAlchemy](https://www.sqlalchemy.org/) for the ORM toolkit.
3. [psycopg2](https://pypi.org/project/psycopg2/) for the PostgreSQL adapter
