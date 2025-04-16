# Bidirectional ClickHouse & Flat File Data Ingestion Tool

## Overview

This web application provides a user-friendly interface to facilitate data ingestion between a ClickHouse database and Flat Files (primarily CSV). It supports bidirectional data flow, allowing users to import data from ClickHouse to Flat Files and vice versa. The application handles JWT token-based authentication for ClickHouse as a source and enables users to select specific columns for the ingestion process. Upon completion, it reports the total number of records processed.

## Core Features

* **Bidirectional Data Ingestion:**
    * **ClickHouse to Flat File:** Exports data from selected ClickHouse tables and columns to a Flat File (CSV).
    * **Flat File to ClickHouse:** Imports data from a user-uploaded Flat File (CSV) into a new or existing ClickHouse table, with the option to select specific columns.
* **Source Selection:** Clear UI to choose between "ClickHouse" and "Flat File" as the data source.
* **ClickHouse Connection (as Source):**
    * UI for configuring ClickHouse connection parameters: Host, Port, Database, User, and JWT Token.
    * Utilizes JWT token-based authentication to connect to ClickHouse.
    * Fetches and displays a list of available tables in the selected ClickHouse database.
* **Flat File Integration:**
    * UI for uploading a local Flat File (CSV).
    * Allows specifying the delimiter for the CSV file (if implemented).
* **Schema Discovery & Column Selection:**
    * Retrieves table schema (column names) from ClickHouse.
    * Parses the header row of the uploaded Flat File to identify column names.
    * Presents column names in the UI with checkboxes for user selection.
* **Ingestion Process:**
    * Executes data transfer based on user-selected source, target, tables/files, and columns.
    * Implements efficient data handling (e.g., batching).
* **Completion Reporting:** Displays the total count of records ingested upon successful completion of the data transfer.
* **Basic Error Handling:** Provides user-friendly error messages for connection failures, authentication issues, query errors, and ingestion problems.

## Technologies Used

* **Backend:** Python (FastAPI)
* **Frontend:** React
* **ClickHouse Client Library:** `clickhouse-connect`
* **Flat File Handling:** Python's `csv` module
* **JWT Handling:** `PyJWT`
* **ClickHouse Instance:** Local Docker setup

## Setup and Installation

### Prerequisites

* Python 3.x
* Node.js with npm
* Docker
* A running ClickHouse instance
* JWT token for ClickHouse access

### Backend Setup
```bash
# Navigate to the backend directory
cd backend

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Linux/macOS
# venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt
```

### Frontend Setup
```bash
# Navigate to the frontend directory
cd frontend

# Install dependencies
npm install
```

## Configuration

### ClickHouse Connection (from UI)
* **Host:** Hostname or IP of ClickHouse
* **Port:** ClickHouse port (e.g., 8123)
* **Database:** Target database
* **User:** Username (if applicable)
* **JWT Token:** For authentication

### Flat File
* **ClickHouse to Flat File:** App generates a CSV file in a designated folder.
* **Flat File to ClickHouse:** User uploads a CSV file via the UI.

## Running the Application

### Start Backend
```bash
# From backend directory
uvicorn main:app --reload
```

### Start Frontend
```bash
# From frontend directory
npm start
```

## Usage Instructions

1. Open app in browser (e.g., http://localhost:3000)
2. Select data source: ClickHouse or Flat File
3. Configure source:
   * For ClickHouse: Enter connection info and connect
   * For Flat File: Upload CSV
4. Load tables or columns
5. Select desired columns
6. Set target (ClickHouse or file)
7. Click "Start Ingestion"
8. Monitor progress
9. View result message (records ingested or errors)

## Bonus Features (Implemented)
* None

## Optional Features (Implemented)
* None

## Technical Considerations

* **Backend Language:** Python (FastAPI) – lightweight and fast API development
* **Frontend Framework:** React – modern UI with dynamic rendering
* **ClickHouse Client:** `clickhouse-connect` – easy integration with ClickHouse
* **JWT Handling:** Secure API access using `PyJWT`
* **Data Handling:** Batched uploads/reads from ClickHouse

## Testing

1. **ClickHouse -> Flat File:** Verified with sample table and column selection
2. **Flat File -> ClickHouse:** Uploaded CSV and verified row count in DB
3. **Connection Failures:** Simulated wrong tokens and host
4. **Missing Column Selection:** Tested for warnings or prevention

## AI Tools Usage

The following prompts were used with AI assistance during development. Refer to the attached `prompts.txt` file for detailed conversations.

* Debugging `clickhouse-connect` connection with JWT
* Setting up FastAPI with Dockerized ClickHouse
* React frontend enhancements and layout suggestions
* Schema discovery logic for CSV and ClickHouse tables
* Prompts on error handling and column validation logic
