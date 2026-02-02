# CDC Pipeline: PostgreSQL to Azure AI Search

This repository contains a Proof of Concept (POC) implementation of a Change Data Capture (CDC) pipeline. It streams data record changes (INSERT, UPDATE, DELETE) from a PostgreSQL database to an Azure AI Search index in real-time.

## Architecture Architecture

The pipeline uses PostgreSQL's logical replication feature to capture changes. A Python-based consumer connects to the replication slot, decodes the Write-Ahead Log (WAL) stream, transforms the data, and pushes updates to Azure AI Search.

**Components:**
*   **Source**: PostgreSQL 15 (Dockerized) with `wal_level=logical`.
*   **Consumer**: Python script using `psycopg2` for replication protocol handling.
*   **Destination**: Azure AI Search Service.

## Prerequisites

*   Docker Engine and Docker Compose
*   Python 3.8 or higher
*   Azure Subscription with an active Azure AI Search service.

## Configuration

### Environment Variables
Create a `.env` file in the root directory (copy from `.env.example`) and configure the following:

| Variable | Description |
| :--- | :--- |
| `AZURE_SEARCH_ENDPOINT` | The URL of your Azure AI Search service (e.g., `https://<service>.search.windows.net`) |
| `AZURE_SEARCH_API_KEY` | Admin API key for the search service. |
| `DB_HOST` | Database host (default: `localhost`) |
| `DB_PORT` | Database port (default: `5432`) |
| `DB_NAME` | Database name (default: `mydb`) |
| `DB_USER` | Database user (default: `myuser`) |
| `DB_PASSWORD` | Database password (default: `mypassword`) |

## Setup & Usage

### 1. Start Support Infrastructure
Launch the PostgreSQL container using Docker Compose.
```bash
docker-compose up -d
```

### 2. Install Dependencies
Initialize a Python virtual environment and install required packages.
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Initialize Database
Run the setup script to create the `customers` table and seed initial data.
```bash
python src/db_setup.py
```

### 4. Configure Search Index
Run the setup script to define the index schema in Azure AI Search.
```bash
python src/search_setup.py
```
*Note: This script creates an index named `customers-index`.*

### 5. Start the Pipeline
Execute the main pipeline script to begin listening for replication events.
```bash
python src/pipeline.py
```
The process will run in the foreground, logging events as they are processed.

## Verification

To verify the CDC functionality:

1.  Ensure `src/pipeline.py` is running.
2.  Open a separate terminal and insert a new record into the database:
    ```bash
    docker exec -it cdc_postgres psql -U myuser -d mydb -c "INSERT INTO customers (first_name, last_name, email, city) VALUES ('Test', 'User', 'test.user@example.com', 'New York');"
    ```
3.  Observe the log output in the pipeline terminal.
4.  Verify the new document exists in your Azure AI Search index using the Azure Portal or REST API.

