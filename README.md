# CEP Processor

A Python system for collecting, processing, and storing Brazilian postal codes (CEPs) with controlled web scraping, queue-based API processing, and data export capabilities.

## Overview

This system collects CEPs from codigo-postal.org (São Paulo/SP), queries detailed information via ViaCEP API, processes them through a queue system to respect rate limits, stores successful results in PostgreSQL, and exports data in JSON and XML formats.

## Features

- **Web Scraping**: Controlled scraping from codigo-postal.org, navigating neighborhood by neighborhood
- **Queue System**: RabbitMQ integration for controlled API rate limiting
- **Database Storage**: PostgreSQL with SQLAlchemy ORM
- **Data Export**: JSON and XML export formats
- **Docker Support**: Full containerization with Docker Compose
- **Error Handling**: Comprehensive error tracking and logging
- **Rate Limiting**: Configurable rate limits to respect API constraints

## Requirements

- Python 3.11 or 3.12
- PostgreSQL 15+
- RabbitMQ 3+
- Docker and Docker Compose (optional, recommended)

## Installation

### Using Docker (Recommended)

1. Clone the repository:

```bash
git clone git@github.com:GabrielDornelas/cep-processor.git
cd cep-processor
```

2. Start services with Docker Compose:

```bash
docker-compose up -d
```

This will start:

- PostgreSQL database
- RabbitMQ message broker
- Application container

3. The application container is ready to use. You can execute commands inside it:

```bash
docker-compose exec app python src/main.py --help
```

### Local Installation

1. Clone the repository:

```bash
git clone git@github.com:GabrielDornelas/cep-processor.git
cd cep-processor
```

2. Create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Configure environment variables:

```bash
cp env.local.template .env.local
# Edit .env.local with your configuration
```

## Configuration

Copy env.local.template as listed below and edit as desired

### Environment File Precedence

The system loads environment files in the following order (later files override earlier ones):

1. `.env` - Default values
2. `.env.local` - Local overrides
3. `.env.{ENV}` - Environment-specific overrides (e.g., `.env.staging`)

## Usage

### Basic Usage

Run the complete workflow (collect → process → export):

```bash
python src/main.py
```

### Advanced Usage

#### Skip Collection (Use Existing CSV)

```bash
python src/main.py --skip-collect
```

#### Limit Number of CEPs

```bash
# Collect only 100 CEPs
python src/main.py --max-ceps 100

# Process only first 50 CEPs from queue
python src/main.py --process-limit 50
```

#### Export Options

```bash
# Export only JSON
python src/main.py --export-format json

# Export only XML
python src/main.py --export-format xml

# Skip export
python src/main.py --no-export
```

#### Using Docker

```bash
# Run full workflow
docker-compose exec app python src/main.py

# With options
docker-compose exec app python src/main.py --max-ceps 100 --export-format json
```

## Architecture

### System Overview

```
┌─────────────────┐
│  Web Scraper    │  Collects CEPs from codigo-postal.org
│  (Collectors)   │  Saves to CSV file
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  CSV Handler    │  Validates and reads CEPs from CSV
│  (Processors)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Queue Manager  │  Publishes CEPs to RabbitMQ queue
│  (Queue)        │  Manages rate limiting
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  ViaCEP Client  │  Queries ViaCEP API for each CEP
│  (Processors)   │  Handles retries and errors
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Database       │  Stores CEP data in PostgreSQL
│  (Storage)      │  SQLAlchemy ORM
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Exporters      │  Exports to JSON/XML
│  (Exporters)    │
└─────────────────┘
```

### Component Details

#### 1. Collectors (`src/collectors/`)

- **WebScraper**: Navigates codigo-postal.org to collect CEPs
  - Supports sequential and parallel scraping
  - Respects delays between requests
  - Saves results to CSV

#### 2. Processors (`src/processors/`)

- **CSVHandler**: Reads and validates CEP CSV files
  - Validates CEP format (8 digits)
  - Uses Pandas for efficient processing
- **ViaCEPClient**: Queries ViaCEP API
  - Implements retry logic
  - Handles timeouts and errors
  - Records errors to CSV

#### 3. Queue (`src/queue/`)

- **QueueManager**: Manages RabbitMQ queue
  - Publishes CEPs to queue
  - Consumes with rate limiting
  - Handles connection errors

#### 4. Storage (`src/storage/`)

- **DatabaseManager**: PostgreSQL operations
  - Connection pooling
  - Session management
  - CRUD operations for CEPs
- **Models**: SQLAlchemy models
  - CEP model with all fields
  - Indexes for performance

#### 5. Exporters (`src/exporters/`)

- **JSONExporter**: Exports to JSON format
  - Pretty printing option
  - Metadata support
- **XMLExporter**: Exports to XML format
  - Customizable root element
  - Pretty printing option

#### 6. Utils (`src/utils/`)

- **ConfigHelper**: Environment configuration
  - Multi-environment support
  - .env file loading
- **Logger**: Structured logging
- **ErrorHandler**: Error tracking to CSV

### Data Flow

1. **Collection Phase**

   - WebScraper navigates neighborhoods
   - Extracts CEPs from pages
   - Saves to CSV file

2. **Queue Phase**

   - CSVHandler reads and validates CEPs
   - QueueManager publishes to RabbitMQ
   - Rate limiting configured

3. **Processing Phase**

   - QueueManager consumes from queue
   - ViaCEPClient queries API for each CEP
   - DatabaseManager stores results
   - Errors recorded to CSV

4. **Export Phase**
   - Exporters read from database
   - Generate JSON/XML files
   - Include metadata

## Step-by-Step Examples

### Example 1: Complete Workflow (First Time)

This example shows the complete workflow from scratch:

```bash
# 1. Start Docker services
docker-compose up -d

# 2. Wait for services to be ready (check logs)
docker-compose logs -f

# 3. Run the complete workflow
docker-compose exec app python src/main.py

# Expected output:
# ============================================================
# CEP Processor - Full Workflow
# ============================================================
# Setting up connections...
# ✓ Database connected
# ✓ RabbitMQ connected
# ✓ ViaCEP client initialized
# ============================================================
# Step 1: Collecting CEPs via web scraping
# ============================================================
# Starting web scraping...
# ... (scraping progress)
# ✓ Collected CEPs saved to: data/ceps_collected.csv
# ============================================================
# Step 2: Publishing CEPs to queue
# ============================================================
# Found 10000 valid CEPs
# ✓ Published 10000 CEPs to queue
# ============================================================
# Step 3: Processing CEPs from queue
# ============================================================
# Processing CEPs from queue...
# ... (processing progress)
# ✓ Processed 10000 CEPs
# ============================================================
# Step 4: Exporting data
# ============================================================
# ✓ JSON exported to: data/ceps_export.json
# ✓ XML exported to: data/ceps_export.xml
# ============================================================
# Workflow Summary
# ============================================================
# CEPs processed: 10000
# Total CEPs in database: 10000
# ============================================================
```

### Example 2: Using Existing CSV File

If you already have a CSV file with CEPs:

```bash
# 1. Place your CSV file in data/ directory
# File should have a 'cep' column

# 2. Run with --skip-collect flag
docker-compose exec app python src/main.py --skip-collect

# This will:
# - Skip web scraping
# - Read CEPs from existing CSV
# - Process and store in database
# - Export to JSON/XML
```

### Example 3: Processing a Small Sample

For testing or development:

```bash
# Collect and process only 10 CEPs
docker-compose exec app python src/main.py --max-ceps 10 --process-limit 10

# This is useful for:
# - Testing the system
# - Development
# - Quick validation
```

### Example 4: Export Only (No Processing)

If you just want to export existing data from the database:

```bash
# Simple one-liner
docker-compose exec app python scripts/export_only.py
```

This will export all CEPs from the database to both JSON and XML formats.

### Example 5: Custom Export Format

```bash
# Export only JSON
docker-compose exec app python src/main.py --export-format json

# Export only XML
docker-compose exec app python src/main.py --export-format xml

# Skip export entirely
docker-compose exec app python src/main.py --no-export
```

### Example 6: Local Development (Without Docker)

```bash
# 1. Start PostgreSQL and RabbitMQ locally
# Or use Docker just for these services:
docker-compose up -d postgres rabbitmq

# 2. Set environment variables
export DATABASE_URL=postgresql://cep_user:cep_password@localhost:5432/cep_processor
export RABBITMQ_URL=amqp://guest:guest@localhost:5672/

# 3. Run the application
python src/main.py --max-ceps 100
```

## Project Structure

```
cep-processor/
├── src/
│   ├── collectors/      # Web scraping modules
│   │   └── web_scraper.py
│   ├── processors/       # CSV and API processing
│   │   ├── csv_handler.py
│   │   └── viacep_client.py
│   ├── queue/           # Queue management
│   │   └── queue_manager.py
│   ├── storage/         # Database models and connections
│   │   ├── database.py
│   │   └── models.py
│   ├── exporters/       # JSON and XML exporters
│   │   ├── json_exporter.py
│   │   └── xml_exporter.py
│   ├── utils/           # Utilities
│   │   ├── config_helper.py
│   │   ├── error_handler.py
│   │   └── logger.py
│   └── main.py          # Main orchestrator
├── tests/               # Unit tests
│   ├── test_collectors.py
│   ├── test_processors.py
│   ├── test_queue.py
│   ├── test_storage.py
│   ├── test_exporters.py
│   └── ...
├── scripts/             # Utility scripts
├── data/                # Data files (CSV, JSON, XML)
├── docker-compose.yml   # Docker Compose configuration
├── Dockerfile           # Docker image definition
├── requirements.txt     # Python dependencies
└── README.md           # This file
```

## Development

### Running Tests

```bash
# Run all tests
docker-compose exec app pytest

# Run with verbose output
docker-compose exec app pytest -v

# Run specific test file
docker-compose exec app pytest tests/test_storage.py

# Run with coverage
docker-compose exec app pytest --cov=src
```

### Code Quality

The project follows Python best practices:

- Type hints throughout
- Comprehensive docstrings
- Error handling
- Structured logging
- Unit tests for all modules

### Adding New Features

1. Create feature branch
2. Implement feature with tests
3. Ensure all tests pass
4. Update documentation
5. Submit pull request

## Troubleshooting

### Database Connection Issues

```bash
# Check if PostgreSQL is running
docker-compose ps postgres

# Check logs
docker-compose logs postgres

# Test connection
docker-compose exec app python -c "
from src.storage.database import DatabaseManager
db = DatabaseManager()
print('Connected!' if db.connect() else 'Failed!')
"
```

### RabbitMQ Connection Issues

```bash
# Check if RabbitMQ is running
docker-compose ps rabbitmq

# Check logs
docker-compose logs rabbitmq

# Access RabbitMQ Management UI
# http://localhost:15672 (guest/guest)
```

### Queue Processing Stuck

```bash
# Check queue size
docker-compose exec app python -c "
from src.queue.queue_manager import QueueManager
qm = QueueManager()
qm.connect()
print(f'Queue size: {qm.get_queue_size()}')
qm.disconnect()
"

# Purge queue if needed
docker-compose exec app python -c "
from src.queue.queue_manager import QueueManager
qm = QueueManager()
qm.connect()
qm.purge_queue()
qm.disconnect()
"
```

## License

MIT License - see LICENSE file for details

## Author

Gabriel Dornelas
