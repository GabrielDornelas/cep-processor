# CEP Processor

A Python system for collecting, processing, and storing Brazilian postal codes (CEPs) with controlled web scraping, queue-based API processing, and data export capabilities.

## Overview

This system collects exactly 10,000 CEPs from codigo-postal.org (São Paulo/SP), queries detailed information via ViaCEP API, processes them through a queue system to respect rate limits, stores successful results in PostgreSQL, and exports data in JSON and XML formats.

## Features

- **Web Scraping**: Controlled scraping from codigo-postal.org, navigating neighborhood by neighborhood
- **Queue System**: RabbitMQ/Redis integration for controlled API rate limiting
- **Database Storage**: PostgreSQL with SQLAlchemy ORM
- **Data Export**: JSON and XML export formats
- **Docker Support**: Full containerization with Docker Compose
- **AWS Integration**: Optional deployment with AWS Glue Jobs and Lambda functions

## Requirements

- Python 3.11 or 3.12
- PostgreSQL
- RabbitMQ or Redis (for queue system)
- Docker and Docker Compose (optional)

## Installation

1. Clone the repository:

```bash
git clone git@github.com:GabrielDornelas/cep-processor.git
cd cep-processor
```

1. Create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

1. Install dependencies:
```bash
pip install -r requirements.txt
```

**Note**: `lxml` is optional. If not installed, the code will use Python's built-in `html.parser`. For better performance, install `lxml` (see `INSTALL_WSL.md` for WSL instructions).

1. Configure environment variables:

```bash
cp .env.example .env
# Edit .env with your configuration
```

## Configuration

Create a `.env` file with the following variables:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/cep_processor

# ViaCEP API
VIACEP_BASE_URL=https://viacep.com.br/ws
RATE_LIMIT_PER_SECOND=5

# Web Scraping
SCRAPING_BASE_URL=https://codigo-postal.org/pt-br/brasil/sp/sao-paulo/
SCRAPING_DELAY=2

# Queue System
QUEUE_TYPE=rabbitmq  # or redis
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
# REDIS_URL=redis://localhost:6379/0

# Processing
MAX_CEPS=10000
REQUEST_TIMEOUT=30
RETRY_ATTEMPTS=3
LOG_LEVEL=INFO
```

## Usage

Run the main orchestrator:

```bash
python src/main.py
```

## Project Structure

```
cep-processor/
├── src/
│   ├── collectors/      # Web scraping modules
│   ├── processors/     # CSV and API processing
│   ├── queue/          # Queue management and rate limiting
│   ├── storage/        # Database models and connections
│   ├── exporters/      # JSON and XML exporters
│   ├── utils/          # Utilities (logging, etc.)
│   └── main.py         # Main orchestrator
├── tests/              # Unit tests
├── docker/             # Docker configuration
└── aws/                # AWS deployment scripts
```

## Development

Run tests:

```bash
pytest
```

## License

MIT License - see LICENSE file for details

## Author

Gabriel Dornelas
