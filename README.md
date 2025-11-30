# Analytics Engine - High-Volume Data Processing

A Django-based backend system for processing large CSV files and providing real-time analytics APIs.

## Features

- **File Upload**: Accept CSV files up to 3GB with instant task queuing
- **Streaming Processing**: Memory-efficient processing using Celery workers
- **Analytics APIs**: 7 comprehensive analytics endpoints
- **Performance Tracking**: Real-time metrics and progress monitoring
- **Docker Support**: Complete containerized setup

## Quick Start

1. **Clone and Setup**

   ```bash
   git clone <repo-url>
   cd analytic-engine
   cp .env.example .env
   ```

2. **Run with Docker**

   ```bash
   docker-compose up --build
   ```

3. **Initialize Database**

   ```bash
   docker-compose exec web python src/manage.py migrate
   ```

4. **Generate Test Data**
   ```bash
   python scripts/generate_test_data.py
   ```

## API Endpoints

### Upload APIs

- `POST /api/uploads/` - Upload CSV file
- `GET /api/performance-stats/{task_id}/` - Get processing metrics

### Analytics APIs

- `GET /api/analytics/zone-leaderboard/` - Top 20 zones by performance
- `GET /api/analytics/category-distribution/` - Category percentage distribution
- `GET /api/analytics/dormant-merchants/` - Merchants with zero transactions
- `GET /api/analytics/hourly-pattern/` - 24-hour activity pattern
- `GET /api/analytics/anomalies/` - Transactions > 3 stddev above mean
- `GET /api/analytics/customer-retention/` - Repeat customer analysis
- `GET /api/analytics/full-report/` - Combined analytics report

## Architecture

- **Django + Django Ninja**: REST API framework
- **MySQL**: Primary database with optimized indexes
- **Celery + Redis**: Background task processing
- **Streaming CSV Processing**: Memory-efficient file handling
- **Docker**: Containerized deployment

## Performance Benchmarks

| File Size | Records | Processing Time | Memory Usage | Throughput |
| --------- | ------- | --------------- | ------------ | ---------- |
| 10MB      | 100K    | ~30s            | ~50MB        | 3.3K/s     |
| 100MB     | 1M      | ~5min           | ~80MB        | 3.3K/s     |
| 1GB       | 10M     | ~50min          | ~100MB       | 3.3K/s     |

## Development

1. **Local Setup**

   ```bash
   python -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Run Services**

   ```bash
   # Terminal 1: Django
   python src/manage.py runserver

   # Terminal 2: Celery
   celery -A config worker --loglevel=info
   ```

## API Documentation

Visit `http://localhost:8000/api/docs/` for interactive Swagger documentation.

## Memory Optimization

- Streaming CSV processing (never loads full file)
- Bulk database operations (1000 records per batch)
- Connection pooling
- Optimized database indexes
- 512MB Docker memory limit compliance
