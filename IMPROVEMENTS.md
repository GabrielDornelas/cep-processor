# Future Improvements

This document lists potential improvements and enhancements for the CEP Processor project. These are suggestions for future development and are not currently implemented.

## Code Quality & Tooling

### Linter and Code Formatter

- **Linter Integration**: Integrate a linter (e.g., `flake8`, `pylint`) and code formatter (e.g., `black`, `isort`) into the development workflow.
- **Pre-commit Hooks**: Set up pre-commit hooks to automatically run linting and formatting before commits, ensuring code style consistency across the project.
- **CI/CD Integration**: Include linting and formatting checks in the CI/CD pipeline (if implemented) to catch style issues early.

### Code Organization

- **Import Optimization**: Optimize imports in `src/queue/queue_manager.py` where `src.storage.models.CEP` is imported inside `process_single_cep`. It's preferable to import at the top of the file for clarity and to avoid conditional or repeated imports.
- **Consistency in Data Handling**: The `process_single_cep` function constructs a `CEP` object manually, while `DatabaseManager.save_cep` expects a dictionary (`viacep_data`). It would be more consistent if `process_single_cep` passes the raw `viacep_data` to `db_manager.save_cep`, which already has the `from_viacep_response` logic.

## CI/CD

### Continuous Integration/Continuous Deployment

- **Pipeline Implementation**: Implement a CI/CD pipeline (e.g., GitHub Actions) to automate:
  - Test execution on every push or pull request
  - Linting and code quality checks
  - Docker image building and validation
  - Automated deployment (if applicable)
- **Benefits**: This would improve code quality, catch issues early, and speed up the delivery process.

## Monitoring & Observability

### Metrics and Monitoring

- **Monitoring Tools**: Integrate monitoring tools (e.g., Prometheus, Grafana) to collect system performance metrics:
  - Scraping time and throughput
  - Queue processing rate
  - API error rates and response times
  - Database query performance
  - System resource usage
- **Alerting**: Set up alerts for critical issues (e.g., high error rates, queue backlog, API failures).

## Documentation

### API Documentation

- **OpenAPI/Swagger**: If the Lambda function is exposed via API Gateway, create OpenAPI/Swagger documentation for the API endpoints.
- **Benefits**: This would make it easier for developers to understand and integrate with the API.

## Performance & Optimization

### Database Optimization

- **Connection Pooling**: Ensure proper connection pooling configuration in SQLAlchemy to optimize database resource usage.
- **Query Optimization**: The CEP model already includes indexes (`idx_cep_uf`, `idx_cep_localidade`, `idx_cep_created_at`), which is crucial for query performance. Consider adding more indexes based on common query patterns.

### Scraping Optimization

- **Duplicate Prevention**: Although `WebScraper` uses a `set` for `collected_ceps`, ensure that the navigation logic doesn't revisit already processed pages. This can optimize scraping in large-scale scenarios.
- **Configurable Scraping**: The `base_url` of the scraper is currently hardcoded for São Paulo. While this matches the current scope, making it configurable via environment variable or command-line argument would make the scraper more generic for other states/cities.

## Security

### Secrets Management

- **Secrets Manager**: For production environments, consider using a secrets manager (e.g., AWS Secrets Manager, HashiCorp Vault) for credentials instead of only environment variables. This adds an extra layer of security and makes credential rotation easier.

## Testing

### Test Coverage

- **Coverage Metrics**: While test coverage is good, it would be valuable to see the exact code coverage percentage (`pytest --cov=src`) to identify any gaps.
- **Integration Tests**: Add integration tests for the complete flow using Docker Compose to validate the interaction between different services (PostgreSQL, RabbitMQ, application).
- **Benefits**: Integration tests would help catch issues that only appear when services interact, ensuring the entire system works correctly end-to-end.

---

## Contributing

If you'd like to contribute by implementing any of these improvements, please:

1. Create a feature branch
2. Implement the improvement with appropriate tests
3. Update documentation as needed
4. Ensure all tests pass
5. Submit a pull request

---
