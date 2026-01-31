# Quality Assurance Guide - CNPJ Receita Federal ETL

## Overview
This document provides comprehensive guidance for maintaining code quality, testing standards, and CI/CD best practices for the CNPJ Receita Federal ETL pipeline project.

## Current Quality Infrastructure

### Testing Framework
- **Framework**: pytest with custom markers for integration tests
- **Coverage**: Configured with `--cov=src --cov-report=term-missing`
- **Markers**: 
  - `integration`: Tests requiring live PostgreSQL database
  - Default tests run without integration marker for faster CI

### Code Quality Tools
- **Linting**: Ruff with E, F rules (ignoring E501 line length)
- **Type Checking**: MyPy for static type analysis
- **Formatting**: Black with 100 character line length
- **Pre-commit Hooks**: Automated quality checks before commits

### CI/CD Pipeline
Three-stage validation:
1. **Test Stage**: Unit tests with coverage reporting
2. **Lint Stage**: Code quality checks (ruff, mypy)
3. **Integration Stage**: Database integration tests with PostgreSQL service
4. **Docker Build**: Container validation

## Identified Quality Issues

### Test Coverage Gaps
Based on code analysis, potential areas needing attention:
- Error handling edge cases in downloader module
- Data validation scenarios in validation module
- Database constraint enforcement testing
- Async operation failure scenarios

### Code Quality Improvements
- Some test files use generic exception handling
- Integration tests are conditionally skipped based on environment
- Limited test coverage for edge cases in data processing

## Recommended Quality Enhancements

### 1. Test Coverage Improvements
```python
# Add to test_downloader.py
def test_download_retry_mechanism():
    """Test tenacity retry logic for network failures"""
    # Mock network failures and verify retry behavior

def test_download_integrity_check_failure():
    """Test file integrity validation failures"""
    # Verify proper error handling for corrupted downloads
```

### 2. Error Handling Standardization
```python
# Standardize error handling across modules
class CNPJProcessingError(Exception):
    """Base exception for CNPJ processing errors"""
    pass

class DownloadError(CNPJProcessingError):
    """Download-related errors"""
    pass

class ValidationError(CNPJProcessingError):
    """Data validation errors"""
    pass
```

### 3. Performance Testing
Add performance benchmarks for:
- Download throughput under various network conditions
- Database load performance with large datasets
- Memory usage during CSV processing
- Concurrent processing efficiency

### 4. Data Quality Validation
Implement comprehensive data quality checks:
- CNPJ format validation with proper masking
- Referential integrity validation
- Data consistency checks across tables
- Duplicate record detection and handling

## CI/CD Enhancement Recommendations

### Test Execution Strategy
```yaml
# Enhanced CI configuration
test_matrix:
  strategy:
    matrix:
      python-version: ["3.10", "3.11", "3.12"]
      postgres-version: ["14", "15", "16"]
  
steps:
  - name: Run tests with coverage
    run: |
      pytest -q --cov=src --cov-report=xml --cov-report=term-missing
      coverage report --fail-under=85
```

### Quality Gates
Implement stricter quality gates:
- Minimum 85% test coverage
- Zero mypy errors
- Zero ruff warnings
- All integration tests must pass

### Security Scanning
Add security validation:
- Dependency vulnerability scanning
- Code security analysis
- Container image security checks
- Secret detection in commits

## Monitoring and Alerting

### Test Result Monitoring
- Track test execution time trends
- Monitor flaky test identification
- Alert on coverage degradation
- Performance regression detection

### Code Quality Metrics
- Maintain quality dashboard
- Track technical debt indicators
- Monitor code complexity trends
- Document architectural decisions

## Best Practices for Contributors

### Before Submitting PRs
1. Run full test suite locally: `pytest -q`
2. Check code quality: `ruff check . && mypy src`
3. Verify integration tests: `PG_INTEGRATION=1 pytest -q -m integration`
4. Update documentation for changes
5. Add tests for new functionality

### Code Review Checklist
- [ ] Tests added/updated for changes
- [ ] Type hints included
- [ ] Error handling implemented
- [ ] Documentation updated
- [ ] Performance impact considered
- [ ] Security implications reviewed

### Testing Guidelines
- Write tests for all new functionality
- Include edge cases and error scenarios
- Use descriptive test names
- Mock external dependencies appropriately
- Test both success and failure paths

## Continuous Improvement

### Regular Quality Reviews
- Monthly test coverage analysis
- Quarterly dependency updates
- Annual architecture review
- Performance benchmarking cycles

### Knowledge Sharing
- Document lessons learned
- Share best practices
- Maintain troubleshooting guides
- Conduct code review sessions

## Quick Reference

### Running Quality Checks
```bash
# Full quality check
./tasks.ps1 lint
./tasks.ps1 test

# Individual tools
ruff check .
mypy src
pytest -q --cov=src

# Integration tests
PG_INTEGRATION=1 pytest -q -m integration
```

### Common Issues and Solutions
- **Test failures**: Check PostgreSQL service status
- **Linting errors**: Use `ruff check . --fix` for auto-fixes
- **Type errors**: Review mypy output and add proper type hints
- **Integration test skips**: Ensure `PG_INTEGRATION=1` environment variable

This guide should be updated regularly as the project evolves and new quality patterns emerge.