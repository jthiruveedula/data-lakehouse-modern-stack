# Contributing

Thanks for your interest! Here's how to contribute.

## Development Setup

```bash
git clone https://github.com/jthiruveedula/data-lakehouse-modern-stack
cd data-lakehouse-modern-stack
make install-dev
```

## Workflow

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/your-feature`
3. Make changes with tests
4. Run `make lint test` — both must pass
5. Push and open a pull request

## Code Style

- `ruff` for linting + formatting (`make format`)
- `mypy` for type checking (`make type-check`)
- No comments unless the WHY is non-obvious

## Testing

- All new modules need unit tests in `tests/`
- Use `conftest.py` fixtures for mock Spark sessions
- Minimum 70% coverage on new code

## Commit Messages

Follow conventional commits:
```
feat: add Iceberg branch/tag support
fix: handle empty Bronze partition on first write
docs: update Trino federation example
chore: bump delta-spark to 3.1.0
```

## Questions

Open a [GitHub Issue](https://github.com/jthiruveedula/data-lakehouse-modern-stack/issues).
