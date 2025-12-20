# Smoke Tests

End-to-end tests that exercise real CLI functionality without mocking.

## Running Smoke Tests

```bash
cd cli && uv run pytest tests/smoke -v
```

## Test Coverage

The smoke tests cover:
- Demo mode enable/disable workflow
- CLI commands in demo mode context
- Setup command behavior
- Error handling for invalid inputs

## Design Principles

Smoke tests should:
- ✅ Run the actual CLI in a subprocess
- ✅ Use real file I/O
- ✅ Exercise real database operations (with test database)
- ✅ Test actual user workflows
- ❌ NOT use mocks
- ❌ NOT patch internal functions

## See Also

- Unit tests in `tests/unit/` - isolated testing with mocks
