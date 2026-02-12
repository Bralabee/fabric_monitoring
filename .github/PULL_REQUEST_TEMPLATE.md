## Description

<!-- Provide a concise summary of the changes and the motivation behind them. -->

## Type of Change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to change)
- [ ] Refactor / code cleanup
- [ ] Documentation update
- [ ] CI/CD or infrastructure change
- [ ] Configuration / schema change

## Related Issues

<!-- Link any related issues: Fixes #123, Closes #456 -->

## Testing

- [ ] Unit tests pass (`make test`)
- [ ] Linting passes (`ruff check src/`)
- [ ] Formatting verified (`ruff format --check src/`)
- [ ] Type checks pass (`mypy src/`)
- [ ] Config validation passes (`make validate-config`)
- [ ] Integration tests pass (if applicable, `pytest tests/ -m integration`)

## Data Integrity Checklist

<!-- If changes affect data pipeline or star schema -->
- [ ] `record_count` sum used for counting (not `activity_id` count)
- [ ] Dimension column names match schema (e.g., `hour_24`, not `hour`)
- [ ] Surrogate keys coerced to Int64 (`coerce_surrogate_keys()`)
- [ ] Smart Merge behavior preserved (audit log vs job history distinction)

## Security Checklist

- [ ] No secrets, tokens, or credentials in code or config
- [ ] `.env.template` updated if new env vars added
- [ ] Audit logs don't contain sensitive information

## Screenshots / Logs

<!-- If applicable, add screenshots or relevant log output -->

## Reviewer Notes

<!-- Any specific areas you'd like reviewers to focus on -->
