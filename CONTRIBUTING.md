# Contributing to Tavana

Thank you for your interest in contributing to Tavana!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/Tavana.git`
3. Create a branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Run tests: `cargo test --all`
6. Format code: `cargo fmt --all`
7. Run linter: `cargo clippy --all-targets`
8. Commit: `git commit -m "feat: your feature description"`
9. Push: `git push origin feature/your-feature`
10. Open a Pull Request

## Commit Convention

We follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` - New feature
- `fix:` - Bug fix
- `docs:` - Documentation only
- `style:` - Formatting, missing semicolons, etc.
- `refactor:` - Code change that neither fixes a bug nor adds a feature
- `perf:` - Performance improvement
- `test:` - Adding or updating tests
- `chore:` - Updating build tasks, package manager configs, etc.

## Code Style

- Follow Rust standard formatting (enforced by `cargo fmt`)
- Add tests for new features
- Update documentation for API changes
- Keep commits atomic and well-described

## Pull Request Process

1. Ensure all tests pass
2. Update README.md with details of changes if applicable
3. Update DEPLOYMENT.md if deployment process changes
4. The PR will be merged once you have approval from maintainers

## Reporting Issues

- Use GitHub Issues
- Include reproduction steps
- Provide environment details (OS, Kubernetes version, etc.)
- Include relevant logs

## Questions?

Open a GitHub Discussion or reach out to the maintainers.

