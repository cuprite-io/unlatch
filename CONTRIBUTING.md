# Contributing to Unlatch

First off, thank you for considering contributing to Unlatch! It's people like you that make it a great tool.

## Technical Philosophy

- **Lock-Free Concurrency**: Maximize read scalability by keeping lookups wait-free and avoiding locks on the hot path.
- **Cache-Line Alignment**: Bins and overflow buckets must align with 64-byte hardware cache lines to prevent false sharing and unnecessary bus traffic.
- **Zero-Allocation Lookups**: Read paths must remain allocation-free. Keep string conversions and intermediate struct instantiations off the retrieval paths.
- **Race Detector Cleanliness**: Concurrency features must be 100% compliant with Go's race detector (`go test -race`).

## Development Workflow

1. **Fork the Repo**: Create a feature branch.
2. **Local Development**:
   - Ensure your code follows `go fmt`.
   - Run existing tests: `go test -v -race ./...`
3. **Testing**: 
   - If you add a feature, add a unit test in `map_test.go`.
   - Ensure you run benchmarks (`go test -bench=. -benchmem ./...`) to verify there are no performance regressions.
4. **Pull Request**:
   - Provide a clear description of the change.
   - Ensure the CI (GitHub Actions) passes.

## Code of Conduct

Be respectful and professional. We aim to build a welcoming community for everyone.

## Reporting Bugs

Use GitHub Issues to report bugs. Provide:
- A clear description of the issue.
- Steps to reproduce.
- Environment details (Go version, OS).
