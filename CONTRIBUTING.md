# Contributing to spark40connect

Thank you for your interest in contributing to this project!

## How to Contribute

1. **Fork the Repository**: Create a personal fork on GitHub.
2. **Create a Feature Branch**:
   ```bash
   git checkout -b feature/my-new-feature
   ```
3. **Make Your Changes**:
   - Ensure code compiles cleanly with Java 21, Scala 2.13, and Maven 3.9+.
   - Add unit tests covering any new functionality or bug fixes.
   - Follow standard Scala and Spark Connect conventions.
4. **Run Tests Locally**:
   Ensure a local Spark Connect server is running (see [README.md](README.md#running-the-spark-connect-server-locally)), then execute:
   ```bash
   mvn clean test
   ```
   Verify all unit tests pass before opening a pull request.
5. **Commit with Conventional Commits**:
   - Use clear commit messages (e.g., `feat: ...`, `fix: ...`, `docs: ...`, `chore: ...`).
6. **Submit a Pull Request**:
   - Push your branch to GitHub and open a PR against the `main` branch.
   - Describe what changed and why.

## Reporting Issues

If you find a bug or have an idea for an enhancement, please open an issue on GitHub with:
- Operating system, Java version, and Spark version
- Full error message or stack trace
- Steps to reproduce
