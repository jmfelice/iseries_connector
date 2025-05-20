# Python Standard Library Template

This is a template repository for creating Python libraries with a standardized structure and development workflow.

## Features

- Modern Python package structure
- Complete testing setup with pytest
- CI/CD configuration with GitHub Actions
- Development tools configuration (tox, ruff, mypy)
- Documentation structure
- Build and packaging configuration

## How to Use This Template

1. Create a new repository using this template
2. Replace the following placeholders in the codebase:
   - `{{package_name}}` - Your package name
   - `{{author_name}}` - Your name
   - `{{author_email}}` - Your email
   - `{{github_username}}` - Your GitHub username
   - `{{package_description}}` - Your package description

3. Update the following files:
   - `pyproject.toml` - Update package metadata and dependencies
   - `src/{{package_name}}/` - Replace with your package code
   - `tests/` - Update with your test cases
   - `docs/` - Update with your documentation
   - `.github/workflows/` - Update CI/CD workflows if needed

## Project Structure

```
.
├── .github/              # GitHub Actions workflows
├── .vscode/             # VS Code configuration
├── docs/                # Documentation
├── src/                 # Source code
│   └── {{package_name}}/
├── tests/              # Test files
├── .editorconfig       # Editor configuration
├── .gitignore         # Git ignore rules
├── LICENSE            # License file
├── Makefile          # Development commands
├── MANIFEST.in       # Package manifest
├── pyproject.toml    # Project configuration
├── requirements_dev.txt # Development dependencies
└── tox.ini           # Tox configuration
```

## Development Workflow

1. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   .\venv\Scripts\activate   # Windows
   ```

2. Install development dependencies:
   ```bash
   pip install -r requirements_dev.txt
   ```

3. Run tests:
   ```bash
   make test
   ```

4. Run linting:
   ```bash
   make lint
   ```

5. Build documentation:
   ```bash
   make docs
   ```

## License

This template is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 