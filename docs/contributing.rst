Contributing
===========

Thank you for your interest in contributing to the AWS Connector package! This guide will help you get started with development and submitting contributions.

Development Setup
--------------

1. Clone the repository:

.. code-block:: bash

    git clone https://github.com/yourusername/aws_connector.git
    cd aws_connector

2. Create a virtual environment:

.. code-block:: bash

    # Windows
    python -m venv venv
    .\venv\Scripts\activate

    # Linux/macOS
    python3 -m venv venv
    source venv/bin/activate

3. Install development dependencies:

.. code-block:: bash

    pip install -e ".[dev]"

4. Install pre-commit hooks:

.. code-block:: bash

    pre-commit install

Project Structure
--------------

::

    aws_connector/
    ├── src/
    │   └── aws_connector/
    │       ├── __init__.py
    │       ├── redshift.py
    │       ├── s3.py
    │       ├── aws_sso.py
    │       ├── exceptions.py
    │       └── utils.py
    ├── tests/
    │   ├── __init__.py
    │   ├── test_redshift.py
    │   ├── test_s3.py
    │   └── test_aws_sso.py
    ├── docs/
    │   ├── conf.py
    │   ├── index.rst
    │   └── modules/
    ├── setup.py
    ├── pyproject.toml
    └── README.md

Development Guidelines
-------------------

1. Code Style:

   - Follow PEP 8 guidelines
   - Use type hints
   - Write docstrings for all public functions and classes
   - Keep functions small and focused
   - Use meaningful variable names

2. Testing:

   - Write unit tests for all new features
   - Maintain test coverage above 80%
   - Use pytest for testing
   - Mock external services in tests

3. Documentation:

   - Update docstrings when changing code
   - Add examples in docstrings
   - Update relevant documentation files
   - Keep the README up to date

4. Git Workflow:

   - Create a new branch for each feature/fix
   - Use descriptive branch names
   - Write clear commit messages
   - Keep commits focused and atomic

Example Development Workflow
-------------------------

1. Create a new branch:

.. code-block:: bash

    git checkout -b feature/new-feature

2. Make your changes:

.. code-block:: python

    # Add your code
    def new_feature():
        """Add a new feature to the package.
        
        Returns:
            bool: True if successful, False otherwise.
            
        Example:
            >>> new_feature()
            True
        """
        return True

3. Write tests:

.. code-block:: python

    def test_new_feature():
        """Test the new feature."""
        assert new_feature() is True

4. Run tests:

.. code-block:: bash

    pytest tests/

5. Update documentation:

.. code-block:: python

    # Update docstrings
    # Update relevant .rst files
    # Update README if needed

6. Commit your changes:

.. code-block:: bash

    git add .
    git commit -m "Add new feature"
    git push origin feature/new-feature

7. Create a pull request:

   - Go to GitHub
   - Create a new pull request
   - Fill in the template
   - Request review

Pull Request Process
-----------------

1. Update the README.md with details of changes if needed
2. Update the documentation with any new features
3. The PR will be merged once you have the sign-off of at least one other developer
4. Make sure all tests pass
5. Update the version number in setup.py and pyproject.toml

Code Review Process
----------------

1. All submissions require review
2. Use the GitHub pull request process
3. Any reviewer can request changes
4. All tests must pass
5. Documentation must be updated

Testing
------

1. Run the test suite:

.. code-block:: bash

    pytest

2. Run with coverage:

.. code-block:: bash

    pytest --cov=aws_connector

3. Run specific tests:

.. code-block:: bash

    pytest tests/test_redshift.py

4. Run with verbose output:

.. code-block:: bash

    pytest -v

Documentation
-----------

1. Build the documentation:

.. code-block:: bash

    cd docs
    make html

2. View the documentation:

.. code-block:: bash

    # Open docs/_build/html/index.html in your browser

3. Update documentation:

   - Edit .rst files in docs/
   - Update docstrings in code
   - Rebuild documentation

Release Process
------------

1. Update version numbers:

   - setup.py
   - pyproject.toml
   - __init__.py

2. Update CHANGELOG.md

3. Create a release on GitHub

4. Build and upload to PyPI:

.. code-block:: bash

    python -m build
    twine upload dist/*

Questions and Support
------------------

- Open an issue on GitHub
- Join our community chat
- Check the documentation

Thank you for contributing! 