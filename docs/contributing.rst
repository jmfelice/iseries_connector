Contributing
===========

We love your input! We want to make contributing to iSeries Connector as easy and transparent as possible, whether it's:

* Reporting a bug
* Discussing the current state of the code
* Submitting a fix
* Proposing new features
* Becoming a maintainer

Development Process
----------------

We use GitHub to host code, to track issues and feature requests, as well as accept pull requests.

1. Fork the repo and create your branch from `main`
2. If you've added code that should be tested, add tests
3. If you've changed APIs, update the documentation
4. Ensure the test suite passes
5. Make sure your code lints
6. Issue that pull request!

Pull Request Process
-----------------

1. Update the README.md with details of changes to the interface, if applicable
2. Update the docs/ with any necessary documentation changes
3. The PR will be merged once you have the sign-off of at least one other developer

Any contributions you make will be under the MIT Software License
------------------------------------------------------------

In short, when you submit code changes, your submissions are understood to be under the same [MIT License](http://choosealicense.com/licenses/mit/) that covers the project. Feel free to contact the maintainers if that's a concern.

Report bugs using GitHub's [issue tracker](https://github.com/enterprise-dw/iseries-connector/issues)
------------------------------------------------------------------------------------------------

We use GitHub issues to track public bugs. Report a bug by [opening a new issue](https://github.com/enterprise-dw/iseries-connector/issues/new); it's that easy!

Write bug reports with detail, background, and sample code
------------------------------------------------------

**Great Bug Reports** tend to have:

* A quick summary and/or background
* Steps to reproduce
  * Be specific!
  * Give sample code if you can
* What you expected would happen
* What actually happens
* Notes (possibly including why you think this might be happening, or stuff you tried that didn't work)

Development Setup
--------------

1. Clone the repository:

   .. code-block:: bash

      git clone https://github.com/enterprise-dw/iseries-connector.git
      cd iseries-connector

2. Create a virtual environment:

   .. code-block:: bash

      python -m venv venv
      source venv/bin/activate  # Linux/macOS
      .\venv\Scripts\activate   # Windows

3. Install development dependencies:

   .. code-block:: bash

      pip install -e ".[dev]"

4. Run tests:

   .. code-block:: bash

      make test

5. Run linting:

   .. code-block:: bash

      make lint

6. Build documentation:

   .. code-block:: bash

      make docs

Code Style
---------

We use the following tools to maintain code quality:

* `ruff` for linting
* `mypy` for type checking
* `pytest` for testing
* `black` for code formatting

Before submitting a PR, make sure your code passes all checks:

.. code-block:: bash

   make lint
   make test
   make type-check

Documentation
-----------

We use Sphinx for documentation. When adding new features or changing existing ones, please update the documentation accordingly.

1. Update docstrings in the code
2. Update relevant documentation files in `docs/`
3. Build and check the documentation:

   .. code-block:: bash

      make docs
      make docs-serve  # To view the documentation locally

License
------

By contributing, you agree that your contributions will be licensed under its MIT License. 