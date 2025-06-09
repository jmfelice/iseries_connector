Installation
===========

This guide will help you install the iSeries Connector library and its dependencies.

Requirements
-----------

* Python 3.9 or higher
* pyodbc
* pandas

Installing the Package
--------------------

You can install the package directly from GitHub:

.. code-block:: bash

    git clone https://github.com/jmfelice/iseries-connector.git
    cd iseries-connector
    python setup.py install

Installing Dependencies
---------------------

The package requires the following dependencies which will be installed automatically during setup:

* pyodbc: For database connectivity
* pandas: For data manipulation and analysis

Verifying the Installation
------------------------

To verify that the package is installed correctly, you can run:

.. code-block:: python

   from iseries_connector import ISeriesConn, ISeriesConfig
   print(ISeriesConn.__version__)

Development Installation
----------------------

If you want to install the package for development:

1. Clone the repository:

   .. code-block:: bash

      git clone https://github.com/jmfelice/iseries-connector.git
      cd iseries-connector

2. Create a virtual environment:

   .. code-block:: bash

      python -m venv venv
      source venv/bin/activate  # Linux/macOS
      .\venv\Scripts\activate   # Windows

3. Install development dependencies:

   .. code-block:: bash

      python setup.py develop

4. Run tests:

   .. code-block:: bash

      make test 