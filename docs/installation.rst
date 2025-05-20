Installation
===========

This guide will help you install the iSeries Connector library and its dependencies.

Requirements
-----------

* Python 3.9 or higher
* pyodbc
* pandas
* IBM iSeries Access ODBC Driver

Installing the Package
--------------------

You can install the package using pip:

.. code-block:: bash

   pip install iseries-connector

Installing Dependencies
---------------------

The package requires the following dependencies:

* pyodbc: For database connectivity
* pandas: For data manipulation and analysis

These will be installed automatically when you install the package.

Installing the ODBC Driver
------------------------

To use this package, you need to have the IBM iSeries Access ODBC Driver installed on your system.

Windows
~~~~~~~

1. Download the IBM iSeries Access ODBC Driver from the IBM website
2. Run the installer
3. Follow the installation wizard
4. Configure your DSN using the ODBC Data Source Administrator

Linux
~~~~~

1. Download the IBM iSeries Access ODBC Driver for Linux
2. Install the required packages:

   .. code-block:: bash

      sudo apt-get install unixodbc unixodbc-dev  # For Debian/Ubuntu
      sudo yum install unixODBC unixODBC-devel    # For RHEL/CentOS

3. Install the driver:

   .. code-block:: bash

      sudo ./install.sh

4. Configure your DSN in `/etc/odbc.ini`

macOS
~~~~~

1. Download the IBM iSeries Access ODBC Driver for macOS
2. Install the driver:

   .. code-block:: bash

      sudo ./install.sh

3. Configure your DSN in `/etc/odbc.ini`

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