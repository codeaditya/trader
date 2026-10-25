======
trader
======

**trader** is a Python 3 module that provides functions for downloading
and processing stock market data.

It is intended to serve as a back-end module for a GUI application that
uses its functionality. However, users are free to use the module for
other purposes.


Exchange/Segments Supported
---------------------------

- NSE Indices (India)
- NSE Equities (India)
- NSE Futures (India)


Functions Available
-------------------

Public Functions
++++++++++++++++

This module provides the following public functions:

``process_nse_indices()``
    Download and process NSE Indices data, saving the results as CSV files.

``process_nse_equities()``
    Download and process NSE Equities data, saving the results as CSV files.

``process_nse_futures()``
    Download and process NSE Futures data, saving the results as CSV files.

``download_file()``
    Download files from the internet.

``apply_server_timestamp()``
    Apply the Last-Modified timestamp from the HTTP response headers to files.

``to_datetime_date()``
    Return a ``datetime.date`` object when possible; otherwise, return ``None``.

``create_folder()``
    Create a folder safely.

``get_request_headers()``
    Return the request headers dictionary used by ``download_file()``.

``unzip()``
    Extract the contents of a ZIP archive into the current working directory.

``write_csv()``
    Generate and save a CSV file using the supplied arguments.


Private Functions
+++++++++++++++++

See the docstrings of individual private functions for details:

- ``_read_input_as_list()``
- ``_convert_dash_to_zero()``
- ``_convert_blank_to_zero()``
- ``_sanitize_ohlc()``
- ``_pop_unnecessary_keys()``
- ``_format_output_data()``
- ``_finalize_output()``

- ``_download_nse_indices()``
- ``_download_nse_equities()``
- ``_download_nse_futures()``

- ``_get_nse_indices_fieldnames()``
- ``_get_nse_equities_fieldnames()``
- ``_get_nse_futures_fieldnames()``

- ``_get_nse_indices_filenames()``
- ``_get_nse_equities_filenames()``
- ``_get_nse_futures_filenames()``

- ``_manipulate_nse_indices()``
- ``_manipulate_nse_equities()``
- ``_manipulate_nse_futures()``

- ``_parse_nse_indices()``
- ``_parse_nse_equities()``
- ``_parse_nse_futures()``

- ``_output_nse_indices()``
- ``_output_nse_equities()``
- ``_output_nse_futures()``


Global Variables
----------------

``DEBUGGING``
    When ``True``, no attempt is made to download files.

``DOWNLOAD_DIR``
    Directory used to store downloaded files.

``PROCESSED_DIR``
    Directory used to store processed files.


How To Build Or Install The Module
----------------------------------

Clone the repository and change into the repository directory.

Building the distribution packages
++++++++++++++++++++++++++++++++++

Install the ``build`` package and build the source distribution and wheel::

    python3 -m pip install --upgrade build
    python3 -m build

The generated distribution files will be available in the ``dist/``
directory.

Installing the built package
++++++++++++++++++++++++++++

Install the package from the locally built distribution files::

    python3 -m pip install --upgrade --no-index --find-links=dist/ trader

Note that ``--no-index`` prevents pip from accessing package indexes.
Any dependencies not already installed must also be available locally.


How To Use This Module
----------------------

Import the module::

    import trader

The module provides the following primary functions for downloading and
processing stock market data:

- ``process_nse_indices()``
- ``process_nse_equities()``
- ``process_nse_futures()``

Call the appropriate function to download and process data for the
required exchange segment. Refer to the function docstrings for details
about the required arguments.

The ``download_file()`` function can also be used to download files
from the internet.


Principal Author And Maintainer
-------------------------------

- Aditya <code.aditya@gmail.com>


License
-------

**trader** is free and open-source software distributed under the
GNU General Public License, version 3 or (at your option) any later
version.

See the `GNU General Public License v3
<https://www.gnu.org/licenses/gpl-3.0.html>`_.
