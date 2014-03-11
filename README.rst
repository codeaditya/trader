=========
trader.py
=========

trader.py is a Python 3 module which provides functions to download and process
the data for Stock Markets.

This is expected to be used as a back-end module for a GUI which utilizes the
work of this module. However, users are free to find their own creative uses.

Currently, the module provides support for the following Exchange/Segments:

- NSE/Indices (India)
- NSE/Equity (India)
- NSE/Futures (India)

This module provides the following user-facing functions:

- :`process_nse_indices()`: download and process NSE Indices data to save as csv
- :`process_nse_equities()`: download and process NSE Equity data to save as csv
- :`process_nse_futures()`: download and process NSE Futures data to save as csv
- :`download_file()`: download the files off internet
- :`to_datetime_date()`: return datetime.date object where possible else None
- :`ensure_trailing_slash()`: ensure a trailing slash at the end of the string
- :`create_folder()`: create the folder in a safe manner
- :`get_request_headers()`: return "request headers" dict for download_file()
- :`unzip()`: extract the contents of zip in present working directory
- :`write_csv()`: generate and save the file given necessary arguments

and defines the following private functions:

- `_convert_dash_to_zero()`
- `_sanitize_ohlc()`
- `_pop_unnecessary_keys()`
- `_format_output_data()`
- `_download_nse_indices()`
- `_download_nse_equities()`
- `_download_nse_futures()`
- `_get_nse_indices_fieldnames()`
- `_get_nse_equities_fieldnames()`
- `_get_nse_futures_fieldnames()`
- `_get_nse_indices_filenames()`
- `_get_nse_equities_filenames()`
- `_get_nse_futures_filenames()`
- `_manipulate_nse_indices()`
- `_manipulate_nse_equities()`
- `_manipulate_nse_futures()`
- `_output_nse_indices()`
- `_output_nse_equities()`
- `_output_nse_futures()`


How To Use This Module
----------------------
(See the docstrings of individual functions for details)

1. Import the module using:

       import trader

2. The user is expected to primarily make use of ``process_nse_indices()``,
   ``process_nse_equities()`` and ``process_nse_futures()`` functions. Just
   pass the required arguments to them to download and process the data for
   required Exchange/Segments.

3. ``download_file()`` function can be used to download anything off the
   internet.


How To Build Or Install The Module
----------------------------------

1. Clone the repository, ``cd`` into it and run the following command to install
   the module:

       python3 setup.py install

2. To build/create a source distribution archive (zip/tar.gz):

       python3 setup.py sdist

3. To generate an msi installer which would install the module in
   "<Python3_installation_folder>/Lib/site-packages/" and repair/remove it on
   running the file again:

       python3 setup.py bdist_msi


Principal Author And Maintainer
-------------------------------

- Aditya <code.aditya@gmail.com>


License
-------

trader.py is an Open Source Project released under `GNU General Public License
v3`_ (or any later version).

.. _GNU General Public License v3: https://www.gnu.org/licenses/gpl.html
