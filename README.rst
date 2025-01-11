======
trader
======

**trader** is a Python 3 module which provides functions to download and
process the data for Stock Markets.

This is expected to be used as a back-end module for a GUI which
utilizes the work of this module. However, users are free to find their
own creative uses.


Exchange/Segments Supported
---------------------------

- NSE/Indices (India)
- NSE/Equities (India)
- NSE/Futures (India)


Functions Available
-------------------

Public Functions
++++++++++++++++

This module provides the following public functions:

``process_nse_indices()``:
    download and process NSE Indices data to save as csv
``process_nse_equities()``:
    download and process NSE Equities data to save as csv
``process_nse_futures()``:
    download and process NSE Futures data to save as csv
``download_file()``:
    download the files off internet
``apply_server_timestamp()``:
    apply the last-modified timestamp to files as received from headers
``to_datetime_date()``:
    return datetime.date object where possible else None
``create_folder()``:
    create the folder in a safe manner
``get_request_headers()``:
    return "request headers" dict for `download_file()`
``unzip()``:
    extract the contents of zip file in present working directory
``write_csv()``:
    generate and save the file given necessary arguments

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

- ``DEBUGGING = False``
    When it is ``True``, no attempt is made to download the files.


How To Use This Module
----------------------

1. Import the module using:

       import trader

2. The user is expected to primarily make use of the following functions:

   - ``process_nse_indices()``
   - ``process_nse_equities()``
   - ``process_nse_futures()``

   Just pass the required arguments to them to download and process the
   data for required Exchange/Segments.

3. ``download_file()`` function can be used to download anything off the
   internet.


How To Build Or Install The Module
----------------------------------

1. Clone the repository, ``cd`` into it and run the following command to
   install the module:

        pip install dist/trader-2025.1.12-py3-none-any.whl

2. To build/create a source distribution archive (tar.gz and whl):

        python3 -m build


Principal Author And Maintainer
-------------------------------

- Aditya <code.aditya@gmail.com>


License
-------

**trader** is an Open Source Project released under `GNU General Public
License v3`_ (or any later version).

.. _GNU General Public License v3: https://www.gnu.org/licenses/gpl.html
