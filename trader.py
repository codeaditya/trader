#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Copyright 2013, 2014 Aditya <code.aditya@gmail.com>

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

------------------------------------------------------------------------

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
    When it is ``True``, no attempt is made to download the files


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

------------------------------------------------------------------------

"""

# Import Standard modules
import csv
import datetime
import logging
import os
import urllib.error
import urllib.request
import zipfile
import zlib

# Import Third-party modules
from dateutil.parser import parse

###############################################################################
#### Add logging functionality ################################################
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
# Output formatting
formatter = logging.Formatter("{asctime} - {module:15s} - {process} - "
                              "{levelname:8s} - {message}", style="{")
ch.setFormatter(formatter)
# Add console handler to logger object
logger.addHandler(ch)
###############################################################################

###############################################################################
#### Other Global Variables ###################################################
# When DEBUGGING is set as True, it doesn't attempt to download anything
DEBUGGING = False
###############################################################################


def process_nse_indices(start_date,
                        end_date=None,
                        download_location=os.path.join(os.getcwd(),
                                                       'downloads'),
                        output_location=os.path.join(os.getcwd(),
                                                     'processed_data'),
                        ignore_weekend=True):
    """Processes the data for NSE Indices.

    :param start_date: date from when the files should be downloaded
    :param end_date: date upto which the files should be downloaded
    :param download_location: where to save the downloaded files
    :param output_location: where to save the processed output files
    :param ignore_weekend: when set as True, it doesn't attempt to
                           process the data for Saturdays and Sundays

    :type start_date: datetime.datetime or datetime.date or str
    :type end_date: None or datetime.datetime or datetime.date or str
    :type download_location: str
    :type output_location: str
    :type ignore_weekend: bool

    When passing `start_date` and `end_date` arguments as str, it should
    be in the form of "YYYY-MM-DD" like "2014-01-01".

    `download_location` is used to pass as `download_location` argument
    to `_download_nse_indices()` and `input_location` argument to
    `_output_nse_indices()`.

    `output_location` is used to pass as `output_location` argument to
    `_output_nse_indices()`.

    Pass `ignore_weekend` as False if any trades happen on a Saturday or
    a Sunday. This would be specially useful in case of Muhurat Trading.

    Examples:

        >>> process_nse_indices("2014-01-01")
        >>> process_nse_indices("2014-01-01", "2014-01-01")
        >>> process_nse_indices("2014-01-01", "2014-01-31")
        >>> process_nse_indices(datetime.datetime(2014,1,1), datetime.date(2014,1,1))
        >>> process_nse_indices("2014-01-01", "2014-01-31", ignore_weekend=False)
        >>> process_nse_indices("2014-01-01", "2014-01-31", download_location=os.getcwd(), output_location=os.getcwd())

    """
    logger.info("Processing NSE Indices...")

    if end_date is None:
        end_date = start_date

    # Ensure date inputs are datetime.date objects
    start_date = to_datetime_date(start_date)
    end_date = to_datetime_date(end_date)

    # Other initialization stuff
    current_date = start_date
    time_delta = datetime.timedelta(days=1)

    # Loop through dates to download files and output csv files
    while current_date <= end_date:
        if ignore_weekend and datetime.date.isoweekday(current_date) in (6, 7):
            logger.debug("Skipped NSE Indices for {0}.".format(current_date))
            current_date += time_delta
            continue
        _download_nse_indices(current_date,
                              download_location=download_location)
        _output_nse_indices(current_date,
                            input_location=download_location,
                            output_location=output_location)
        current_date += time_delta


def process_nse_equities(start_date,
                         end_date=None,
                         download_location=os.path.join(os.getcwd(),
                                                        'downloads'),
                         output_location=os.path.join(os.getcwd(),
                                                      'processed_data'),
                         ignore_weekend=True):
    """Processes the data for NSE Equities.

    :param start_date: date from when the files should be downloaded
    :param end_date: date upto which the files should be downloaded
    :param download_location: where to save the downloaded files
    :param output_location: where to save the processed output files
    :param ignore_weekend: when set as True, it doesn't attempt to
                           process the data for Saturdays and Sundays

    :type start_date: datetime.datetime or datetime.date or str
    :type end_date: None or datetime.datetime or datetime.date or str
    :type download_location: str
    :type output_location: str
    :type ignore_weekend: bool

    When passing `start_date` and `end_date` arguments as str, it should
    be in the form of "YYYY-MM-DD" like "2014-01-01".

    `download_location` is used to pass as `download_location` argument
    to `_download_nse_equities()` and `input_location` argument to
    `_output_nse_equities()`.

    `output_location` is used to pass as `output_location` argument to
    `_output_nse_equities()`.

    Pass `ignore_weekend` as False if any trades happen on a Saturday or
    a Sunday. This would be specially useful in case of Muhurat Trading.

    Examples:

        >>> process_nse_equities("2014-01-01")
        >>> process_nse_equities("2014-01-01", "2014-01-01")
        >>> process_nse_equities("2014-01-01", "2014-01-31")
        >>> process_nse_equities(datetime.datetime(2014,1,1), datetime.date(2014,1,1))
        >>> process_nse_equities("2014-01-01", "2014-01-31", ignore_weekend=False)
        >>> process_nse_equities("2014-01-01", "2014-01-31", download_location=os.getcwd(), output_location=os.getcwd())

    """
    logger.info("Processing NSE Equities...")

    if end_date is None:
        end_date = start_date

    # Ensure date inputs are datetime.date objects
    start_date = to_datetime_date(start_date)
    end_date = to_datetime_date(end_date)

    # Misc variables
    current_date = start_date
    time_delta = datetime.timedelta(days=1)

    # Loop through dates to download files and output csv files
    while current_date <= end_date:
        if ignore_weekend and datetime.date.isoweekday(current_date) in (6, 7):
            logger.debug("Skipped NSE Equities for {0}.".format(current_date))
            current_date += time_delta
            continue
        _download_nse_equities(current_date,
                               download_location=download_location)
        _output_nse_equities(current_date,
                             input_location=download_location,
                             output_location=output_location)
        current_date += time_delta


def process_nse_futures(start_date,
                        end_date=None,
                        download_location=os.path.join(os.getcwd(),
                                                       'downloads'),
                        output_location=os.path.join(os.getcwd(),
                                                     'processed_data'),
                        ignore_weekend=True):
    """Processes the data for NSE Futures.

    :param start_date: date from when the files should be downloaded
    :param end_date: date upto which the files should be downloaded
    :param download_location: where to save the downloaded files
    :param output_location: where to save the processed output files
    :param ignore_weekend: when set as True, it doesn't attempt to
                           process the data for Saturdays and Sundays

    :type start_date: datetime.datetime or datetime.date or str
    :type end_date: None or datetime.datetime or datetime.date or str
    :type download_location: str
    :type output_location: str
    :type ignore_weekend: bool

    When passing `start_date` and `end_date` arguments as str, it should
    be in the form of "YYYY-MM-DD" like "2014-01-01".

    `download_location` is used to pass as `download_location` argument
    to `_download_nse_futures()` and `input_location` argument to
    `_output_nse_futures()`.

    `output_location` is used to pass as `output_location` argument to
    `_output_nse_futures()`.

    Pass `ignore_weekend` as False if any trades happen on a Saturday or
    a Sunday. This would be specially useful in case of Muhurat Trading.

    Examples:

        >>> process_nse_futures("2014-01-01")
        >>> process_nse_futures("2014-01-01", "2014-01-01")
        >>> process_nse_futures("2014-01-01", "2014-01-31")
        >>> process_nse_futures(datetime.datetime(2014,1,1), datetime.date(2014,1,1))
        >>> process_nse_futures("2014-01-01", "2014-01-31", ignore_weekend=False)
        >>> process_nse_futures("2014-01-01", "2014-01-31", download_location=os.getcwd(), output_location=os.getcwd())

    """
    logger.info("Processing NSE Futures...")

    if end_date is None:
        end_date = start_date

    # Ensure date inputs are datetime.date objects
    start_date = to_datetime_date(start_date)
    end_date = to_datetime_date(end_date)

    # Other initialization stuff
    current_date = start_date
    time_delta = datetime.timedelta(days=1)

    # Loop through dates to download files and output csv files
    while current_date <= end_date:
        if ignore_weekend and datetime.date.isoweekday(current_date) in (6, 7):
            logger.debug("Skipped NSE Futures for {0}.".format(current_date))
            current_date += time_delta
            continue
        _download_nse_futures(current_date,
                              download_location=download_location)
        _output_nse_futures(current_date,
                            input_location=download_location,
                            output_location=output_location)
        current_date += time_delta


def download_file(*urls,
                  download_location=os.path.join(os.getcwd(), 'downloads')):
    """Downloads the files provided as multiple url arguments.

    :param urls: the url for files to be downloaded, separated by commas
    :param download_location: where to save the downloaded files

    :type urls: str
    :type download_location: str

    The function would download the files and save it in the folder
    provided as keyword-argument for `download_location`. If
    `download_location` is not provided, then the file would be saved in
    directory called as "downloads" created in the current working
    directory. Folder for `download_location` would be created if it
    doesn't already exist.

    If the download encounters an error it would alert about it and
    provide the information about the Error Code and Error Reason where
    possible.

    It preserves the Last Modification time as received from the server.

    It uses `DEBUGGING` global variable to activate the Debug Mode.

    In Debug Mode, files are not downloaded, neither there is any
    attempt to establish the connection with the server. It just prints
    out the filename and its url that would have been attempted to be
    downloaded in Normal Mode.

    Examples:

        >>> download_file('http://localhost/index.html', 'http://localhost/info.php')
        >>> download_file('http://localhost/index.html', download_location='/home/aditya/Download/test')
        >>> download_file('http://localhost/index.html', download_location='/home/aditya/Download/test/')

    """
    # Some initialization stuff
    create_folder(download_location)
    headers = get_request_headers()

    # Loop through all the files to be downloaded
    for url in urls:
        if not url:
            logger.debug("None url: Nothing to download.")
            continue
        filename = os.path.basename(url)
        if not DEBUGGING:
            logger.info("Downloading {0}.".format(url))
            try:
                request_sent = urllib.request.Request(url, headers=headers)
                response_received = urllib.request.urlopen(request_sent)
                logger.debug("Response received from {0}.".format(url))
            except urllib.error.URLError as e:
                logger.warning("{0}: File could not be downloaded."
                               "".format(filename))
                if hasattr(e, 'code'):
                    logger.warning("{0}: Error Code: {1}."
                                   "".format(filename, e.code))
                if hasattr(e, 'reason'):
                    logger.warning("{0}: Error Reason: {1}."
                                   "".format(filename, e.reason))
            else:
                output_file = os.path.join(download_location, filename)
                read_response = response_received.read()
                # Since we "Accept-Encoding" as "gzip", we need to
                # decompress the response if received as such
                # See: http://stackoverflow.com/q/16084117/1928540
                if response_received.headers.get('Content-Encoding') == 'gzip':
                    logger.debug("Response received with 'gzip' header. "
                                 "Decompressing it.")
                    read_response = zlib.decompress(read_response,
                                                    16 + zlib.MAX_WBITS)
                with open(output_file, 'wb') as downloaded_file:
                    downloaded_file.write(read_response)
                last_modified = response_received.headers.get('Last-Modified')
                apply_server_timestamp(output_file, last_modified)
                logger.info("{0}: Downloaded successfully.".format(filename))
        else:
            logger.info("DEBUG MODE: {0} would be downloaded from {1}."
                        "".format(filename, url))


def apply_server_timestamp(output_file, last_modified):
    """Changes the last modified and last access time of the output file
    to the one as received from Last-Modified response header.

    :param output_file: the file whose timestamp is to be altered
    :param last_modified: time string as received from Response headers

    :type output_file: str
    :type last_modified: str
    """
    server_datetime = datetime.datetime.strptime(last_modified,
                                                 "%a, %d %b %Y %H:%M:%S GMT")
    server_utc_time = server_datetime.replace(tzinfo=datetime.timezone.utc)
    server_timestamp = server_utc_time.timestamp()
    os.utime(output_file, times=(server_timestamp, server_timestamp))


def to_datetime_date(input_date):
    """Checks the type of `input_date` and returns a datetime.date
    object where possible, else returns None.

    :param input_date: date to be converted to datetime.date object
    :type input_date: datetime.datetime or datetime.date or str

    :rtype: datetime.date or None

    :raises: ValueError when `input_date` is provided as str not in
             format of "%Y-%m-%d"

    """
    date = None
    if type(input_date) is datetime.date:
        date = input_date
    elif type(input_date) is datetime.datetime:
        date = input_date.date()
    elif type(input_date) is str:
        date = datetime.datetime.strptime(input_date, '%Y-%m-%d').date()
    return date


def create_folder(folder):
    """Creates the folder, no problem if the folder already exists.

    :param folder: the location to be created
    :type folder: str

    """
    try:
        os.makedirs(folder, exist_ok=True)
        logger.debug("{0} folder exists. Ready to process further."
                     "".format(folder))
    except FileExistsError:
        # This is raised on Linux when a folder already exists however
        # its mode (ie; Permissions) do not match with the default mode
        logger.warning("{0}: Make sure the folder already exists and you have "
                       "write permissions for it.".format(folder))


def get_request_headers():
    """Returns "Request Headers" information as a dictionary.

    :rtype: dict

    """
    accept = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    accept_encoding = 'gzip, deflate'
    accept_language = 'en-US,en;q=0.5'
    connection = 'keep-alive'
    user_agent = 'Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:27.0) Gecko/20100101 Firefox/27.0'
    headers = {'Accept': accept,
               'Accept-Encoding': accept_encoding,
               'Accept-Language': accept_language,
               'Connection': connection,
               'User-Agent': user_agent,
               }
    return headers


def unzip(input_zipfile):
    """Unzips all the contents of the file in present working directory.

    :param input_zipfile: the file to be extracted
    :type input_zipfile: str

    """
    try:
        with zipfile.ZipFile(input_zipfile, 'r') as input_file:
            input_file.extractall()
        logger.debug("{0}: Zip file extracted.".format(input_zipfile))
    except FileNotFoundError:
        logger.warning("{0}: File not found. Zip file could not be extracted."
                       "".format(input_zipfile))


def write_csv(output_file, header, fieldnames, output_data):
    """Outputs the processed data to a file for final consumption.

    :param output_file: the file to be generated
    :param header: the header line to be written to `output_file`
    :param fieldnames: fieldnames present in each element of
                       `output_data`
    :param output_data: the data to be written

    :type output_file: str
    :type header: tuple
    :type fieldnames: tuple
    :type output_data: list containing each element as a dict

    """
    if len(output_data) < 1:
        return
    with open(output_file, 'w', newline='') as file:
        csv.writer(file, delimiter=',').writerow(header)
        csv.DictWriter(file, fieldnames, delimiter=',').writerows(output_data)
    logger.info("{0}: File generated.".format(output_file))


def _read_input_as_list(input_file, input_fieldnames):
    """Reads the input_file and returns a list, each element of which is
    a dictionary containing the record for a particular Symbol.

    :param input_file: file to be read
    :param input_fieldnames: fieldnames present in `input_file`

    :type input_file: str
    :type input_fieldnames: tuple

    :returns: data read with csv.DictReader
    :rtype: list containing each element as a dict

    """
    with open(input_file, 'r') as file:
        input_data = list(csv.DictReader(file,
                                         delimiter=',',
                                         fieldnames=input_fieldnames,
                                         restkey='Skip',
                                         restval='Skip'))
    return input_data


def _convert_dash_to_zero(data):
    """Takes a dictionary and if any of the values is "-", it converts
    it to "0".

    :param data: dictionary of record
    :type data: dict

    """
    for element in data:
        if data[element] == '-':
            data[element] = '0'


def _convert_blank_to_zero(data):
    """Takes a dictionary and if any of the values is blank i.e. "", it
    converts it to "0".

    :param data: dictionary of record
    :type data: dict

    """
    for element in data:
        if data[element] == '':
            data[element] = '0'


def _sanitize_ohlc(input_dict):
    """Takes a dictionary and modifies the values of Open, High and Low
    to be the value of Close; when the values of all of them is equal to
    zero.

    :param input_dict: dictionary of record to be sanitized
    :type input_dict: dict

    """
    keys = ['Open', 'High', 'Low']
    if input_dict['Close'] != '0' and all(input_dict[x] == '0' for x in keys):
        for field in keys:
            input_dict[field] = input_dict['Close']


def _pop_unnecessary_keys(data):
    """Takes a list containing each element as a dictionary and removes
    unnecessary keys for final output.

    :param data: list of output data
    :type data: list containing each element as a dict

    """
    unnecessary_keys = ['Change', 'Change_pct', 'Turnover', 'PE', 'PB', 'ISIN',
                        'Div_yield', 'Prev_Close', 'Skip', 'Series', 'LTP',
                        'Total_Trades', 'Instrument', 'Expiry_Date',
                        'Strike_Price', 'Option_Type', 'Settlement_Price',
                        'Contracts', 'Turnover_lakh', 'OI_Change']
    for element in data:
        for key in unnecessary_keys:
            element.pop(key, None)


def _format_output_data(data):
    """Takes a list containing each element as a dictionary and formats
    the keys for final output.

    :param data: list of output data
    :type data: list containing each element as a dict

    """
    strip_spaces = ['Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
    format_floats = ['Open', 'High', 'Low', 'Close']

    for element in data:
        # element['Date'] is to be formatted in _manipulate_*() functions
        for key in strip_spaces:
            element[key] = element[key].strip()
        for key in format_floats:
            element[key] = "{0:.2f}".format(float(element[key]))


def _finalize_output(data):
    """Takes a list containing each element as a dictionary and
    finalizes it for final consumption.

    :param data: list of output data
    :type data: list containing each element as a dict

    """
    if len(data) < 1:
        logger.warning("No data available.")
        return
    _pop_unnecessary_keys(data)
    _format_output_data(data)


def _download_nse_indices(date,
                          download_location=os.path.join(os.getcwd(),
                                                         'downloads')):
    """Downloads the files for NSE Indices.

    :param date: date for which files to be downloaded
    :param download_location: where to save the downloaded files

    :type date: datetime.date
    :type download_location: str

    The files downloaded are:

    - **Bhavcopy File:** This is the main file which contains most of
      the required data.
    - **Vix File:** This contains the data for INDIAVIX Index. Data for
      INDIAVIX is not present in the Bhavcopy File.

    """
    # Generate download URLs
    bhavcopy, vix = None, None
    bhavcopy = r'http://www.nseindia.com/content/indices/ind_close_all_{full_date}.csv'.format(
        full_date=date.strftime('%d%m%Y'))
    # Since 14th May 2014, data for INDIAVIX is included in bhavcopy
    # itself, so no need to download it separately
    if date < datetime.date(2014, 5, 14):
        vix = r'http://nseindia.com/content/vix/histdata/hist_india_vix_{full_date}_{full_date}.csv'.format(
            full_date=date.strftime('%d-%b-%Y'))

    # Download the files
    download_file(bhavcopy, vix, download_location=download_location)


def _download_nse_equities(date,
                           download_location=os.path.join(os.getcwd(),
                                                          'downloads')):
    """Downloads the files for NSE Equities.

    :param date: date for which files to be downloaded
    :param download_location: where to save the downloaded files

    :type date: datetime.date
    :type download_location: str

    The files downloaded are:

    - **Bhavcopy File:** This is the main file which contains most of
      the required data.
    - **Delivery File:** This file is downloaded because it contains the
      data for Deliverable Quantity ie; how much quantity was traded for
      Delivery ie; (Total Volume - Intraday Volume).

    The Deliverable Quantity data would be inserted into Open Interest
    field in the `_output_nse_equities()` function.

    """
    # Generate download URLs
    bhavcopy = r'http://nseindia.com/content/historical/EQUITIES/{year}/{mon}/cm{date}{mon}{year}bhav.csv.zip'.format(
        year=date.strftime('%Y'),
        mon=(date.strftime('%b')).upper(),
        date=date.strftime('%d'))
    delivery = r'http://www.nseindia.com/archives/equities/mto/MTO_{full_date}.DAT'.format(
        full_date=date.strftime('%d%m%Y'))

    # Download the files
    download_file(bhavcopy, delivery, download_location=download_location)


def _download_nse_futures(date,
                          download_location=os.path.join(os.getcwd(),
                                                         'downloads')):
    """Downloads the files for NSE Futures.

    :param date: date for which files to be downloaded
    :param download_location: where to save the downloaded files

    :type date: datetime.date
    :type download_location: str

    The files downloaded are:

    - **Bhavcopy File:** This is the main file which contains most of
      the required data.

    """
    # Generate download URLs
    bhavcopy = r'http://nseindia.com/content/historical/DERIVATIVES/{year}/{mon}/fo{date}{mon}{year}bhav.csv.zip'.format(
        year=date.strftime('%Y'),
        mon=(date.strftime('%b')).upper(),
        date=date.strftime('%d'))

    # Download the files
    download_file(bhavcopy, download_location=download_location)


def _get_nse_indices_fieldnames():
    """Returns the fieldnames present in bhavcopy file, vix file and the
    output file for NSE Indices that we would generate.

    :rtype: tuple of tuples

    """
    bhav_fieldnames = ('Symbol', 'Date', 'Open', 'High', 'Low', 'Close',
                       'Change', 'Change_pct', 'Volume', 'Turnover', 'PE',
                       'PB', 'Div_yield')
    vix_fieldnames = ('Date', 'Open', 'High', 'Low', 'Close', 'Prev_Close',
                      'Change', 'Change_pct')
    ind_fieldnames = ('Symbol', 'Date', 'Open', 'High', 'Low', 'Close',
                      'Volume', 'OI')
    return bhav_fieldnames, vix_fieldnames, ind_fieldnames


def _get_nse_equities_fieldnames():
    """Returns the fieldnames present in bhavcopy file, delivery file
    and the output file for NSE Equities that we would generate.

    :rtype: tuple of tuples

    """
    bhav_fieldnames = ('Symbol', 'Series', 'Open', 'High', 'Low', 'Close',
                       'LTP', 'Prev_Close', 'Volume', 'Turnover', 'Date',
                       'Total_Trades', 'ISIN')
    delv_fieldnames = ('Type', 'Sl_No', 'Symbol', 'Series', 'Volume', 'OI',
                       'OI_%')
    eq_fieldnames = ('Symbol', 'Date', 'Open', 'High', 'Low', 'Close',
                     'Volume', 'OI')
    return bhav_fieldnames, delv_fieldnames, eq_fieldnames


def _get_nse_futures_fieldnames():
    """Returns the fieldnames present in bhavcopy file and the output
    file for NSE Futures that we would generate.

    :rtype: tuple of tuples

    """
    bhav_fieldnames = ('Instrument', 'Symbol', 'Expiry_Date', 'Strike_Price',
                       'Option_Type', 'Open', 'High', 'Low', 'Close',
                       'Settlement_Price', 'Contracts', 'Turnover_lakh', 'OI',
                       'OI_Change', 'Date')
    fut_fieldnames = ('Symbol', 'Date', 'Open', 'High', 'Low', 'Close',
                      'Volume', 'OI')
    return bhav_fieldnames, fut_fieldnames


def _get_nse_indices_filenames(date):
    """Generates filenames for bhavcopy csv file, vix csv file and the
    output csv file for NSE Indices.

    :param date: date for which filenames are required
    :type date: datetime.date

    :rtype: tuple of str

    """
    bhav_filename = r'ind_close_all_{current_date}.csv'.format(
        current_date=date.strftime('%d%m%Y'))
    vix_filename = r'hist_india_vix_{current_date}_{current_date}.csv'.format(
        current_date=date.strftime('%d-%b-%Y'))
    ind_filename = r'NSE-Indices-{current_date}.csv'.format(
        current_date=date.strftime('%Y-%m-%d'))
    return bhav_filename, vix_filename, ind_filename


def _get_nse_equities_filenames(date):
    """Generates filenames for bhavcopy csv.zip file, delivery DAT file
    and the output csv file for NSE Equities.

    :param date: date for which filenames are required
    :type date: datetime.date

    :rtype: tuple of str

    """
    bhav_filename = r'cm{current_date}bhav.csv.zip'.format(
        current_date=(date.strftime('%d%b%Y')).upper())
    delv_filename = r'MTO_{current_date}.DAT'.format(
        current_date=date.strftime('%d%m%Y'))
    eq_filename = r'NSE-Equities-{current_date}.csv'.format(
        current_date=date.strftime('%Y-%m-%d'))
    return bhav_filename, delv_filename, eq_filename


def _get_nse_futures_filenames(date):
    """Generates filenames for bhavcopy csv.zip file and the output csv
    file for NSE Futures.

    :param date: date for which filenames are required
    :type date: datetime.date

    :rtype: tuple of str

    """
    bhav_filename = r'fo{current_date}bhav.csv.zip'.format(
        current_date=(date.strftime('%d%b%Y')).upper())
    fut_filename = r'NSE-Futures-{current_date}.csv'.format(
        current_date=date.strftime('%Y-%m-%d'))
    return bhav_filename, fut_filename


def _manipulate_nse_indices(input_data, output_data):
    """Manipulates the data for NSE Indices.

    :param input_data: data to be manipulated
    :param output_data: data after manipulation

    :type input_data: list containing each element as a dict
    :type output_data: list containing each element as a dict

    :returns: `output_data`
    :rtype: list containing each element as a dict

    The function takes `input_data`, manipulates and appends it to
    `output_data` and returns `output_data` at the end.

    """
    for foo in input_data:
        record = foo.copy()
        record['OI'] = '0'
        if 'Symbol' in record:
            record['Symbol'] = record['Symbol'].upper().replace(" ", "")
            try:
                # Turnover is in Rs. Crore, convert it to Rs. Lakh and
                # use it as Volume which is more useful than sum of
                # volume of all the scrips in case of Indices
                volume = float(record['Turnover']) * 100
                record['Volume'] = "{0:.0f}".format(volume)
            except ValueError:
                # This is raised because of the header line
                pass
        else:
            record['Symbol'] = 'INDIAVIX'
            record['Volume'] = '0'
        try:
            date_str = record.get('Date')
            record['Date'] = parse(date_str, dayfirst=True).date().isoformat()
        except TypeError:
            # This is raised because of the header line
            continue
        _convert_dash_to_zero(record)
        _convert_blank_to_zero(record)
        _sanitize_ohlc(record)
        output_data.append(record)
    return output_data


def _manipulate_nse_equities(input_bhav, input_delv, output_data):
    """Manipulates the data for NSE Equities.

    :param input_bhav: primary data to be manipulated
    :param input_delv: contains OI data for each record
    :param output_data: data after manipulation

    :type input_bhav: list containing each element as a dict
    :type input_delv: None or list containing each element as a dict
    :type output_data: list containing each element as a dict

    :returns: `output_data`
    :rtype: list containing each element as a dict

    The function takes `input_bhav` and `input_delv`, manipulates them
    and append it to `output_data` and returns `output_data` at the end.

    """
    # Generate a dictionary with key as (Symbol, Series) tuple and
    # value as OI from input_delv. We would use this to get the value of
    # OI for our record below.
    # This is tremendously faster than looping through input_delv for
    # each foo in input_bhav.
    delv_oi = None
    if input_delv:
        delv_oi = {(x['Symbol'], x['Series']): x.get('OI') for x in input_delv}

    # Loop through input_bhav
    for foo in input_bhav:
        record = None
        if foo['Series'] == 'BE':
            record = foo.copy()
            record['OI'] = record['Volume']
        elif foo['Series'] == 'EQ':
            record = foo.copy()
            if input_delv is None:
                record['OI'] = '0'
            else:
                record['OI'] = delv_oi.get((foo['Symbol'], foo['Series']))
        if record is not None:
            date_str = record.get('Date')
            record['Date'] = parse(date_str, dayfirst=True).date().isoformat()
            _sanitize_ohlc(record)   # Not generally required for NSE Equities
            output_data.append(record)
    return output_data


def _manipulate_nse_futures(input_bhav, output_data):
    """Manipulates the data for NSE Futures.

    :param input_bhav: data to be manipulated
    :param output_data: data after manipulation

    :type input_bhav: list containing each element as a dict
    :type output_data: list containing each element as a dict

    :returns: `output_data`
    :rtype: list containing each element as a dict

    The function takes `input_bhav`, manipulates and appends it to
    `output_data` and returns `output_data` at the end.

    """
    pos = 0
    previous_symbol = None
    future_suffix = ['-I', '-II', '-III', '-IV', '-V', '-VI', '-VII', '-VIII',
                     '-IX', '-X', '-XI', '-XII', '-XIII', '-XIV', '-XV', '-XVI']

    # Loop through input_bhav
    for foo in input_bhav:
        if foo['Instrument'] in ('FUTIDX', 'FUTIVX', 'FUTSTK'):
            current_symbol = foo['Symbol']
            record = foo.copy()
            if current_symbol == previous_symbol:
                pos += 1
            else:
                pos = 0
            record['Symbol'] = record['Symbol'] + future_suffix[pos]
            record['Volume'] = record['Contracts']
            date_str = record.get('Date')
            record['Date'] = parse(date_str, dayfirst=True).date().isoformat()
            record['Close'] = record['Settlement_Price']
            _sanitize_ohlc(record)
            output_data.append(record)
            previous_symbol = current_symbol
    return output_data


def _parse_nse_indices(input_file, input_fieldnames, output_data):
    """Parses the input file for NSE Indices.

    :param input_file: location of the input file
    :param input_fieldnames: fieldnames present in `input_file`
    :param output_data: list to append the parsed data for output

    :type input_file: str
    :type input_fieldnames: tuple
    :type output_data: list

    The function reads the `input_file` and manipulates them to return
    the `output_data`.

    """
    try:
        input_data = _read_input_as_list(input_file, input_fieldnames)
    except FileNotFoundError:
        logger.error("{0}: File not found.".format(input_file))
    else:
        _manipulate_nse_indices(input_data=input_data,
                                output_data=output_data)


def _parse_nse_equities(input_bhav_file, input_bhav_fieldnames,
                        input_delv_file, input_delv_fieldnames,
                        output_data):
    """Parses the input files for NSE Equities.

    :param input_bhav_file: location of the input bhavcopy file
    :param input_bhav_fieldnames: fieldnames present in `input_bhav_file`
    :param input_delv_file: location of the input delivery file
    :param input_delv_fieldnames: fieldnames present in `input_delv_file`
    :param output_data: list to append the parsed data for output

    :type input_bhav_file: str
    :type input_bhav_fieldnames: tuple
    :type input_delv_file: str
    :type input_delv_fieldnames: tuple
    :type output_data: list

    The function reads the input files and manipulates them to return
    the `output_data`.

    """
    bhav_data, delv_data = None, None
    try:
        bhav_data = _read_input_as_list(input_bhav_file, input_bhav_fieldnames)
        delv_data = _read_input_as_list(input_delv_file, input_delv_fieldnames)
    except FileNotFoundError:
        if not bhav_data:
            logger.error("{0}: File not found. Nothing is processed."
                         "".format(input_bhav_file))
            return
        if not delv_data:
            # We could not find the Delivery File. Okay, at least
            # process the Bhav file with OI data for 'EQ' Series as 0
            logger.warning("{0}: File not found. Delivery data is not being "
                           "processed.".format(input_delv_file))
            # We only want 'EQ'/'BE' Series data.
            _manipulate_nse_equities(input_bhav=bhav_data,
                                     input_delv=None,
                                     output_data=output_data)
    else:
        logger.debug("Both the files found - bhavcopy and delivery data.")
        # We only want 'EQ'/'BE' Series data. Also obtain the value of
        # OI for 'EQ' Series from delv
        _manipulate_nse_equities(input_bhav=bhav_data,
                                 input_delv=delv_data,
                                 output_data=output_data)


def _parse_nse_futures(input_file, input_fieldnames, output_data):
    """Parses the input file for NSE Futures.

    :param input_file: location of the input file
    :param input_fieldnames: fieldnames present in `input_file`
    :param output_data: list to append the parsed data for output

    :type input_file: str
    :type input_fieldnames: tuple
    :type output_data: list

    The function reads the `input_file` and manipulates them to return
    the `output_data`.

    """
    try:
        input_data = _read_input_as_list(input_file, input_fieldnames)
    except FileNotFoundError:
        logger.error("{0}: File not found.".format(input_file))
    else:
        _manipulate_nse_futures(input_bhav=input_data,
                                output_data=output_data)


def _output_nse_indices(date,
                        input_location=os.path.join(os.getcwd(),
                                                    'downloads'),
                        output_location=os.path.join(os.getcwd(),
                                                     'processed_data')):
    """Outputs the files for NSE Indices as csv.

    :param date: date for which files to be processed
    :param input_location: where are the files to be processed
    :param output_location: where to save the processed output files

    :type date: datetime.date
    :type input_location: str
    :type output_location: str

    """
    bhav_filename, vix_filename, ind_filename = _get_nse_indices_filenames(date)

    # Input files
    bhav_file = os.path.join(input_location, bhav_filename)
    vix_file = os.path.join(input_location, vix_filename)

    # Where would we save the processed files
    create_folder(output_location)
    ind_file = os.path.join(output_location, ind_filename)

    # Fieldnames present in files
    bhav_fieldnames, vix_fieldnames, ind_fieldnames = _get_nse_indices_fieldnames()

    # The header line which we would write in the output file
    ind_header = 'Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', \
                 'OI'

    output_data = []
    _parse_nse_indices(input_file=bhav_file,
                       input_fieldnames=bhav_fieldnames,
                       output_data=output_data)
    # Since 14th May 2014, data for INDIAVIX is included in bhavcopy
    # itself, so no need to read it separately
    if date < datetime.date(2014, 5, 14):
        _parse_nse_indices(input_file=vix_file,
                           input_fieldnames=vix_fieldnames,
                           output_data=output_data)
    _finalize_output(output_data)
    write_csv(ind_file, ind_header, ind_fieldnames, output_data)


def _output_nse_equities(date,
                         input_location=os.path.join(os.getcwd(),
                                                     'downloads'),
                         output_location=os.path.join(os.getcwd(),
                                                      'processed_data')):
    """Outputs the files for NSE Equities as csv.

    :param date: date for which files to be processed
    :param input_location: where are the files to be processed
    :param output_location: where to save the processed output files

    :type date: datetime.date
    :type input_location: str
    :type output_location: str

    """
    bhav_filename, delv_filename, eq_filename = _get_nse_equities_filenames(date)

    # Input files
    bhav_file = os.path.join(input_location, bhav_filename)
    delv_file = os.path.join(input_location, delv_filename)

    # Where would we save the processed files
    create_folder(output_location)
    eq_file = os.path.join(output_location, eq_filename)

    # Extract the bhav_file downloaded as zip and refer to extracted csv file
    unzip(bhav_file)
    bhav_file = os.path.join(os.getcwd(), os.path.splitext(bhav_filename)[0])

    # Fieldnames present in files
    bhav_fieldnames, delv_fieldnames, eq_fieldnames = _get_nse_equities_fieldnames()

    # The header line which we would write in the output file
    eq_header = 'Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI'

    output_data = []
    _parse_nse_equities(input_bhav_file=bhav_file,
                        input_bhav_fieldnames=bhav_fieldnames,
                        input_delv_file=delv_file,
                        input_delv_fieldnames=delv_fieldnames,
                        output_data=output_data)
    _finalize_output(output_data)
    write_csv(eq_file, eq_header, eq_fieldnames, output_data)
    if os.path.exists(bhav_file):
        os.remove(bhav_file)


def _output_nse_futures(date,
                        input_location=os.path.join(os.getcwd(),
                                                    'downloads'),
                        output_location=os.path.join(os.getcwd(),
                                                     'processed_data')):
    """Outputs the files for NSE Futures as csv.

    :param date: date for which files to be processed
    :param input_location: where are the files to be processed
    :param output_location: where to save the processed output files

    :type date: datetime.date
    :type input_location: str
    :type output_location: str

    """
    bhav_filename, fut_filename = _get_nse_futures_filenames(date)

    # Input files
    bhav_file = os.path.join(input_location, bhav_filename)

    # Where would we save the processed files
    create_folder(output_location)
    fut_file = os.path.join(output_location, fut_filename)

    # Extract the bhav_file downloaded as zip and refer to extracted csv file
    unzip(bhav_file)
    bhav_file = os.path.join(os.getcwd(), os.path.splitext(bhav_filename)[0])

    # Fieldnames present in files
    bhav_fieldnames, fut_fieldnames = _get_nse_futures_fieldnames()

    # The header line which we would write in the output file
    fut_header = 'Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', \
                 'OI'

    output_data = []
    _parse_nse_futures(input_file=bhav_file,
                       input_fieldnames=bhav_fieldnames,
                       output_data=output_data)
    _finalize_output(output_data)
    write_csv(fut_file, fut_header, fut_fieldnames, output_data)
    if os.path.exists(bhav_file):
        os.remove(bhav_file)


if __name__ == "__main__":
    # DEBUGGING = True
    process_nse_indices("2014-05-28")
    process_nse_equities("2014-05-28")
    process_nse_futures("2014-05-28")
