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

--------------------------------------------------------------------------------

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

"""

import csv
import datetime
import logging
import os
import urllib.error
import urllib.request
import zipfile
import zlib

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


def process_nse_indices(start_date,
                        end_date=None,
                        download_location=os.path.join(os.getcwd(), 'downloads'),
                        output_location=os.path.join(os.getcwd(), 'processed_data'),
                        ignore_weekend=True,
                        debugging=False):
    """Processes the data for NSE Indices.

    The function takes six arguments:
     : start_date        : Accepts date of type datetime.datetime,
                           datetime.date and str in the form of
                           "YYYY-MM-DD" like "2014-01-01".
                           This refers the date from when the files
                           should be downloaded.
     : end_date          : Accepts date of type datetime.datetime,
                           datetime.date and str in the form of
                           "YYYY-MM-DD" like "2014-01-31".
                           This refers the date upto which the files
                           should be downloaded.
     : download_location : Where to save the downloaded files.
                           Used to pass as download_location argument to
                           `_download_nse_indices()` and input_location
                           argument to `_output_nse_indices()`.
     : output_location   : Where to save the processed output files.
                           Used to pass as output_location argument to
                           `_output_nse_indices()`.
     : ignore_weekend    : This takes boolean values ie; either True or
                           False. When set as True, it doesn't attempt
                           to download the data for Saturdays and
                           Sundays. Set its value as False if any trades
                           happen on a Saturday or a Sunday. This would
                           be specially useful in case of Muhurat
                           Trading.
     : debugging         : This takes boolean values and doesn't attempt
                           to download anything when set to True.
                           Used to pass the argument to
                           `_download_nse_indices()`.

    Examples:
    >>>process_nse_indices("2014-01-01")
    >>>process_nse_indices("2014-01-01", "2014-01-01")
    >>>process_nse_indices("2014-01-01", "2014-01-31")
    >>>process_nse_indices(datetime.datetime(2014,1,1), datetime.date(2014,1,1))
    >>>process_nse_indices("2014-01-01", "2014-01-31", ignore_weekend=False)
    >>>process_nse_indices("2014-01-01", "2014-01-31", ignore_weekend=False, debugging=True)
    >>>process_nse_indices("2014-01-01", "2014-01-31", download_location=os.getcwd(), output_location=os.getcwd(), debugging=True)

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
                              download_location=download_location,
                              debugging=debugging)
        _output_nse_indices(current_date,
                            input_location=download_location,
                            output_location=output_location)
        current_date += time_delta


def process_nse_equities(start_date,
                         end_date=None,
                         download_location=os.path.join(os.getcwd(), 'downloads'),
                         output_location=os.path.join(os.getcwd(), 'processed_data'),
                         ignore_weekend=True,
                         debugging=False):
    """Processes the data for NSE Equities.

    The function takes six arguments:
     : start_date        : Accepts date of type datetime.datetime,
                           datetime.date and str in the form of
                           "YYYY-MM-DD" like "2014-01-01".
                           This refers the date from when the files
                           should be downloaded.
     : end_date          : Accepts date of type datetime.datetime,
                           datetime.date and str in the form of
                           "YYYY-MM-DD" like "2014-01-31".
                           This refers the date upto which the files
                           should be downloaded.
     : download_location : Where to save the downloaded files.
                           Used to pass as download_location argument to
                           `_download_nse_equities()` and input_location
                           argument to `_output_nse_equities()`.
     : output_location   : Where to save the processed output files.
                           Used to pass as output_location argument to
                           `_output_nse_equities()`.
     : ignore_weekend    : This takes boolean values ie; either True or
                           False. When set as True, it doesn't attempt
                           to download the data for Saturdays and
                           Sundays. Set its value as False if any trades
                           happen on a Saturday or a Sunday. This would
                           be specially useful in case of Muhurat
                           Trading.
     : debugging         : This takes boolean values and doesn't attempt
                           to download anything when set to True.
                           Used to pass the argument to
                           `_download_nse_equities()`.

    Examples:
    >>>process_nse_equities("2014-01-01")
    >>>process_nse_equities("2014-01-01", "2014-01-01")
    >>>process_nse_equities("2014-01-01", "2014-01-31")
    >>>process_nse_equities(datetime.datetime(2014,1,1), datetime.date(2014,1,1))
    >>>process_nse_equities("2014-01-01", "2014-01-31", ignore_weekend=False)
    >>>process_nse_equities("2014-01-01", "2014-01-31", ignore_weekend=False, debugging=True)
    >>>process_nse_equities("2014-01-01", "2014-01-31", download_location=os.getcwd(), output_location=os.getcwd(), debugging=True)

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
                               download_location=download_location,
                               debugging=debugging)
        _output_nse_equities(current_date,
                             input_location=download_location,
                             output_location=output_location)
        current_date += time_delta


def process_nse_futures(start_date,
                        end_date=None,
                        download_location=os.path.join(os.getcwd(), 'downloads'),
                        output_location=os.path.join(os.getcwd(), 'processed_data'),
                        ignore_weekend=True,
                        debugging=False):
    """Processes the data for NSE Futures.

    The function takes six arguments:
     : start_date        : Accepts date of type datetime.datetime,
                           datetime.date and str in the form of
                           "YYYY-MM-DD" like "2014-01-01".
                           This refers the date from when the files
                           should be downloaded.
     : end_date          : Accepts date of type datetime.datetime,
                           datetime.date and str in the form of
                           "YYYY-MM-DD" like "2014-01-31".
                           This refers the date upto which the files
                           should be downloaded.
     : download_location : Where to save the downloaded files.
                           Used to pass as download_location argument to
                           `_download_nse_futures()` and input_location
                           argument to `_output_nse_futures()`.
     : output_location   : Where to save the processed output files.
                           Used to pass as output_location argument to
                           `_output_nse_futures()`.
     : ignore_weekend    : This takes boolean values ie; either True or
                           False. When set as True, it doesn't attempt
                           to download the data for Saturdays and
                           Sundays. Set its value as False if any trades
                           happen on a Saturday or a Sunday. This would
                           be specially useful in case of Muhurat
                           Trading.
     : debugging         : This takes boolean values and doesn't attempt
                           to download anything when set to True.
                           Used to pass the argument to
                           `_download_nse_futures()`.

    Examples:
    >>>process_nse_futures("2014-01-01")
    >>>process_nse_futures("2014-01-01", "2014-01-01")
    >>>process_nse_futures("2014-01-01", "2014-01-31")
    >>>process_nse_futures(datetime.datetime(2014,1,1), datetime.date(2014,1,1))
    >>>process_nse_futures("2014-01-01", "2014-01-31", ignore_weekend=False)
    >>>process_nse_futures("2014-01-01", "2014-01-31", ignore_weekend=False, debugging=True)
    >>>process_nse_futures("2014-01-01", "2014-01-31", download_location=os.getcwd(), output_location=os.getcwd(), debugging=True)

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
                              download_location=download_location,
                              debugging=debugging)
        _output_nse_futures(current_date,
                            input_location=download_location,
                            output_location=output_location)
        current_date += time_delta


def download_file(*urls,
                  download_location=os.path.join(os.getcwd(), 'downloads'),
                  debugging=False):
    """Downloads the files provided as multiple url arguments.

    Provide the url for files to be downloaded as strings. Separate the
    files to be downloaded by a comma.

    The function would download the files and save it in the folder
    provided as keyword-argument for download_location. If
    download_location is not provided, then the file would be saved in
    directory called as "downloads" created in the current working
    directory. Folder for download_location would be created if it
    doesn't already exist. Do not worry about trailing slash at the end
    for download_location. The code would take carry of it for you.

    If the download encounters an error it would alert about it and
    provide the information about the Error Code and Error Reason (if
    received from the server).

    Normal Usage:
    >>> download_file('http://localhost/index.html', 'http://localhost/info.php')
    >>> download_file('http://localhost/index.html', download_location='/home/aditya/Download/test')
    >>> download_file('http://localhost/index.html', download_location='/home/aditya/Download/test/')

    In Debug Mode, files are not downloaded, neither there is any
    attempt to establish the connection with the server. It just prints
    out the filename and its url that would have been attempted to be
    downloaded in Normal Mode.

    By Default, Debug Mode is inactive. In order to activate it, we
    need to supply a keyword-argument as 'debugging=True', like:
    >>> download_file('http://localhost/index.html', debugging=True)
    >>> download_file('http://localhost/index.html', download_location='/home/aditya/Download/test', debugging=True)

    """
    # Some initialization stuff
    download_location = ensure_trailing_slash(download_location)
    create_folder(download_location)
    headers = get_request_headers()

    # Loop through all the files to be downloaded
    for url in urls:
        filename = os.path.basename(url)
        if not debugging:
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
                logger.info("{0}: Downloaded successfully.".format(filename))
        else:
            logger.debug("DEBUGGING: {0} would be downloaded from {1}."
                         "".format(filename, url))


def to_datetime_date(input_date):
    """Checks the type of input_date and returns a datetime.date object
    where possible, else returns None. If input_date is a string not in
    format of '%Y-%m-%d' like 2013-12-31, it would raise ValueError.

    """
    date = None
    if type(input_date) is datetime.date:
        date = input_date
    elif type(input_date) is datetime.datetime:
        date = input_date.date()
    elif type(input_date) is str:
        date = datetime.datetime.strptime(input_date, '%Y-%m-%d').date()
    return date


def ensure_trailing_slash(input_string):
    """Ensures that there is a trailing slash at the end of the string.

    """
    if not input_string.endswith(os.sep):
        input_string = os.path.join(input_string, '')
    return input_string


def create_folder(folder):
    """Creates the folder, no problem if the folder already exists.

    """
    ensure_trailing_slash(folder)
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
    """Returns 'Request Headers' information as a dictionary.

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

    """
    with open(output_file, 'w', newline='') as file:
        csv.writer(file, delimiter=',').writerow(header)
        csv.DictWriter(file, fieldnames, delimiter=',').writerows(output_data)
    logger.info("{0}: File generated.".format(output_file))


def _convert_dash_to_zero(data):
    """Takes a dictionary and if any of the values is '-', it converts
    it to '0'.

    """
    for element in data:
        if data[element] == '-':
            data[element] = '0'


def _pop_unnecessary_keys(data):
    """Takes a list containing each element as a dictionary and removes
    unnecessary keys for final output.

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

    """
    strip_spaces = ['Symbol', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI']
    format_floats = ['Open', 'High', 'Low', 'Close']

    for element in data:
        # element['Date'] is to be formatted in _manipulate_*() functions
        for key in strip_spaces:
            element[key] = element[key].strip()
        for key in format_floats:
            element[key] = "{0:.2f}".format(float(element[key]))


def _download_nse_indices(date,
                          download_location=os.path.join(os.getcwd(), 'downloads'),
                          debugging=False):
    """Downloads the files for NSE Indices.

    The function takes one of the arguments as date which is a
    datetime.date object passed by `process_nse_indices()` function.
    So, this function ie; `_download_nse_indices()` is not intended to
    be used on a standalone basis.

    The files downloaded are:
    : Bhavcopy File : This is the main file which contains most of the
                      required data.
    : Vix File      : This contains the data for INDIAVIX Index. Data
                      for INDIAVIX is not present in the Bhavcopy File.

    """
    # Generate download URLs
    bhavcopy = r'http://www.nseindia.com/content/indices/ind_close_all_{full_date}.csv'.format(
        full_date=date.strftime('%d%m%Y'))
    vix = r'http://nseindia.com/content/vix/histdata/hist_india_vix_{full_date}_{full_date}.csv'.format(
        full_date=date.strftime('%d-%b-%Y'))

    # Download the files
    download_file(bhavcopy, vix,
                  download_location=download_location,
                  debugging=debugging)


def _download_nse_equities(date,
                           download_location=os.path.join(os.getcwd(), 'downloads'),
                           debugging=False):
    """Downloads the files for NSE Equities.

    The function takes one of the arguments as date which is a
    datetime.date object passed by `process_nse_equities()` function.
    So, this function ie; `_download_nse_equities()` is not intended to
    be used on a standalone basis.

    The files downloaded are:
    : Bhavcopy File : This is the main file which contains most of the
                      required data.
    : Delivery File : This file is downloaded because it contains the
                      data for Deliverable Quantity ie; how much
                      quantity was traded for Delivery ie;
                      (Total Volume - Intraday Volume).

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
    download_file(bhavcopy, delivery,
                  download_location=download_location,
                  debugging=debugging)


def _download_nse_futures(date,
                          download_location=os.path.join(os.getcwd(), 'downloads'),
                          debugging=False):
    """Downloads the files for NSE Futures.

    The function takes one of the arguments as date which is a
    datetime.date object passed by `process_nse_futures()` function.
    So, this function ie; `_download_nse_futures()` is not intended to
    be used on a standalone basis.

    The files downloaded are:
    : Bhavcopy File : This is the main file which contains most of the
                      required data.

    """
    # Generate download URLs
    bhavcopy = r'http://nseindia.com/content/historical/DERIVATIVES/{year}/{mon}/fo{date}{mon}{year}bhav.csv.zip'.format(
        year=date.strftime('%Y'),
        mon=(date.strftime('%b')).upper(),
        date=date.strftime('%d'))

    # Download the files
    download_file(bhavcopy,
                  download_location=download_location,
                  debugging=debugging)


def _get_nse_indices_fieldnames():
    """Returns the fieldnames present in bhavcopy file, vix file and the
    output file for NSE Indices that we would generate.

    """
    bhav_fieldnames = ('Symbol', 'Date', 'Open', 'High', 'Low', 'Close',
                       'Change', 'Change_pct', 'Volume', 'Turnover',
                       'PE', 'PB', 'Div_yield')
    vix_fieldnames = ('Date', 'Open', 'High', 'Low', 'Close', 'Prev_Close',
                      'Change', 'Change_pct')
    ind_fieldnames = ('Symbol', 'Date', 'Open', 'High', 'Low', 'Close',
                      'Volume', 'OI')
    return bhav_fieldnames, vix_fieldnames, ind_fieldnames


def _get_nse_equities_fieldnames():
    """Returns the fieldnames present in bhavcopy file, delivery file
    and the output file for NSE Equities that we would generate.

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

    """
    bhav_fieldnames = ('Instrument', 'Symbol', 'Expiry_Date', 'Strike_Price',
                       'Option_Type', 'Open', 'High', 'Low', 'Close',
                       'Settlement_Price', 'Contracts', 'Turnover_lakh', 'OI',
                       'OI_Change', 'Date')
    fut_fieldnames = ('Symbol', 'Date', 'Open', 'High', 'Low', 'Close',
                      'Volume', 'OI')
    return bhav_fieldnames, fut_fieldnames


def _get_nse_indices_filenames(date):
    """Generates file names for bhavcopy csv file, vix csv file and the
     output csv file for NSE Indices.

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

    """
    bhav_filename = r'fo{current_date}bhav.csv.zip'.format(
        current_date=(date.strftime('%d%b%Y')).upper())
    fut_filename = r'NSE-Futures-{current_date}.csv'.format(
        current_date=date.strftime('%Y-%m-%d'))
    return bhav_filename, fut_filename


def _manipulate_nse_indices(input_data, output_data, input_date_format):
    """Manipulates input_data, appends it to output_data and returns
    output_data at the end of the function.

    The function takes three arguments:

    : input_data        : a list each element of which is a dictionary
    : output_data       : a list each element of which is a dictionary
    : input_date_format : a string which represents the format of date
                          as present in input_data according to
                          datetime.datetime specifications.

    """
    for foo in input_data:
        record = foo.copy()
        record['OI'] = '0'
        if 'Symbol' in record:
            record['Symbol'] = record['Symbol'].upper().replace(" ", "")
        else:
            record['Symbol'] = 'INDIAVIX'
            record['Volume'] = '0'
        try:
            record['Date'] = datetime.datetime.strptime(
                record['Date'], input_date_format).date().isoformat()
        except ValueError:
            # This is raised because of the header line
            continue
        _convert_dash_to_zero(record)
        output_data.append(record)
    return output_data


def _manipulate_nse_equities(input_bhav, input_delv, output_data):
    """Manipulates input_bhav and input_delv, appends it to output_data
    and returns output_data at the end of the function.

    The function takes three arguments:

    : input_bhav  : a list each element of which is a dictionary
    : input_delv  : None or a list each element of which is a dictionary
    : output_data : a list each element of which is a dictionary

    """
    # Generate a dictionary with key as (Symbol, Series) tuple and
    # value as OI from input_delv. We would use this to get the value of
    # OI for our record below.
    # This is tremendously faster than looping through input_delv for
    # each foo in input_bhav.
    bar_oi = None
    if input_delv:
        bar_oi = {(bar['Symbol'], bar['Series']): bar.get('OI') for bar in input_delv}

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
                record['OI'] = bar_oi.get((foo['Symbol'], foo['Series']))
        if record:
            record['Date'] = datetime.datetime.strptime(
                record['Date'], '%d-%b-%Y').date().isoformat()
            output_data.append(record)
    return output_data


def _manipulate_nse_futures(input_bhav, output_data):
    """Manipulates input_bhav, appends it to output_data and returns
    output_data at the end of the function.

    The function takes two arguments:

    : input_bhav  : a list each element of which is a dictionary
    : output_data : a list each element of which is a dictionary

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
            record['Date'] = datetime.datetime.strptime(
                record['Date'], '%d-%b-%Y').date().isoformat()
            record['Close'] = record['Settlement_Price']
            if record['Close'] != '0' and (record['Open'] == '0' and
                                           record['High'] == '0' and
                                           record['Low'] == '0'):
                record['Open'] = record['Close']
                record['High'] = record['Close']
                record['Low'] = record['Close']
            output_data.append(record)
            previous_symbol = current_symbol
    return output_data


def _output_nse_indices(date,
                        input_location=os.path.join(os.getcwd(), 'downloads'),
                        output_location=os.path.join(os.getcwd(), 'processed_data')):
    """Outputs the files for NSE Indices as csv.

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

    data = None
    try:
        # Read the bhav_file as a list, each element of which is a
        # dictionary containing the record for a particular Symbol
        bhav = list(csv.DictReader(open(bhav_file, 'r'),
                                   delimiter=',',
                                   fieldnames=bhav_fieldnames,
                                   restkey='Skip',
                                   restval='Skip'))
    except FileNotFoundError:
        # We could not find our indices file.
        logger.error("{0}: File not found.".format(bhav_file))
    else:
        data = []
        _manipulate_nse_indices(input_data=bhav,
                                output_data=data,
                                input_date_format='%d-%m-%Y')
    try:
        # Read the bhav_file as a list, each element of which is a
        # dictionary containing the record for a particular Symbol
        vix = list(csv.DictReader(open(vix_file, 'r'),
                                  delimiter=',',
                                  fieldnames=vix_fieldnames,
                                  restkey='Skip',
                                  restval='Skip'))
    except FileNotFoundError:
        # We could not find our vix file.
        logger.error("{0}: File not found.".format(vix_file))
    else:
        if data is None:
            data = []
        _manipulate_nse_indices(input_data=vix,
                                output_data=data,
                                input_date_format='%d-%b-%Y')
    if data is not None:
        _pop_unnecessary_keys(data)
        _format_output_data(data)
        write_csv(ind_file, ind_header, ind_fieldnames, data)


def _output_nse_equities(date,
                         input_location=os.path.join(os.getcwd(), 'downloads'),
                         output_location=os.path.join(os.getcwd(), 'processed_data')):
    """Outputs the files for NSE Equities as csv.

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

    try:
        # Read the bhav_file as a list, each element of which is a
        # dictionary containing the record for a particular Symbol
        bhav = list(csv.DictReader(open(bhav_file, 'r'),
                                   delimiter=',',
                                   fieldnames=bhav_fieldnames,
                                   restkey='Skip',
                                   restval='Skip'))
    except FileNotFoundError:
        # We could not find our main file. Nothing should be processed.
        logger.error("{0}: File not found. Nothing is processed."
                     "".format(bhav_file))
        return
    else:
        data = []
        try:
            # Read the delv_file as a list, each element of which is a
            # dictionary containing the record for a particular Symbol
            delv = list(csv.DictReader(open(delv_file, 'r'),
                                       delimiter=',',
                                       fieldnames=delv_fieldnames,
                                       restval='Skip'))
        except FileNotFoundError:
            # We could not find the Delivery File. Okay, at least
            # process the Bhav file with OI data for 'EQ' Series as 0
            logger.warning("{0}: File not found. Delivery data is not being "
                           "processed.".format(delv_file))
            # We only want 'EQ'/'BE' Series data.
            _manipulate_nse_equities(input_bhav=bhav,
                                     input_delv=None,
                                     output_data=data)
        else:
            logger.debug("Both the files found - bhavcopy and delivery data.")
            # We only want 'EQ'/'BE' Series data. Also obtain the value
            # of OI for 'EQ' Series from delv
            _manipulate_nse_equities(input_bhav=bhav,
                                     input_delv=delv,
                                     output_data=data)
        finally:
            _pop_unnecessary_keys(data)
            _format_output_data(data)
            write_csv(eq_file, eq_header, eq_fieldnames, data)
    os.remove(bhav_file)


def _output_nse_futures(date,
                        input_location=os.path.join(os.getcwd(), 'downloads'),
                        output_location=os.path.join(os.getcwd(), 'processed_data')):
    """Outputs the files for NSE Futures as csv.

    """
    bhav_filename, fut_filename = _get_nse_futures_filenames(date)

    # Input files
    bhav_file = os.path.join(input_location, bhav_filename)

    # Where would we save the processed files
    create_folder(output_location)
    fno_file = os.path.join(output_location, fut_filename)

    # Extract the bhav_file downloaded as zip and refer to extracted csv file
    unzip(bhav_file)
    bhav_file = os.path.join(os.getcwd(), os.path.splitext(bhav_filename)[0])

    # Fieldnames present in files
    bhav_fieldnames, fut_fieldnames = _get_nse_futures_fieldnames()

    # The header line which we would write in the output file
    fno_header = 'Symbol', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'OI'

    try:
        # Read the bhav_file as a list, each element of which is a
        # dictionary containing the record for a particular Symbol
        bhav = list(csv.DictReader(open(bhav_file, 'r'),
                                   delimiter=',',
                                   fieldnames=bhav_fieldnames,
                                   restkey='Skip',
                                   restval='Skip'))
    except FileNotFoundError:
        # We could not find our main file. Nothing should be processed.
        logger.error("{0}: File not found. Nothing is processed."
                     "".format(bhav_file))
        return
    else:
        data = []
        _manipulate_nse_futures(input_bhav=bhav,
                                output_data=data)
        _pop_unnecessary_keys(data)
        _format_output_data(data)
        write_csv(fno_file, fno_header, fut_fieldnames, data)
    os.remove(bhav_file)


if __name__ == "__main__":
    # Toggle the debugging argument as necessary
    process_nse_indices("2014-02-28", debugging=True)
    process_nse_equities("2014-02-28", debugging=True)
    process_nse_futures("2014-02-28", debugging=True)
