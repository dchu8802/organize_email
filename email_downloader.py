"""
    Download files for budget
    iterate through files
    truncate and load to staging tables
    combine all months into one column
    combine all data for all files into
    one temporary table using union
    pivot budget and forecast then
    insert into final table
    archive staging to archive table
"""

import os
import sys
import logging
import io
import argparse
import email
import imaplib
import smtplib
import base64

from datetime import datetime, timedelta
from functools import wraps

SEARCH_KEY = '(OR Subject "Google" Subject "welcome")'



def _parse_args():
    """Parse commandline arguments.

    Parameters:
    --debug = whether to enter debug mode


    Returns: list of arguments
    """
    parser = argparse.ArgumentParser(
        description="Add -d flag to show debug messages")
    parser.add_argument(
        "--debug", "-d",
        help="Show the debug messsages for logging",
        action="store_true"
    )
    raw_args = parser.parse_args()

    debug = raw_args.debug

    args = {}
    args['debug'] = debug
    return args

def log_wrap(func):
    """
    log decorator function to wrap each function call.
    Will write to logger the function being run.
    If there is an error will log to error.

    TO-DO:
    capture custom error message from function

    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        """
            wrapper function to wrap functionality
        """
        if debug_on:
            logger.debug("Running function '{}'".format(func.__name__))
            if args:
                arguments = ','.join(str(i) for i in args)
                logger.debug("Arguments: \n{}".format(arguments))
            if kwargs:
                keywords = ','.join('{}: {}'.format(str(k), str(v)) for k,v in kwargs)
                logger.debug("Keyword Arguments: \n{}".format(keywords))
        try:
            return func(*args, **kwargs)
        except Exception as e:
            err_msg = 'Error with message {}'.format(e)
            logger.error(err_msg, exc_info=True)
            raise e

    return wrapper

@log_wrap
def _add(x, y):
    return x + y

def email_login(login, password):
    """
    Connect to Google mail server using IMAP

    Parameter:
    login = username for email address
    password = password for email

    Return:
    imap_conn = IMAP connection instance
    """
    imap_conn = imaplib.IMAP4_SSL('imap.gmail.com')
    imap_conn.login(login, password)
    imap_conn.select('INBOX')
    return imap_conn

def open_email_get_file(imap_conn, since_date):
    """
    Search email account using search keys of subject and since date.
    Collect Ids of matching emails.
    Iterate over ids and fetch the email body.
    Convert the raw email into bytes.
    Get the payloads of the email.
    Call to get the filename from payload if it exists
    Store file name and payload in dict named attachments.
    Flag email as 'Seen'.

    Parameter:
    imap_conn = IMAP connection object
    since_date = last inserted date for the table

    Return:
    attachments = stg table as key, tuple of file name, payload for value
    """
    resp, data = imap_conn.search(
        None, SEARCH_KEY, 'SINCE "{0}"'.format(since_date))
    ids = data[0]
    if len(ids) == 0:
        return
    attachments = {}
    for i in ids.split():
        results, body = imap_conn.fetch(i, "BODY.PEEK[]")
        raw_email = body[0][1]
        print(raw_email)
        email_message = email.message_from_bytes(raw_email)
        payloads = email_message.get_payload()
        file_name = None
        for p in payloads:
            file_name = p.get_filename()
            if file_name:
                attachments[file_name] = p
                imap_conn.store(i, '+FLAGS', '\Seen')
    if len(attachments) == 0:
        return
    return attachments


def download_file(attachments):
    """
    Iterate over attachments dict.
    Key is the file name and value is file contents.
    Store th file contents as a StringIO.
    Return file dict.

    Parameters:
    attachments = dict of filename and data

    Return:
    to_download = a dict of file name as key and StringIO object as value.
    """
    to_download = {}
    for key, value in attachments.items():
        logger.debug('file name is {}'.format(key))
        data = value.get_payload(decode=True)
        to_download[key] = io.StringIO(data)
        print(to_download[key])
    return to_download

def read_file(file_path, file_name):
    """
    Open a query file from file path.
    read the contents and store to variable.
    return contents.
    
    Parameter:
    file_path = path to file folder
    file_name = name of file to open and read

    Return:
    contents = contents of file
    """
    dir_name = os.path.join(file_path, file_name)
    with open(dir_name, 'r') as file:
        contents = file.read()
    return contents

def main():
    logger.info('Starting Program')
    current_path = os.path.dirname(os.path.realpath(__file__))

    #imap_conn = email_login()
    yesterday = datetime.now() - timedelta(1)
    since_date = (yesterday).strftime('%d-%b-%Y')

    #open_email_get_file(imap_conn, since_date)

    _add(5,6)


    logger.info('Ending Program')


if __name__ == '__main__':
    global debug_on
    ARGS = _parse_args()
    logger = logging.getLogger()
    debug_on = ARGS['debug']
    if debug_on:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    stream = logging.StreamHandler(sys.stdout)
    formater = logging.Formatter('%(message)s @ %(asctime)s')
    stream.setFormatter(formater)
    logger.addHandler(stream)
    logger.debug('*********** In Debugging Mode ************')
    main()
