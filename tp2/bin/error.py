#  SCCS Keywords  "%Z% %M%  %I%  %H%"

# -*- coding: utf-8 -*-
"""
* This Price Determination Model and all associated code including without limitation,
* interface code was developed  by Boston Consulting Group  on behalf of UPS and is
* considered work for hire.
* The Pricing Determination Model and associated coded was provided to UPS without restriction.
* Any associated code including modifications of such code are deemed derivative work(s) of the
* Pricing Determination Model and ownership of such associated code vests with UPS.
* The use, disclosure, reproduction, modification, transfer, or transmittal of this work
* for any purpose in any form or by any means without the
* written permission of United Parcel Service is strictly prohibited.
* Confidential.  Unpublished Property of United Parcel Service.
* Use and Distribution limited solely to Authorized Persons.
*
*Copyright 2017 United Parcel Service of America, Inc.
* All Rights Reserved.
"""

# Modification Log:                                                                             */
#                                                                                               */
#    DATE      SE              DESCRIPTION                                                      */
# ---------   -------------    ---------------------------------------------                    */
# 10/19/17     Mohammed B       "Prior_GRS_REV" and "Prior_NET_REV" columns from                */
# 11/04/17     Mohammed B       "Config.ini has been removed and replace with                   */
#                               'settings.csv'. New functionalilty  read_config                 */
#                                is created which returns the config.                           */
# 11/04/17     Mohammed B        A new python file is created where the static                  */
#                                config items are placed.                                       */
# 11/07/17     Mohammed B        Hard-coding of version has been removed and placed             */
#                                in 'settings.csv'.                                             */
# 11/09/17     Mohammed B        The check for default.ini has been remove and the default.ini  */
#                                file is deleted                                                */



import datetime
import codecs
import logging
import os
import time
import sys
import traceback
from errno import EACCES, EPERM, ENOENT
from logging.handlers import TimedRotatingFileHandler
from config import STATIC_CONFIG
import pandas as pd
from logger_helper import logger


import configparser


# cx_Oracle 5.2.1
# matplotlib 2.0.0
# numpy 1.11.3
# pandas 0.19.2
# scikit-learn 0.18.1
# scipy 0.18.1


def print_error_message(e, file_name, logger=None, message=True):
    if message:
        traceMsg = traceback.format_exc()
    else:
        traceMsg = ""

    if logger is None:
        if hasattr(e, 'errno'):
            # PermissionError
            if e.errno == EPERM or e.errno == EACCES:
                print "Error 1.2a: Cannot read from file / folder {1} due to PermissionError({0}) in module {2}\n{3}".format(
                    e.errno, e.strerror, file_name, traceMsg)

            # FileNotFoundError
            elif e.errno == ENOENT:
                print "Error 1.2a: Cannot read from file / folder {1} due to FileNotFoundError({0}) in module {2}\n{3}".format(
                    e.errno, e.strerror, file_name, traceMsg)
            elif e.__class__ == IOError:
                print "Error 1.2a: Cannot read from file / folder {1} due to I/O error: {0}\n{2}".format(str(e),
                                                                                                         file_name,
                                                                                                         traceMsg)
            elif e.__class__ == OSError:
                print "Error 1.2a: Cannot read from file / folder {1} due to OS error: {0}\n{2}".format(str(e),
                                                                                                        file_name,
                                                                                                        traceMsg)
            else:  # general error
                if hasattr(e, "strerror"):
                    print "Error: {0} in module {1}\n{2}".format(e.strerror, file_name, traceMsg)
                elif hasattr(e, "message"):
                    print "Error: {0} in module {1}\n{2}".format(e.message, file_name, traceMsg)
                else:
                    print "Error in module {0}\n{1}".format(file_name, traceMsg)
        else:
            if hasattr(e, "strerror"):
                print file_name + ": " + e.strerror + "\n"
                print traceMsg
            elif hasattr(e, "message"):
                print file_name + ": " + e.message + "\n"
                print traceMsg
            else:
                print traceMsg
    else:
        if hasattr(e, 'errno'):
            # PermissionError
            if e.errno == EPERM or e.errno == EACCES:
                logger.error(
                    "Error 1.2a: Cannot read from file / folder {1} due to PermissionError({0}) in module {2}\n{3}".format(
                        e.errno, e.strerror, file_name, traceMsg))

            # FileNotFoundError
            elif e.errno == ENOENT:
                logger.error(
                    "Error 1.2a: Cannot read from file / folder {1} due to FileNotFoundError({0}) in module {2}\n{3}".format(
                        e.errno, e.strerror, file_name, traceMsg))
            elif e.__class__ == IOError:
                logger.error("Error 1.2a: Cannot read from file / folder {1} due to I/O error: {0}\n{2}".format(str(e),
                                                                                                                file_name,
                                                                                                                traceMsg))
            elif e.__class__ == OSError:
                logger.error(
                    "Error 1.2a: Cannot read from file / folder {1} due to OS error: {0}\n{2}".format(str(e), file_name,
                                                                                                      traceMsg))
            else:  # general error
                if hasattr(e, "strerror"):
                    logger.error("Error: {0} in module {1}\n{2}".format(e.strerror, file_name, traceMsg))
                elif hasattr(e, "message"):
                    logger.error("Error: {0} in module {1}\n{2}".format(e.message, file_name, traceMsg))
                else:
                    logger.error("Error in module {0}\n{1}".format(file_name, traceMsg))
        else:
            if hasattr(e, "strerror"):
                logger.error(file_name + ": " + e.strerror + "\n")
                logger.error(traceMsg)
            elif hasattr(e, "message"):
                logger.error(file_name + ": " + str(e.message) + "\n")
                logger.error(traceMsg)
            else:
                logger.error(traceMsg)


def createPaths(home, config):
    if home[-1] == '/' or home[-1] == '\\':
        home = home[:-1]

    paths_dict = {'c2p_path': home + config["PATHS"]["c2p_path"],
                  'p2c_path': home + config["PATHS"]["p2c_path"],
                  'log_path': home + config["PATHS"]["log_path"],
                  'error_log_path': os.environ['LOGDIR'] + config["PATHS"]["error_log_path"],
                  'model_log_path': home + config["PATHS"]["model_log_path"],
                  'model_path': home + config["PATHS"]["MODEL_OBJ_PATH"],
                  'init_path': home + config["PATHS"]["init_path"],
                  'input_path': home + config["PATHS"]["INPUT_PATH"]}

    return paths_dict


def read_config():
    path_to_config = os.environ['TP2_HOME']
    # path is in tp2, which is one level above and can be represented as ..
    # remaining path can be supplied to os.path
    _path = os.path.join(path_to_config, 'data', 'inputs', 'settings.csv')
    df = pd.read_csv(_path)
    config = {}
    for row in df.itertuples():
        tmp = config.get(row.Type, {})
        tmp.update({row.Key: row.Value})
        config[row.Type] = tmp

    return config


def checkErrors(type="Consumer", test=False):
    ck_err = False
    error_msg = ""
    success_msg = ""
    logger = ""
    home = ""
    # get config and read versions from it
    config = read_config()
    config.update(STATIC_CONFIG)
    if not config:
        error_msg += "Error 1.2a: no config found!"
        return ck_err, success_msg, error_msg, logger, config, home  # failed due to no config

    CX_ORACLE_V = config.get('VERSIONS', {}).get('CX_ORACLE_V', '')
    NP_V = config.get('VERSIONS', {}).get('NP_V', '')
    PD_V = config.get('VERSIONS', {}).get('PD_V', '')
    MATPLOTLIB_V = config.get('VERSIONS', {}).get('MATPLOTLIB_V', '')
    SKLEARN_V = config.get('VERSIONS', {}).get('SKLEARN_V', '')
    SCIPY_V = config.get('VERSIONS', {}).get('SCIPY_V', '')

    if type == "Consumer":  # producer only needs config, logger, and paths check
        # Check library versions
        try:
            import cx_Oracle as cx
        except ImportError, e:
            error_msg += "Error 1.3a: Python library not found: cx_Oracle\n"
            ck_err = True
        else:
            v = cx.__version__
            if v == CX_ORACLE_V:
                success_msg += "cx_Oracle loaded with version: " + v + "\n"
            else:
                error_msg += "Error 1.3b: Python library loaded with the wrong version: cx_Oracle (" + v + ")\n"
                ck_err = True

        try:
            import matplotlib
        except ImportError, e:
            error_msg += "Error: 1.3a: Python library not found: matplotlib\n"
            ck_err = True
        else:
            v = matplotlib.__version__
            if v == MATPLOTLIB_V:
                success_msg += "matplotlib loaded with version: " + v + "\n"
            else:
                error_msg += "Error 1.3b: Python library loaded with the wrong version: matplotlib (" + v + ")\n"
                ck_err = True

        try:
            import numpy as np
        except ImportError, e:
            error_msg += "Error: 1.3a: Python library not found: numpy\n"
            ck_err = True
        else:
            v = np.__version__
            if v == NP_V:
                success_msg += "numpy loaded with version: " + v + "\n"
            else:
                error_msg += "Error 1.3b: Python library loaded with the wrong version: numpy (" + v + ")\n"
                ck_err = True

        try:
            import pandas as pd
        except ImportError, e:
            error_msg += "Error: 1.3a: Python library not found: pandas\n"
            ck_err = True
        else:
            v = pd.__version__
            if v == PD_V:
                success_msg += "pandas loaded with version: " + v + "\n"
            else:
                error_msg += "Error 1.3b: Python library loaded with the wrong version: pandas (" + v + ")\n"
                ck_err = True

        try:
            import sklearn
        except ImportError, e:
            error_msg += "Error: 1.3a: Python library not found: sklearn\n"
            ck_err = True
        else:
            v = sklearn.__version__
            if v == SKLEARN_V:
                success_msg += "sklearn loaded with version: " + v + "\n"
            else:
                error_msg += "Error 1.3b: Python library loaded with the wrong version: sklearn (" + v + ")\n"
                ck_err = True

        try:
            import scipy
        except ImportError, e:
            error_msg += "Error: 1.3a: Python library not found: scipy\n"
            ck_err = True
        else:
            v = scipy.__version__
            if v == SCIPY_V:
                success_msg += "scipy loaded with version: " + v + "\n"
            else:
                error_msg += "Error 1.3b: Python library loaded with the wrong version: scipy (" + v + ")\n"
                ck_err = True

    # environmental variables
    try:
        os.environ["TP2_HOME"]
    except KeyError:
        error_msg += "Error 1.4: Environmental variable $TP2_HOME not found.\n"
        ck_err = True
    else:
        home = os.environ["TP2_HOME"]
        success_msg += "Environmental variable $TP2_HOME found: " + home + "\n"

    dbEnvironment = ["DBUSER", "DBPWD", "ORACLE_SID", "LOGDIR"]
    for i in dbEnvironment:
        try:
            os.environ[i]
        except KeyError:
            error_msg += "Error 1.4: Environmental variable $" + i + " not found.\n"
            ck_err = True
        else:
            success_msg += "Environmental variable $" + i + " found.\n"

    if ck_err:
        return ck_err, success_msg, error_msg, logger, config, home  # failed due to no libraries or environment variable
    else:
        # Load paths from config
        paths_dict = createPaths(home, config)

        if home[-1] == '/' or home[-1] == '\\':
            home = home[:-1]

        paths_dict['ipc'] = home + "/data/ipc"

        for path in paths_dict:
            try:
                if not os.path.exists(paths_dict[path]):
                    os.mkdir(paths_dict[path])
            except (IOError, OSError) as e:
                error_msg += "Error 1.2b: Cannot write to file/folder:  " + path + " due to :" + e.strerror + "\n"
                ck_err = True
            else:
                success_msg += "Directory " + paths_dict[path] + " is accessible\n"

        # Setup logging
        try:
            # logger = setup_logging(paths_dict['error_log_path'], level=level)
            logger = logger
        except (IOError, OSError) as e:
            error_msg += "Error 1.2b: Cannot write to file/folder: " + paths_dict[
                'error_log_path'] + " due to " + e.strerror + "\n"
            ck_err = True
        else:
            success_msg += "Logging file found and logger started successfully \n"

    if ck_err:
        return ck_err, success_msg, error_msg, logger, config, home  # failed due to pathing
    else:
        # Check to ensure we have connectivity with request transmission system
        if not os.path.exists(paths_dict['p2c_path'] + "init"):
            try:
                with open(paths_dict['p2c_path'] + "init", "w"):
                    pass
            except (IOError, OSError) as e:
                error_msg += "Error 1.2a: Cannot read from file/folder: " + paths_dict[
                    'p2c_path'] + "init due to :" + e.strerror + "\n"
                ck_err = True
            else:
                success_msg += "File " + paths_dict['p2c_path'] + "init is accessible\n"
        else:
            success_msg += "File " + paths_dict['p2c_path'] + "init is accessible\n"

        if not os.path.exists(paths_dict['c2p_path'] + "init"):
            try:
                with open(paths_dict['c2p_path'] + "init", "w"):
                    pass
            except (IOError, OSError) as e:
                error_msg += "Error 1.2a: Cannot read from file/folder: " + paths_dict[
                    'c2p_path'] + "init due to :" + e.strerror + "\n"
                ck_err = True
            else:
                success_msg += "File " + paths_dict['c2p_path'] + "init is accessible\n"
        else:
            success_msg += "File " + paths_dict['c2p_path'] + "init is accessible\n"

    if type == "Consumer" and not test:  # only for consumer and not in test mode
        try:
            dbuser = os.environ['DBUSER']
            dbpwd = os.environ['DBPWD']
            dbSID = os.environ['ORACLE_SID']
            host = config["DB"]["db_host"]
            host = dbSID + host

            db = cx.connect(dbuser, dbpwd, host)

        except Exception, e:
            error_msg += "Error 1.1: Oracle DB connection error:" + str(e) + "\n"
            ck_err = True
        else:
            success_msg += "Oracle connection successfully made.\n"
            db.close()

    return ck_err, success_msg, error_msg, logger, config, home


if __name__ == "__main__":
    # Check for catastrophic errors
    ck_err, success_msg, error_msg, logger, config, home = checkErrors()

    if ck_err:
        error_msg = "Startup encountered the following catastrophic errors:\n" + error_msg
        print error_msg
        success_msg = "Startup was able to perform the following:\n" + success_msg
        print success_msg
        sys.exit(1)
    else:
        success_msg = "Startup was able to perform the following:\n" + success_msg
        logger.info(success_msg)
        sys.exit(0)
