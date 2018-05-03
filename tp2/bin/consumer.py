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
* Copyright 2017 United Parcel Service of America, Inc.
* All Rights Reserved.
"""

# Modification Log:                                                                             */
#                                                                                               */
#    DATE      SE              DESCRIPTION                                                      */
# ---------   -------------    ---------------------------------------------                    */
# 10/20/17     Mohammed B       "num_tires" has been modified to num_tries.	                    */
# 11/17/17     Mohammed B       The config.ini static items have been moved to a config.py file */
# 12/7/17      Mohammed B       "Accessorial_map and Accessorials has been resplaced with       */
#                               "Eligible_Accessorials" and "Accessorial_Ceiling"               */
# 12/22/17     Mohammed B       New Accessorials method is being called and complete data is    */
#                               is passed through the Accessorials at once.                     */



from __future__ import division
import os
import sys
import cPickle as pickle
import datetime
import time
import pandas as pd
from economic_model_support_functions import OptimalIncentives
from error import print_error_message, checkErrors, createPaths
from calibration_support_functions import init_file_parser, settings_from_init_file
from cwt_class import cwt_production_class
from logger_helper import log_function, logger

os.path.dirname(os.path.abspath("__file__"))
PATHS = ['./data/inputs/']
for i in PATHS:
    sys.path.append(i)


@log_function
def main(config, logger, test=False):
    """
    This function loads all the models from memory and sets up the environment
    for receiving requests
    Note: all files in model_path need to be pickle files and all of them will be
    loaded into the model. If the model names are not in the INI file then
    they will not be used in prediction.
    """
    # Load paths from config
    home = os.environ[config["PATHS"]["HOME"]]

    # Load paths from config
    paths_dict = createPaths(home, config)

    ceilinglookup_filename = paths_dict['input_path'] + config["DATA"]["IWA_CEILING_PROD"]
    svc_to_prod_filename = paths_dict['input_path'] + config["DATA"]["SVC_MATCHING"]
    strategic_overlay_filename = paths_dict['input_path'] + config["DATA"]["STRATEGIC_OVERLAY"]
    sic_to_industry_filename = paths_dict['input_path'] + config["DATA"]["SIC_TO_INDUSTRY"]
    eligible_accessorials_filename = paths_dict['input_path'] + config["DATA"]["ELIGIBLE_ACCESSORIALS"]
    accessorial_ceiling_filename = paths_dict['input_path'] + config["DATA"]["ACCESSORIAL_CEILING"]
    datatypes_filename = paths_dict['input_path'] + config["DATA"]["DATA_TYPE"]
    cwt_filename = paths_dict['model_path'] + config["MODELS"]["CWT"] + ".p"


    # Load models from model_path directory
    model_objs = {}
    logger.info("Model Path loaded from: " + paths_dict['model_path'])

    for model in os.listdir(paths_dict['model_path']):
        try:
            if (model == "README.MD") or (model[-2:] != ".p") or (model == "cwt_production.p"):
                continue

            with open(paths_dict['model_path'] + model, "rb") as model_pickle:
                InfoLine = "Loading " + model + " model..."
                logger.info(InfoLine)

                modelName = model[:-2]
                model_objs[modelName] = pickle.load(model_pickle)
        except Exception, e:
            print_error_message(e, "Error 3.2a: Model cannot be loaded: " + model, logger)

    logger.info("All models loaded")

    # Load config variables
    settings_dict={}
    for key, value in config.iteritems():
        settings_dict.update(value)
    settings = settings_from_init_file(settings_dict)


    # Start run() which will check for requests
    logger.debug('******************Exiting function main_Consumer')
    run(home, paths_dict['c2p_path'], paths_dict['p2c_path'],
        model_objs, settings, paths_dict['init_path'],
        ceilinglookup_filename, svc_to_prod_filename,
        strategic_overlay_filename, sic_to_industry_filename,
        datatypes_filename, cwt_filename,
        accessorial_ceiling_filename, eligible_accessorials_filename, test, logger)


@log_function
def run(home, c2p_path, p2c_path, model_objs, settings, init_path,
        ceilinglookup_filename, svc_to_prod_filename,
        strategic_overlay_filename, sic_to_industry_filename,
        datatypes_filename, cwt_filename, accessorial_ceiling_filename,
        eligible_accessorials_filename, test, logger):

    """
    This function runs as continuous loop and receives and processes requests
    using the models brought into memory using the setup() function
    """
    modified_start = max([os.path.getctime(p2c_path + f) \
                          for f in os.listdir(p2c_path)])


    # clean up IPC directories
    for f in os.listdir(p2c_path):
        check_files = os.path.join(p2c_path, f)

        if f == 'init':
            continue

        cleanup(check_files)

    for f in os.listdir(c2p_path):
        check_files = os.path.join(c2p_path, f)

        if f == 'init':
            continue

        cleanup(check_files)

    logger.info("IPC folders cleaned")

    try:
        # read in datatypes and create a dtypes dict
        datatypes_table = pd.read_csv(datatypes_filename, index_col='Feature')
        data_type_dict = datatypes_table.T.to_dict(orient='record')[0]

        ceilinglookup_table = pd.read_csv(ceilinglookup_filename,
                                          dtype={'Product': 'str', 'Min_List_Rev_Wkly': 'float64',
                                                 'Max_List_Rev_Wkly': 'float64',
                                                 'Off_Inc_Cap': 'float64'})
        svc_to_prod_table = pd.read_csv(svc_to_prod_filename, dtype=str)
        strategic_overlay_table = pd.read_csv(strategic_overlay_filename)
        sic_to_industry_table = pd.read_csv(sic_to_industry_filename, dtype=str)
        eligible_accessorials = pd.read_csv(eligible_accessorials_filename)
        accessorial_ceiling = pd.read_csv(accessorial_ceiling_filename)
        # cwt calibration tables

        pd.options.mode.chained_assignment = None
        model = OptimalIncentives(settings=settings, model_objects=model_objs,
                                  ceilinglookup_file=ceilinglookup_table,
                                  svc_to_prod_file=svc_to_prod_table,
                                  industry_name_lookup=sic_to_industry_table,
                                  strategicOverlay=strategic_overlay_table,
                                  accessorial_ceiling = accessorial_ceiling,
                                  eligible_accessorials =eligible_accessorials,
                                  isProduction=True)
        model_cwt = cwt_production_class(cwt_filename, svc_to_prod_table, settings)
    except Exception, e:
        print_error_message(e, "Error 3.2b: Model created error", logger)
        raise
    logger.info("Consumer up and running")
    logger.debug('******************Middle of function run_Consumer')
    while 1:
        request = check_requests(p2c_path, modified_start)
        if request:
            logger.info("Found request %s", str(request))
            for ff in request:
                if ff == 'init':
                    continue

                try:
                    data, tp20_bid_shpr, file_uuid = extract_data_ipc_file(p2c_path + ff)
                    logger.info("Scoring bid# %s from file %s",
                                data['BidNumber'].iloc[0], file_uuid)

                    if data.empty:
                        continue  # file no longer exists

                    data = data.groupby('Product').first().reset_index()
                    # tp20_bid_shpr.to_csv('shipping.csv')

                    for colname in data_type_dict.keys():
                        #logger.debug('Searching for the colname')
                        if colname in data.columns:
                            data[colname] = data[colname].astype(data_type_dict[colname])
                except EOFError as e:
                    logger.error(e, "Error 3.3a: Consumer.py crashed due to file access: " +
                                 e, logger)
                    pass
                except Exception as e:
                    logger.error(e, "Error 3.3b: Consumer.py crashed due to model run: ", logger)
                    pass
                else:
                    try:
                        # Run the prediction using loaded model
                        start_time = datetime.datetime.now()

                        non_cwt_data = data[~data.Product_Mode.isin(['AIR_CWT', 'GND_CWT'])]
                        cwt_data = data[data.Product_Mode.isin(['AIR_CWT', 'GND_CWT'])]
                        results = model.run_calculator_production(non_cwt_data, tp20_bid_shpr)
                        mode_list = data.Product_Mode.unique()
                        results_cwt = None

                        #if 'AIR_CWT' in mode_list or 'GND_CWT' in mode_list:
                        # add variables needed by accessorial function
                        bid_list_rev = cwt_data[['Product', 'Bid_List_Rev_Wkly']]
                        results_cwt = model_cwt.scorer(cwt_data)
                        results_cwt = results_cwt.merge(bid_list_rev)
                        results_cwt['Optimal_UPSInc'] = results_cwt['Incentive_Freight']
                        results_cwt['Optimal_UPSInc_0'] = results_cwt['Incentive_Freight']
                        # call accessorials and attach back to CWT dataset
                        results = results.append(results_cwt).reset_index()
                        results = results.drop(['index'], axis=1)
                        results_acc = model.accessorials(results)
                        results = results.append(results_acc)
                        results = results.reset_index()

                        time_elapsed = datetime.datetime.now() - start_time

                        logger.info("Time elapsed: " + str(time_elapsed))
                        send_response(results, c2p_path, file_uuid)
                        cleanup(p2c_path + ff)

                        #last_file = ff
                    except (IOError, OSError) as e:
                        logger.error(e, "Error 3.3c: Consumer.py crashed due to file access: " +
                                     e, logger)
                        pass
                    except ValueError as e:
                        logger.error(e, "Error 3.3d: Consumer.py crashed due to model run: ",
                                     logger)
                        pass
                    except Exception as e:
                        logger.error(e, "Error 3.3e: Consumer.py crashed due to model run: ",
                                     logger)
                        pass
        try:
            modified_start = max(modified_start, min([os.path.getctime(p2c_path + f) \
                                                  for f in os.listdir(p2c_path)]))
        except Exception:
            logger.error("setting modified_start failed, causing consumer to crash")
            raise

def cleanup(file):
    """Performs cleanup of working files"""
    # Deletes IPC files, ignore if can't be deleted
    try:
        if os.path.isfile(file):
            os.remove(file)
    except:
        pass

@log_function
def extract_data_ipc_file(file_path):
    """Extracts data from a pickles file at file_path
    Returns the data"""
    num_tries = 10
    file_path = file_path.decode('utf-8')

    while True:
        try:
            if os.path.isfile(file_path):
                with open(file_path, "rb") as pickle_file:
                    data, tp20_bid_shpr, file_uuid = pickle.load(pickle_file)

                return data, tp20_bid_shpr, file_uuid
            else:
                return pd.DataFrame(), pd.DataFrame()
        except EOFError:
            if num_tries == 0:
                raise
            else:
                # "EOFError on file: " + str(file_path) + " trying again..."
                num_tries = num_tries - 1
                continue
        except (OSError, IOError) as e:
            print_error_message(e, file_path)
            raise
        except Exception as e:
            raise


def send_response(data, folder_path, file_uuid):
    """
    Write requests to unique file in folder_path
    """
    file_name = file_uuid
    with open(folder_path + file_name, 'wb') as pickle_file:
        pickle.dump(data, pickle_file, pickle.HIGHEST_PROTOCOL)


def check_requests(folder_path, modified_start):
    """
    This functions checks to see if there are any pending requests to process
    and returns a list of files to process
    """
    all_files = {f: os.path.getctime(folder_path + f) for f in os.listdir(folder_path)}
    unprocessed = [f for f in all_files.keys() if all_files[f] > modified_start]
    unprocessed.sort(key=lambda fn: os.path.getmtime(os.path.join(folder_path, fn)))

    if unprocessed:
        return unprocessed
    else:
        return False


if __name__ == "__main__":
    from logger_helper import logger
    # Check for catastrophic errors
    if len(sys.argv) > 1 and sys.argv[1] == '-t':  # anything in the argument assumes test run
        cError, successMsg, errorMsg, _, config, home = checkErrors("Consumer", True)
    else:
        cError, successMsg, errorMsg, _, config, home = checkErrors()

    if cError:
        print "Startup encountered the following errors:"
        print errorMsg
        print "Startup was able to perform the following:"
        print successMsg
        sys.exit(1)
    else:
        logger.info("Consumer started")
        logger.info(successMsg)

        if len(sys.argv) > 1 and sys.argv[1] == '-t':  # anything in the argument assumes test run
            main(config, logger, test=True)
        else:
            main(config, logger)
