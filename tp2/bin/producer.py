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
# 10/19/17     Mohammed B       Gnd CWT check modified.                                         */
#                               Gaps are filled for the '-master.csv' analytic log. And been    */
#                               commented out for the future use.                               */
#                               Modified 'num_tires' to 'num_tries'                             */
#                               Cleanup action has been added up for the files that gets stuck  */
#                               up in the 'Producer2Consumer' Folder which are not being deleted*/
# 10/25/17     Mohammed B       Main method has been modified so that, it checks for consumer is*/
#                               Up and running.                                                 */
# 11/17/17     Mohammed B       "-v" option has been incorporated to test the bids in validation*/
#                               mode where it reads the data from database and put back to csv  */

import os
import time
import datetime
import uuid
import sys
import cPickle as pickle
from datetime import datetime
import pandas as pd
import cx_Oracle as cx
from etl import pullData, pushData, transformData, create_prod_base_tables
from error import print_error_message, checkErrors, createPaths
from logger_helper import logger, log_function


home = os.environ["TP2_HOME"]
os.path.dirname(os.path.abspath("__file__"))
paths = [home + '/data/inputs/', home + '/bin/']
for i in paths:
    sys.path.append(i)


@log_function
def main(bid_number, config, logger, test=False,
         test_bids=5, timeout=30, validations=False):
    """
    Main function, starts the program, setups the paths and submits
    the bidnumber to process
    """
    pd.options.mode.chained_assignment = None

    # Load paths from config
    home = os.environ[config["PATHS"]["HOME"]]

    # Load paths from config
    paths_dict = createPaths(home, config)

    # Write initial error flag
    set_error_flag(bid_number, paths_dict['error_log_path'], str(1), logger)

    # to do bulk scoring: check tp_bid sample and get all unique bids
    bid_numbers = pd.DataFrame()
    if test:
        try:
            tp20_bid = pd.read_csv(home + '/data/tp_bid.csv', dtype=str)
            bid_numbers = tp20_bid['NVP_BID_NR'].unique()

            print "Bid numbers found: "
            #print bid_numbers
        except RuntimeError as e:
            print_error_message(e, "Error 3.0: General producer error due to test run",
                                logger, False)
            sys.exit(1)

    master = pd.DataFrame()
    master_result = pd.DataFrame()
    sql = ''

    if test:
        bids_to_score = test_bids
        if bids_to_score == -1:
            bids_to_score = len(bid_numbers)
    else:
        bids_to_score = 1

    for i in range(0, bids_to_score):
        if test:
            bid_number = str(bid_numbers[i])

        if test:
            logger.info("Processing bid # " + str(i + 1) + " of " + str(len(bid_numbers)))
            print "Processing bid # " + str(i + 1) + " of " + str(len(bid_numbers))

        logger.info("Processing bid: " + bid_number)

        # Get data
        try:
            response, tp20_bid_shpr, tp20_ceiling_svc, tncvcel, tp_accessorial = \
                get_data(home, bid_number, config, test, logger)
        except Exception, e:
            if test:
                continue
            else:
                # print_error_message(e, "Error 2.2a: Data transformation issues: ", logger)
                sys.exit(1)

        try:
            # test for CWT threshold if CWT exists,
            # helps prevent CWT over threshold from going to consumer
            cwt = response[response.Product_Mode.isin(['AIR_CWT', 'GND_CWT'])]
            if not cwt.empty:
                # test CWT threshold
                cwt_filename = paths_dict['model_path'] + config["MODELS"]["CWT"] + ".p"
                with open(cwt_filename, "rb") as pickle_file:
                    air_bt_threshold, air_density_threshold, air_size_threshold, air_cohort_map, \
                    air_incentive_map, gnd_bt_threshold, gnd_density_threshold, \
                    gnd_size_threshold, \
                    gnd_cohort_map, gnd_incentive_map = pickle.load(pickle_file)

                # air cwt check
                air_cwt = cwt[cwt.Product == 'Air_CWT']
                if not air_cwt.empty:
                    air_max = air_size_threshold['MAX VALUE'].max()
                    air_cwt_value = cwt[cwt.Product == 'Air_CWT']['Bid_List_Rev_Wkly'].max()

                    if air_cwt_value > air_max:
                        raise RuntimeError("Error 2.2a: Data transformation issues: "
                                           "CWT threshold reached")

                # gnd cwt check
                gnd_cwt = cwt[cwt.Product == 'Gnd_CWT']
                if not gnd_cwt.empty:
                    gnd_max = gnd_size_threshold['MAX VALUE'].max()
                    gnd_cwt_value = cwt[cwt.Product == 'Gnd_CWT']['Bid_List_Rev_Wkly'].max()

                    if gnd_cwt_value > gnd_max:
                        raise RuntimeError("Error 2.2a: Data transformation issues: "
                                           "CWT threshold reached")

            # Enqueue the data
            master = master.append(response)

            result, result_file, p2c_file = enqueue(response, tp20_bid_shpr, timeout, bid_number,
                                                    paths_dict['c2p_path'],
                                                    paths_dict['p2c_path'], tncvcel,
                                                    paths_dict['log_path'])
            master_result = master_result.append(result)

        except RuntimeError as e:
            print_error_message(e, "", logger, False)

            if test:
                continue
            else:
                sys.exit(1)
        except (IOError, OSError) as e:
            print_error_message(e, "Error 3.2a: Model cannot be loaded: " +
                                config["MODELS"]["CWT"], logger)

        try:
            # store data
            if validations:
                test = True
            sql_result = put_data(home, bid_number, config, test, result,
                                  tp20_ceiling_svc, tp_accessorial, logger)
            if test:
                sql = sql + sql_result
                # True
        except Exception, e:
            print_error_message(e, "", logger, False)
            logger.warning("Bid " + bid_number + " scoring failed.")
        else:
            cleanup(paths_dict['c2p_path'] + result_file)
            logger.info("Bid " + bid_number + " successfully scored.")
            # Once done with everything write success flag
            set_error_flag(bid_number, paths_dict['error_log_path'], str(0), logger)

    # Output master dataset immediately
    #file_name = paths_dict['log_path'] + bid_number + '-' + p2c_file + '-master.csv'
    #master.to_csv(file_name)
    logger.debug('******************Exiting function main_Producer')
    if test:
        master_result.to_csv("results.csv")
        sql_file = open("sql_results.txt", "w")
        sql_file.write(sql)
        sql_file.close()


def cleanup(file):
    """Performs cleanup of working files"""
    # Deletes IPC files, ignore if can't be deleted
    try:
        os.remove(file)
    except:
        pass


def set_error_flag(bid_number, log_path, flag, logger):
    try:
        with open(log_path + 'tp2_' + bid_number + '.log', 'w') as error_log:
            error_log.write(flag + "\n")
    except (IOError, OSError) as e:
        print_error_message(e, log_path, logger)
        raise


@log_function
def get_data(home, bid_number, config, test, logger):
    """
    Gets the ETL'd data from controller
    """

    try:
        tp20_bid, tp20_bid_shpr, tp20_svc_grp, tp20_ceiling_svc, \
        tp20_shpr_svc, ttpsvgp, zone_weight, tncvcel, \
        tp_accessorial = pullData(bid_number, config["DB"]["db_host"], home, test)
    except ValueError, e:
        print_error_message(e,
                            "Error 2.2b: Data transformation issue for bid number " + bid_number +
                            " in step pullData",
                            logger)
        raise
    except Exception, e:
        print_error_message(e,
                            "Error 2.2a: Data transformation issue for bid number " + bid_number +
                            " in step pullData",
                            logger)
        raise

    try:
        tp_bid_table, tp_bid_svc_table = transformData(tp20_bid,
                                                       tp20_bid_shpr, tp20_svc_grp,
                                                       tp20_shpr_svc, ttpsvgp)
    except Exception, e:
        print_error_message(e, "Error 2.2a: Data transformation issue for bid number "
                            + bid_number + " in step transformaData", logger)
        raise

    try:
        # add data check
        if zone_weight is None:
            prod_table = create_prod_base_tables(home, tp_bid_table, tp_bid_svc_table)
        else:
            prod_table = create_prod_base_tables(home, tp_bid_table,
                                                 tp_bid_svc_table, zone_weight, tp20_svc_grp)
    except Exception, e:
        print_error_message(e, "Error 2.2a: Data transformation issue for bid number "
                            + bid_number + " in step createProdBaseTable", logger)
        raise

    prod_table = prod_table.fillna(0)
    logger.debug('******************Exiting function get_data')
    return prod_table[prod_table.BidNumber == bid_number], \
           tp20_bid_shpr, tp20_ceiling_svc, tncvcel, tp_accessorial


@log_function
def put_data(home, bid_number, config, test, response, tp20_ceiling_svc,
             tp_accessorial, logger):
    """
    Places data into Oracle DB
    """

    try:
        acy_table = response[response['Product_Mode'] == 'ACY']
        if not acy_table.empty:
            acy_table = acy_table.drop('SVC_GRP_NR', axis=1)  # remove due to blank causing issues
            acy_table = acy_table.merge(tp_accessorial,
                                    how='inner', on=['MVM_DRC_CD', 'SVM_TYP_CD',
                                                     'ASY_SVC_TYP_CD'])
            acy_table= acy_table.merge(tp20_ceiling_svc, how='inner',
                                      left_on=['BidNumber', 'SVC_GRP_NR'], right_on=['NVP_BID_NR',
                                                                                     'SVC_GRP_NR'])

        prod_table = response[response['Product_Mode'] != 'ACY']
        prod_table = prod_table.merge(tp20_ceiling_svc, how='inner',
                                      left_on=['BidNumber', 'SVC_GRP_NR'], right_on=['NVP_BID_NR',
                                                                                     'SVC_GRP_NR'])

        response = prod_table.append(acy_table)

        if test:
            acy_table = response[response['Product_Mode'] == 'ACY']
            prod_table = response[response['Product_Mode'] != 'ACY']

            prod_table = prod_table[
                ["BidNumber", "Product", "Incentive_Freight", "Target_Low",
                 "Target_High"]].drop_duplicates()

            for index, row in prod_table.iterrows():
                logger.info("{0}: Inc - {1}, Low - {2}, High - {3}".format(row["Product"],
                                                                           row["Incentive_Freight"],
                                                                           row["Target_Low"],
                                                                           row["Target_High"]))

                # try:
                #     acy_table = acy_table[["BidNumber", "MKG_SVP_DSC_TE", "ASY_SVC_TYP_CD",
                #                                           "RESI", "DAS"]].drop_duplicates()
                #
                #     for index, row in acy_table.iterrows():
                #         if row["ASY_SVC_TYP_CD"] == 'RES':
                #             logger.info("{0}: Inc - {1}".format(row["MKG_SVP_DSC_TE"],
                #  row["RESI"]))
                #         elif row["ASY_SVC_TYP_CD"] == 'GDL':
                #             logger.info("{0}: Inc - {1}".format(row["MKG_SVP_DSC_TE"],
                # row["DAS"]))
                # except Exception, e:
                #     pass
        logger.debug('******************Exiting function put_data')
        return pushData(home, bid_number, config["DB"]["db_host"], config, response,
                        tp_accessorial, logger, test)
    except (IOError, OSError) as e:
        print_error_message(e, home + "/" + bid_number + "_results.csv", logger)
        raise
    except RuntimeError, e:
        print_error_message(e, "", logger)
        raise



@log_function
def send_results(output_data, tncvcel, bidNumber, log_path, result_file, p2c_file):
    """
    Combine the output and input and
    write to a pickle file
    """
    file_name = bidNumber + '-' + p2c_file

    acy = output_data[output_data['Product_Mode'] == 'ACY']
    reg = output_data[output_data['Product_Mode'] != 'ACY']
    #reg = reg.drop('SVC_GRP_NR', axis=1)

    # merge tncvcel to capture TP 1.0 values
    tncvcel = tncvcel.rename(columns={'NVP_BID_NR': 'BidNumber', 'SVC_TYP_CD': 'SVM_TYP_CD',
                                      'RCM_NCV_QY': 'TP1_Target', 'NCV_MIN_QY': 'TP1_Low',
                                      'NCV_MAX_QY': 'TP1_High'})

    output_data = reg.merge(tncvcel, how="inner")

    # re-add accessorials
    output_data = output_data.append(acy)

    # check is RESI and DAS are columns
    if 'DAS' not in output_data.columns:
        output_data['DAS'] = ''

    if 'RESI' not in output_data.columns:
        output_data['RESI'] = ''

    # add timestamp
    output_data['Timestamp'] = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
    output_data.to_csv(log_path + file_name + '.csv')
    logger.debug('******************Exiting function send_results')
    return output_data, result_file, p2c_file


@log_function
def send_request(data, tp20_bid_shpr, folder_path, logger):
    """
    Write requests to unique file in folder_path
    """
    file_name = str(uuid.uuid4())

    # print "sending request: " + folder_path + file_name

    try:
        with open(folder_path + file_name, 'wb') as pickle_file:
            pickle.dump((data, tp20_bid_shpr, file_name), pickle_file, pickle.HIGHEST_PROTOCOL)
    except Exception as e:
        print_error_message(e, "Error 2.2b: Data exchange issues writing " +
                            folder_path + file_name, logger)
        raise
    logger.debug('******************Exiting function send_request')
    return file_name


def check_requests(folder_path):
    """
    This functions checks to see if there are any pending requests to process
    and returns a list of files to process
    """
    all_files = {f: os.path.getctime(folder_path + f) for f in os.listdir(folder_path)}
    unprocessed = [f for f in all_files.keys()]
    if unprocessed:
        return unprocessed
    else:
        return False


@log_function
def extract_data_ipc_file(file_path, logger):
    """Extracts data from a pickles file at file_path
    Returns the data"""
    num_tries = 10
    file_path = file_path.decode('utf-8')

    while True:
        try:
            time.sleep(0.1)

            if os.path.isfile(file_path):
                with open(file_path, "rb") as pickle_file:
                    data = pickle.load(pickle_file)
                    #print data

                return data
            else:
                return pd.DataFrame()

        except EOFError:
            if num_tries == 0:
                raise
            else:
                logger.info("EOFError on extracting: " + str(file_path) + " trying again...")
                num_tries = num_tries - 1
                continue
        except (OSError, IOError) as e:
            print_error_message(e, "Error 2.2b: Data exchange issues related to file: " +
                                file_path, logger)


@log_function
def enqueue(data, tp20_bid_shpr, timeout, bid_number, c2p_path, p2c_path, tncvcel, log_path):
    """
    Write the data to the Producer to Consumer file and check the Consumer to
    Producer file for the results
    """
    start_time = datetime.now()
    try:
        modified_start = max(start_time, max([os.path.getctime(p2c_path + f) \
                                              for f in os.listdir(p2c_path)]))
    except:
        modified_start = time.mktime(start_time.timetuple())

    p2c_file = send_request(data, tp20_bid_shpr, p2c_path, logger)

    while True:
        if (time.mktime(datetime.now().timetuple()) - modified_start) > timeout:
            cleanup(p2c_path + p2c_file)
            raise RuntimeError("Error 3.1: Producer.py timed out on bid number: " + bid_number)

        result_files = check_requests(c2p_path)
        if result_files:
            for result_file in result_files:
                if result_file == p2c_file:
                    check_file = extract_data_ipc_file(c2p_path + result_file, log_path)
                    return send_results(check_file, tncvcel, bid_number, log_path,
                                        result_file, p2c_file)

                time.sleep(0.1)


if __name__ == "__main__":
    import psutil
    import time

    cError, successMsg, errorMsg, _, config, home = checkErrors("Producer")
    # init as consumer is not running, in case it is running we will mark it as True
    # Inorder to debug the code make it True initially
    consumer_is_running = False
    # get all python processes and check if consumer is one of them, if so update consumer_is_running to True
    for i in psutil.process_iter():
        name = i.name()
        if 'python' in str(name):
            script_list = i.cmdline()
            from sys import platform
            if platform == 'win32' or platform == 'win64':
                if script_list and len(script_list) > 1 and 'consumer.py' in script_list[1]:
                    consumer_is_running = True
            else:
                if script_list and len(script_list) > 1 and home + '/bin/consumer.py' in script_list[1]:
                    consumer_is_running = True

    if not consumer_is_running:
        logger.error("Consumer is not running at this moment, Please start the consumer then producer.")
        raise AssertionError("Consumer is not running at this moment, Please start the consumer then producer.")
    logger.debug("Consumer is running")

    # Check for catastrophic errors
    if cError:
        print "Startup encountered the following errors:"
        # logger.error("Startup encountered the following errors:")
        print errorMsg
        # logger.error(errorMsg)
        print "Startup was able to perform the following:"
        print successMsg
        sys.exit(1)
    else:
        args = len(sys.argv)
        # first argument is just the name of the file
        if args > 1 and args < 4:
            if args == 3:
                if sys.argv[1] == '-t':  # anything else in the INI assumes production run
                    # logger.propagate = True
                    input_command = "Test scoring:"

                    try:
                        default_bids = int(sys.argv[2])
                    except:
                        default_bids = 5

                    logger.info(input_command + " added to producer to run")
                    main(input_command, config, logger, True, default_bids)
                elif sys.argv[1] == '-v':
                    input_command = "Validation Scoring:"
                    try:
                        bid_number = str(sys.argv[2])
                    except:
                        # default bid number if no bid number passed as input
                        bid_number = 'P010032181'
                    logger.info(input_command + "added to producer to run")
                    main(bid_number, config, logger, validations=True)
                else:
                    logger.error("Error 3.5: Unrecognized argument")
            else:
                try:
                    input_command = sys.argv[1]
                    try:
                        logger.info(input_command + " added to producer to run")
                        logger.info("Producer started")
                        main(input_command, config, logger)

                    except Exception, e:
                        print_error_message(e, "Error 3.0: General Bid Scoring failure", logger)
                except Exception, e:
                    print_error_message(e, "Error 3.4: No bid number provided", logger)

        else:
            logger.error("Error 3.4: No bid number provided")
            print "producer must have at least 2 arguments but no more than 3:"
            print "minimum: producer.py BIDNUMBER"
            print "test: producer.py -t NUMBER_OF_BIDS_TO_TEST"
            print "validations: producer.py -v BID_NUMBER"
            sys.exit(1)
