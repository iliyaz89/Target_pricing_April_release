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
#                               tp20_shpr_svc_priorrev has been modified.                       */
#                               tp20_shpr_svc has been modified from the TransformData Function */
# 11/2/17      Mohammed B       "PKGBOL" has been replace with "PKGVOL".                        */
# 12/7/17      Mohammed B       PushData method has been modified, created a single API to      */
#                               update the TNCVCEL table and removed multiple occurrences       */
#                               copies of the same code for different purposes.                 */
# 12/22/17     Mohammed B       Filtered sql query to get only the IWA reported items.          */


import os
import sys
import pandas as pd
import numpy as np
import cx_Oracle as cx
from logger_helper import log_function, logger
DB = None


def connect(host):
    global DB
    dbuser = os.environ['DBUSER']
    dbpwd = os.environ['DBPWD']
    dbSID = os.environ['ORACLE_SID']

    host = dbSID + host

    try:
        if DB is None:
            DB = cx.connect(dbuser, dbpwd, host)
    except Exception:
        raise WindowsError("Unable to connect to database")

    return DB

@log_function
def pullData(bid_number, host, home, test):
    """
    This function fetches the data for the given bid_number from the
    database as defined in the @connect.

    :param bid_number: varchar, input to fetch data
    :param host: host is where the data is stored
    :param home: TP2_HOME
    :param test: Mode of execution is Test(if test = 'True')
    :return: data frame from the data queried
    """
    verbose = False
    try:
        if test:
            db = None
        else:
            db = connect(host)

        # tp_bid
        squery = "SELECT * FROM V_TP20_BID WHERE NVP_BID_NR = '" + bid_number + "'"
        if test:
            tp20_bid = pd.read_csv(home + '/data/tp_bid.csv')
            tp20_bid = tp20_bid[tp20_bid['NVP_BID_NR'] == bid_number]

            if verbose:
                print squery
        else:
            tp20_bid = pd.read_sql(squery, con=db)

        #tp_bid_shpr
        squery = "SELECT * FROM V_TP20_BID_SHPR_INFO WHERE NVP_BID_NR = '" + bid_number + "'"
        if test:
            tp20_bid_shpr = pd.read_csv(home + '/data/tp_bid_shpr_info.csv', dtype=str)\
                .sort_values(by=['NVP_BID_NR', 'SHR_AC_NR'], ascending=[1, 0])
            tp20_bid_shpr = tp20_bid_shpr[tp20_bid_shpr['NVP_BID_NR'] == bid_number]

            if verbose:
                print squery
        else:
            tp20_bid_shpr = pd.read_sql(squery, con=db).sort_values(by=['NVP_BID_NR', 'SHR_AC_NR'],
                                                                    ascending=[1, 0])
            tp20_bid_shpr = pd.DataFrame(tp20_bid_shpr, dtype=str)

        #tp_svc_grp
        squery = "SELECT * FROM V_TP20_SERVICE_GROUP WHERE NVP_BID_NR = '" + bid_number + "'"
        if test:
            tp20_svc_grp = pd.read_csv(home + '/data/tp_svc_grp.csv')
            tp20_svc_grp = tp20_svc_grp[tp20_svc_grp['NVP_BID_NR'] == bid_number]

            if verbose:
                print squery
        else:
            tp20_svc_grp = pd.read_sql(squery, con=db)

        #tp_ceiling_svc
        squery = "SELECT * FROM V_TP20_CEILING_SERVICES WHERE NVP_BID_NR = '" + bid_number + "'"
        if test:
            tp20_ceiling_svc = pd.read_csv(home + '/data/tp_ceiling.csv')
            tp20_ceiling_svc = tp20_ceiling_svc[tp20_ceiling_svc['NVP_BID_NR'] == bid_number]

            if verbose:
                print squery
        else:
            tp20_ceiling_svc = pd.read_sql(squery, con=db)

        #tp_shpr_svc
        squery = "SELECT * FROM V_TP20_SHPR_SVC_VOL_REV WHERE NVP_BID_NR = '" + bid_number + "'"
        if test:
            tp20_shpr_svc = pd.read_csv(home +
                                        '/data/tp_shpr_svc_vol_rev.csv').loc[:, ['NVP_BID_NR', 'SVC_GRP_NR',
                                                                                 'SHR_AC_NR', 'RA_TRI_NR',
                                                                                 'SHR_PJT_WVL_QY',  # Current Quantity
                                                                                 'SHR_PJT_GRS_RPP_A',  # Current Price PP
                                                                                 'SHR_PRR_GRS_RVN_A',  # Prior Gross PP
                                                                                 'SHR_PRR_NET_RVN_A']]  # Prior Net PP
            tp20_shpr_svc = tp20_shpr_svc[tp20_shpr_svc['NVP_BID_NR'] == bid_number]

            if verbose:
                print squery
        else:
            tp20_shpr_svc = pd.read_sql(squery, con=db).loc[:, ['NVP_BID_NR', 'SVC_GRP_NR', 'SHR_AC_NR', 'RA_TRI_NR',
                                                               'SHR_PJT_WVL_QY',  # Current Quantity
                                                               'SHR_PJT_GRS_RPP_A',  # Current Price PP
                                                               'SHR_PRR_GRS_RVN_A',  # Prior Gross PP
                                                               'SHR_PRR_NET_RVN_A']]  # Prior Net PP

        #ttpsvgp
        squery = "SELECT NVP_BID_NR, SVC_GRP_NR, PND_STS_CD, CPE_ETM_RPP_A, SVC_GRP_TRG_PSE_A, SVC_TRG_LOW_RNG_A," \
                " SVC_TRG_HI_RNG_A, TRG_PSE_FCR_NR FROM TTPSVGP WHERE NVP_BID_NR = '" + bid_number + "'"
        if test:
            ttpsvgp = pd.read_csv(home + '/data/ttpsvgp.csv').loc[:,
                  ['NVP_BID_NR', 'SVC_GRP_NR', 'PND_STS_CD','CPE_ETM_RPP_A', 'SVC_GRP_TRG_PSE_A', 'SVC_TRG_LOW_RNG_A',
                   'SVC_TRG_HI_RNG_A', 'TRG_PSE_FCR_NR']]
            ttpsvgp = ttpsvgp[ttpsvgp['NVP_BID_NR'] == bid_number]

            if verbose:
                print squery
        else:
            ttpsvgp = pd.read_sql(squery, con=db)

        #zone_weight
        squery = "SELECT NVP_BID_NR,SVC_GRP_NR,SVC_GRP_SUF_NR,DEL_ZN_NR,WGT_MS_UNT_TYP_CD, " \
                "WGT_CGY_WGY_QY,round(ADJ_PKG_VOL_QY ,10) as PKGVOL," \
                "(CASE WGT_MS_UNT_TYP_CD WHEN 'OZ' " \
                "THEN cast(WGT_CGY_WGY_QY as DECIMAL(9,2)) / 16.0 " \
                "ELSE cast(WGT_CGY_WGY_QY as DECIMAL(9,2)) END) " \
                "as WEIGHT FROM V_TP20_ZONE_WGT_VOL_DIST WHERE NVP_BID_NR = '" + bid_number + \
                "' AND DEL_ZN_NR != 'ALL'"
        if test:
            zone_weight = pd.read_csv(home + '/data/tp_zone_weight.csv')
            zone_weight = zone_weight[zone_weight['NVP_BID_NR'] == bid_number]

            if verbose:
                print squery
        else:
            zone_weight = pd.read_sql(squery, con=db)

        #tncvcel
        #Filtered sql query get only the IWA reported items
        squery = "SELECT DISTINCT C.*, D.MVM_DRC_CD, D.SVC_TYP_CD, D.SVC_FEA_TYP_CD FROM " \
        "(SELECT A.* FROM " \
        "(SELECT NVP_BID_NR, SVC_GRP_NR, RCM_NCV_QY, NCV_MIN_QY, NCV_MAX_QY FROM TNCVCEL " \
        "WHERE NVP_BID_NR = '" + bid_number + "'  AND COS_CCL_MTH_TYP_CD = '00' ) A " \
                                              "INNER JOIN V_TP20_CEILING_SERVICES B " \
                                              "ON A.NVP_BID_NR = B.NVP_BID_NR AND A.SVC_GRP_NR = B.SVC_GRP_NR) C " \
                                              "INNER JOIN V_TP20_SERVICE_GROUP D ON C.NVP_BID_NR = D.NVP_BID_NR AND C.SVC_GRP_NR = D.SVC_GRP_NR"


        if test:
            tncvcel = pd.read_csv(home + '/data/tncvcel.csv')
            tncvcel = tncvcel[tncvcel['NVP_BID_NR'] == bid_number]

            if verbose:
                print squery
        else:
            tncvcel = pd.read_sql(squery, con=db)

        #tp_accessorial
        squery = "SELECT * FROM V_TP20_ACCESSORIAL WHERE NVP_BID_NR = '" + bid_number + "'"

        if test:
            tp_accessorial = pd.read_csv(home + '/data/tp_accessorial.csv')
            tp_accessorial = tp_accessorial[tp_accessorial['NVP_BID_NR'] == bid_number]
        else:
            tp_accessorial = pd.read_sql(squery, con=db)


        # need to test the data to make sure we're getting returns
        if tp20_bid.empty:
            raise ValueError("tp20_bid has no data")

        if tp20_bid_shpr.empty:
            raise ValueError("tp20_bid_shpr has no data")

        if tp20_svc_grp.empty:
            raise ValueError("tp20_svc_grp has no data")

        if tp20_ceiling_svc.empty:
            raise ValueError("tp20_ceiling_svc has no data")

        if tp20_shpr_svc.empty:
            raise ValueError("tp20_shpr_svc has no data")

        if ttpsvgp.empty:
            raise ValueError("ttpsvgp has no data")

        if zone_weight.empty:
            zone_weight = None

        if tncvcel.empty and not test:
            raise ValueError("tncvcel has no data")

        if not tp_accessorial.empty:
            # remove accessorial services
            acc_svcs = tp_accessorial['SVC_GRP_NR'].unique()
            tp20_svc_grp = tp20_svc_grp[~tp20_svc_grp['SVC_GRP_NR'].isin(acc_svcs)]

        logger.debug('******************Exiting function PullData')
        return tp20_bid, tp20_bid_shpr, tp20_svc_grp, tp20_ceiling_svc, \
               tp20_shpr_svc, ttpsvgp, zone_weight, tncvcel, tp_accessorial
    except Exception:
        raise

@log_function
def pushData(home, bid_number, host, config, response, tp_accessorial, logger, test=False):
    """
    :param home: TP2_HOME.
    :param host: host is where the database is hosted.
    :param config: config data having all paths in it.
    :param response: dictionary containing information of Accessorials.
    :param tp_accessorial: Data-frame containing tp_accessorials.
    :param logger: Used for logging data(Can see the logs in the command prompt too)
    :param test: Execution mode depends on the boolean value of this attribute(test == True or False)
    """
    acy_table = response[response['Product_Mode'] == 'ACY']
    prod_table = response[response['Product_Mode'] != 'ACY']
    squery = ""
    ssql = ""

    if randomizer(bid_number, config, home):
        # check available accessorials
        if not test:
            db = connect(host)
            cursor = db.cursor()
        # Just bypass when no accessorials are available
        if not acy_table.empty:
            pass

        try:
            # remove accessorial from list
            acc_svcs = tp_accessorial.SVC_GRP_NR.unique()
            prod_table = prod_table[~prod_table['SVC_GRP_NR'].isin(acc_svcs)]

            # loop through each service group to update
            logger.info("start update...")

            # once update once per service group
            prod_table = prod_table[['SVC_GRP_NR', 'Incentive_Freight', 'Target_Low',
                                     'Target_High']].drop_duplicates()

            for index, row in prod_table.iterrows():
                # formatting at %.3f truncates the value to 3 decimal places
                svc_grp_num = row["SVC_GRP_NR"]
                inc_app = 'P'
                incentive = round(row["Incentive_Freight"],3)
                min_inc = round(row["Target_Low"],3)
                max_inc = round(row["Target_High"], 3)


                if test:
                    ssql = ssql + squery + "\n"
                else:
                    Update_Ceilings(cursor, bid_number, svc_grp_num, inc_app, incentive, min_inc, max_inc)

                if test:
                    ssql = ssql + squery + "\n"

            logger.info(str(prod_table.shape[0]) + " rows available to update to TNCVCEL")
        except Exception, e:
            raise RuntimeError(
                'Error 4.1: Oracle DB cannot be updated with query: \n' + squery + 'with error: ' + str(e))

        #add accessorials
        if acy_table.shape[0] != 0:
            try:

                # loop through each service group to update
                logger.info("start update...")
                for index, row in acy_table.iterrows():
                    # NaN test
                    if row["SVC_GRP_NR"] != row["SVC_GRP_NR"]:
                        continue

                    svc_grp_num = row["SVC_GRP_NR"]
                    inc_app = str(row['NCV_DTR_DAT_TYP_CD'])
                    # formatting at %.3f truncates the value to 3 decimal places
                    incentive = round(row["Optimal_UPSInc_0"], 3)
                    min_inc = round(row["Optimal_UPSInc_0"], 3)
                    max_inc = round(row["Optimal_UPSInc_0"], 3)

                    if test:
                        ssql = ssql + squery + "\n"
                    else:
                        Update_Ceilings(cursor, bid_number, svc_grp_num, inc_app, incentive, min_inc, max_inc)
                    #squery = "COMMIT"
                    if test:
                        ssql = ssql + squery + "\n"

                logger.info(str(acy_table.shape[0]) + " accessorial rows available to update to TNCVCEL")
            except Exception, e:
                raise RuntimeError('Error 4.1: Oracle DB cannot be updated with query: \n' + squery +
                                   'with error: ' + str(e))
    else:
        logger.info("Bid number " + bid_number + ": Randomizer determined TP 1.0 incentives are not to be updated.")
        prod_table = prod_table[["BidNumber", "Product", "Incentive_Freight", "Target_Low", "Target_High"]].drop_duplicates()

        for index, row in prod_table.iterrows():
            logger.info("{0}: Inc - {1}, Low - {2}, High - {3}".format(row["Product"], row["Incentive_Freight"],
                                                                       row["Target_Low"], row["Target_High"]))

        # try:
        #     acy_table = acy_table[["BidNumber", "MKG_SVP_DSC_TE", "ASY_SVC_TYP_CD", "RESI", "DAS"]].drop_duplicates()
        #
        #     for index, row in acy_table.iterrows():
        #         if row["ASY_SVC_TYP_CD"] == 'RES':
        #             logger.info("{0}: Inc - {1}".format(row["MKG_SVP_DSC_TE"], row["RESI"]))
        #         elif row["ASY_SVC_TYP_CD"] == 'GDL':
        #             logger.info("{0}: Inc - {1}".format(row["MKG_SVP_DSC_TE"], row["DAS"]))
        # except Exception, e:
        #     pass
    logger.debug('******************Exiting function PushData')
    return ssql

@log_function
def Update_Ceilings(cursor,  bid_number, svc_grp_num, inc_app, incentive, min_inc, max_inc):
    # Single API to update the TNCVCEL instead of multiple copies
    try:
        squery = "UPDATE TNCVCEL " \
                 "SET RCM_NCV_QY = " + str(incentive) + ", NCV_MIN_QY = " + str(min_inc) + \
                 ", NCV_MAX_QY = " + str(max_inc) + \
                 " WHERE NVP_BID_NR = '" + str(bid_number) + \
                 "' AND SVC_GRP_NR = '" + str(svc_grp_num) + \
                 "' AND NCV_DTR_DAT_TYP_CD = '%s'" % inc_app

        cursor.execute(squery)
        squery = "COMMIT"
        cursor.execute(squery)
        logger.debug('******************Exiting function Update_Ceilings')
    except Exception, e:
        raise RuntimeError('Error 4.1: Oracle DB cannot be updated with query: \n' + squery +
                           'with error: ' + str(e))


def randomizer(bid_number, config, home):
    """
    Based on the last digit of the bid_number, the corresponding flag set in the
    configuration file
    is returned by the randomizer function. Configuration file contains flags for all the possible last digits [0-9].

    :param config: config data with the path to randomizer.csv file.
    :param home: TP2_HOME.
    :return: Returns the boolean values.
    """
    last = bid_number[-1:]

    try:
        file_input = home + config["PATHS"]["INPUT_PATH"] + "randomizer.csv"
        file = pd.read_csv(file_input)

        return file.iloc[int(last)]['On'] == True
    except Exception, e:
        print e.message
        return True

@log_function
def transformData(tp20_bid, tp20_bid_shpr, tp20_svc_grp, tp20_shpr_svc, ttpsvgp):
    """
    derives data for columns required by performing calculations
    """
    # create lead account shipper service
    tp20_shpr_svc_leadaccount = tp20_shpr_svc
    tp20_shpr_svc_leadaccount['List_Rev'] = tp20_shpr_svc_leadaccount['SHR_PJT_WVL_QY'] * tp20_shpr_svc_leadaccount[
        'SHR_PJT_GRS_RPP_A']
    tp20_shpr_svc_leadaccount = tp20_shpr_svc_leadaccount.groupby(by=['NVP_BID_NR', 'SHR_AC_NR']).agg(
        {'List_Rev': np.sum})
    tp20_shpr_svc_leadaccount = tp20_shpr_svc_leadaccount.reset_index()
    tp20_shpr_svc_leadaccount = tp20_shpr_svc_leadaccount.sort_values(by = ['NVP_BID_NR', 'List_Rev'], ascending=[1, 0])
    tp20_shpr_svc_leadaccount = tp20_shpr_svc_leadaccount[['NVP_BID_NR', 'SHR_AC_NR']]

    # create prior revenue shipper service
    tp20_shpr_svc_priorrev = tp20_shpr_svc
    tp20_shpr_svc_priorrev['Prior_GRS_Rev'] = tp20_shpr_svc_priorrev['SHR_PRR_GRS_RVN_A']
    tp20_shpr_svc_priorrev['Prior_NET_Rev'] = tp20_shpr_svc_priorrev['SHR_PRR_NET_RVN_A']
    tp20_shpr_svc_priorrev = tp20_shpr_svc_priorrev.groupby(by=['NVP_BID_NR', 'SVC_GRP_NR']).agg(
        {'Prior_GRS_Rev': np.sum, 'Prior_NET_Rev': np.sum})
    tp20_shpr_svc_priorrev = tp20_shpr_svc_priorrev.reset_index()
    tp20_shpr_svc_priorrev = tp20_shpr_svc_priorrev[['NVP_BID_NR','SVC_GRP_NR','Prior_GRS_Rev','Prior_NET_Rev']]

    try:
        Region_array = tp20_bid_shpr[['NVP_BID_NR', 'REG_NR']].groupby(['NVP_BID_NR']).apply(
            lambda g: g.groupby('REG_NR').count().idxmax())
        Region_array.columns = ['Region']
        District_array = tp20_bid_shpr[['NVP_BID_NR', 'DIS_NR']].groupby(['NVP_BID_NR']).apply(
            lambda g: g.groupby('DIS_NR').count().idxmax())
        District_array.columns = ['District']
        LeadAccount_array = tp20_shpr_svc_leadaccount[['NVP_BID_NR', 'SHR_AC_NR']].groupby(['NVP_BID_NR']).apply(
            lambda g: g.groupby('SHR_AC_NR').count().idxmax())
        LeadAccount_array.columns = ['LeadAccount']
        # Step 3.2 : Merge Bid info & Shipper info
        tp20_bid_shpr_Merged = tp20_bid.merge(Region_array, left_on='NVP_BID_NR', right_index=True).merge(
            District_array, left_on='NVP_BID_NR', right_index=True).merge(
            LeadAccount_array, left_on='NVP_BID_NR', right_index=True)
        del Region_array, District_array, LeadAccount_array
    except Exception:
        raise ValueError("No shipper service table.")

    try:
        #ensure category is a string
        tp20_bid_shpr_Merged['NVP_BID_CGY_TYP_CD'] = '0' + tp20_bid_shpr_Merged['NVP_BID_CGY_TYP_CD'].astype(str)

        # Step 3.3 : Recreate TP Bid Table
        tp_bid_table = pd.DataFrame(
            {'BidNumber': tp20_bid_shpr_Merged['NVP_BID_NR'],
             'Status': tp20_bid_shpr_Merged['NVP_BID_STS_CD'],
             'StatusDate': tp20_bid_shpr_Merged['BID_STS_EFF_DT'],
             'ProposedDate': tp20_bid_shpr_Merged['NVP_BID_PRP_DT'],
             'InitiationDate': tp20_bid_shpr_Merged['NVP_BID_INI_DT'],
             'Category': np.where(tp20_bid_shpr_Merged['NVP_BID_CGY_TYP_CD'] == '00', "Not known",
                                  np.where(tp20_bid_shpr_Merged['NVP_BID_CGY_TYP_CD'] == '01', "National",
                                           np.where(tp20_bid_shpr_Merged['NVP_BID_CGY_TYP_CD'] == '02', "Major",
                                                    np.where(tp20_bid_shpr_Merged['NVP_BID_CGY_TYP_CD'] == '03', "Key",
                                                             np.where(
                                                                 tp20_bid_shpr_Merged['NVP_BID_CGY_TYP_CD'] == '04',
                                                                 "Small", "Not known"))))),
             'Region': tp20_bid_shpr_Merged['Region'],
             'District': tp20_bid_shpr_Merged['District'],
             'LeadAccount': tp20_bid_shpr_Merged['LeadAccount'],
             'RequestReason': tp20_bid_shpr_Merged['NVP_BID_RQT_RSN_CD']})

        # %% Step 4: Recreate TP SVC
        """
        Fields that are not able to recreate from IAS:
            - isTargetPriced
            - Offered_Net_Rev_PP (calibration variable)
            - Effective_Offered_Net_Rev_PP (calibration variable)
            - IAS_Min_RPP (need for analysis)
            - IWA_Min_RPP (need for analysis)
        Fields that can't match TP:
            - Prior Gross Rev Wkly
            - Prior Net Rev Wkly

        """
        # Step 4.1: Merge IAS SVC and ttpsvgp
        # Merge variables to ISA SVC tables

        ttpsvgp.drop('PND_STS_CD', 1, inplace=True)
        max_factor = ttpsvgp['TRG_PSE_FCR_NR'].max()
        ttpsvgp = ttpsvgp[ttpsvgp.TRG_PSE_FCR_NR == max_factor]
        ttpsvgp = ttpsvgp.groupby(['NVP_BID_NR', 'SVC_GRP_NR']).agg('max')
        ttpsvgp = ttpsvgp.reset_index()

        tp20_svc_grp = tp20_svc_grp.merge(ttpsvgp, how='left')

        ## Merge Prior Net/Gross Rev
        tp20_svc_grp = tp20_svc_grp.merge(tp20_shpr_svc_priorrev, how='left')

        # Step 4.2: Recreate TP SVC table
        # Create FedEx competitor identifier
        Competitor_Cate = {15: 'FedEx',
                           16: 'FedEx',
                           60: 'FedEx',
                           83: 'FedEx'}

        # Create SVC table
        tp_bid_svc_table = pd.DataFrame(
            {'BidNumber': tp20_svc_grp['NVP_BID_NR'],
             'SVC_GRP_NR': tp20_svc_grp['SVC_GRP_NR'],
             'MVM_DRC_CD': tp20_svc_grp['MVM_DRC_CD'],
             'SVM_TYP_CD': tp20_svc_grp['SVC_TYP_CD'],
             'PKG_CHA_TYP_CD': tp20_svc_grp['PKG_CHA_TYP_CD'],
             'SVC_FEA_TYP_CD': tp20_svc_grp['SVC_FEA_TYP_CD'],
             'True_Density': tp20_svc_grp['CALC_CUBE_PACKAGE_DENSITY'],
             'Competitor': tp20_svc_grp['CPE_CRR_NR'].map(Competitor_Cate).fillna('Other'),
             # Need engineer, a mapping table from UPS?
             'CPE_CRR_NR': tp20_svc_grp['CPE_CRR_NR'],
             'Volume': tp20_svc_grp['BID_UPS_PJT_WVL_QY'],
             'Prior_Volume': tp20_svc_grp['BID_CUS_PRR_WVL_QY'],
             'List_Rev_PP': tp20_svc_grp['NVP_BID_FRT_GRR_A'],
             'List_Rev_PP_Freight': tp20_svc_grp['UPS_PRJ_GRS_RPP_A'],
             'Marginal_Cost_PP': tp20_svc_grp['BID_MRG_CPP_A'],
             'Fully_Allocated_Cost_PP': tp20_svc_grp['BID_FA_CPP_A'],
             'Comp_Keyed_Rev_PP': tp20_svc_grp['CPE_SVC_RPP_A'],
             'Comp_Est_Rev_PP': tp20_svc_grp['CPE_ETM_RPP_A'],
             'Comp_Net_Rev_PP': np.where(tp20_svc_grp['CPE_SVC_RPP_A'] == 0, tp20_svc_grp['CPE_ETM_RPP_A'],
                                         tp20_svc_grp['CPE_SVC_RPP_A']),
             'PriorGrossRevenue': tp20_svc_grp['Prior_GRS_Rev'] / 13,
             'PriorNetRevenue': tp20_svc_grp['Prior_NET_Rev'] / 13,
             'Target_Nominal_Rev_PP': tp20_svc_grp['SVC_GRP_TRG_PSE_A'],
             'Target_Low_Rev_PP': tp20_svc_grp['SVC_TRG_LOW_RNG_A'],
             'Target_High_Rev_PP': tp20_svc_grp['SVC_TRG_HI_RNG_A']
             })

        tp20_shpr_svc = tp20_shpr_svc[['NVP_BID_NR','SVC_GRP_NR','RA_TRI_NR']]
        tp20_shpr_svc = tp20_shpr_svc.groupby(by=['NVP_BID_NR', 'SVC_GRP_NR']).agg(
            {'RA_TRI_NR': lambda x: x.value_counts().index[0]}).reset_index()
        tp_bid_svc_table = tp_bid_svc_table.merge(tp20_shpr_svc, how="left")
        tp_bid_svc_table = tp_bid_svc_table.rename(columns={'RA_TRI_NR':'Billing_Tier'})
        logger.debug('******************Exiting function transformData')
        return tp_bid_table, tp_bid_svc_table
    except Exception:
        raise

## master dataset creation piece
def trace(msg):
    """Print debugging messages with timestamp"""
    import datetime
    print('[{:%Y-%m-%d %H:%M:%S}]: '.format(datetime.datetime.now()) + msg)

@log_function
def zone_weight_table(home, input, tp20_svc_grp):
    try:
        svc_to_prod = pd.read_csv(home + '/data/inputs/svc_to_prod.csv')
        merged_prod =  pd.merge(tp20_svc_grp, svc_to_prod, on=['SVC_FEA_TYP_CD','MVM_DRC_CD','SVM_TYP_CD'])

        bid_number = input['NVP_BID_NR'].values[0]
        input = pd.merge(input, merged_prod)
    except Exception:
        raise ValueError("No services to score")

    try:
        zone_list = ['2_pct',
                     '3_pct',
                     '4_pct',
                     '5_pct',
                     '6_pct',
                     '7_pct',
                     '8_pct',
                     '46_pct',
                     '47_pct',
                     '48_pct']
        bin_vals = [0, 0.5, 1, 2, 3, 4, 5, 6, 10, 15, 20, 25, 30, 40, 50, 75, 100]
        bin_labels = ['0_0_5_lbs',
                      '0_5625_1_lbs',
                      '2_2_lbs',
                      '3_3_lbs',
                      '4_4_lbs',
                      '5_5_lbs',
                      '6_10_lbs',
                      '11_15_lbs',
                      '16_20_lbs',
                      '21_25_lbs',
                      '26_30_lbs',
                      '31_40_lbs',
                      '41_50_lbs',
                      '51_75_lbs',
                      '76_100_lbs',
                      '101_535068_lbs']

        bin_labels_pct = [x + '_pct' for x in bin_labels]

        # update weight from 0 to 0.01 for all weights
        input['WEIGHT'] = input['WEIGHT'].replace(0,0.01)

        input['WeightPkg'] = input.WEIGHT * input.PKGVOL
        zone = input.groupby(by=['NVP_BID_NR', 'Product', 'DEL_ZN_NR']).agg({'WeightPkg': np.sum})
        zone['ZonePct'] = zone.groupby(level=1).apply(lambda x: x / float(x.sum()))

        zone = zone.reset_index()

        zone['DEL_ZN_NR'] = zone['DEL_ZN_NR'].astype(str) + '_pct'

        zone = zone.pivot(index='Product', columns='DEL_ZN_NR', values='ZonePct')
        for z in zone_list:
            if z not in zone:
                zone[z] = np.nan

        zone = zone.fillna(0.0)
        zone['BidNumber'] = bid_number

        weight = input.groupby(by=['NVP_BID_NR', 'Product', 'WEIGHT']).agg({'PKGVOL': np.sum})
        weight = weight.reset_index()

        weight['Weight_Bin'] = pd.cut(weight['WEIGHT'], bins=bin_vals, labels=bin_labels)

        weight = weight.groupby(by=['NVP_BID_NR','Product', 'Weight_Bin']).agg({'PKGVOL': np.sum})
        weight['Weight_Pct'] = weight.groupby(level=1).apply(lambda x: x / float(x.sum()))
        weight = weight.reset_index()

        weight['Weight_Bin'] = weight['Weight_Bin'].astype(str) + '_pct'
        weight = weight.pivot(index='Product', columns='Weight_Bin', values='Weight_Pct')

        for w in bin_labels_pct:
            if w not in weight:
                weight[w] = np.nan

        weight = weight.fillna(0.0)
        weight['BidNumber'] = bid_number

        zone_weight = weight.reset_index().merge(zone.reset_index())
    except Exception:
        raise ValueError("Zone weight table incorrect")

    logger.debug('******************Exiting fucntion zone_weight_table')
    return zone_weight

@log_function
def create_prod_base_tables(home, bid_account_raw, bid_service_raw, zone_weight=None, tp20_svc_grp=None):
    bid_master_dd, product_master_sf = generate_base_tables(home, bid_account_raw, bid_service_raw)
    # Create temporary bid dummy data
    bid_master_dd['Bid_AE_Hist_Num_Bids'] = '1'
    bid_master_dd['Bid_Overall_Incentive'] = '1'
    bid_master_dd['Bid_Incentive_Freight'] = '1'

    # Create temporary product dummy data
    try:
        if zone_weight is None:
            #pre-process class will insert average values for columns with 'none' data. will happen in the
            #econ model step

            product_master_sf['0_0_5_lbs_pct'] = None
            product_master_sf['0_5625_1_lbs_pct'] = None
            product_master_sf['2_2_lbs_pct'] = None
            product_master_sf['3_3_lbs_pct'] = None
            product_master_sf['4_4_lbs_pct'] = None
            product_master_sf['5_5_lbs_pct'] = None
            product_master_sf['6_10_lbs_pct'] = None
            product_master_sf['11_15_lbs_pct'] = None
            product_master_sf['16_20_lbs_pct'] = None
            product_master_sf['21_25_lbs_pct'] = None
            product_master_sf['26_30_lbs_pct'] = None
            product_master_sf['31_40_lbs_pct'] = None
            product_master_sf['41_50_lbs_pct'] = None
            product_master_sf['51_75_lbs_pct'] = None
            product_master_sf['76_100_lbs_pct'] = None
            product_master_sf['101_535068_lbs_pct'] = None
            product_master_sf['2_pct'] = None
            product_master_sf['3_pct'] = None
            product_master_sf['4_pct'] = None
            product_master_sf['5_pct'] = None
            product_master_sf['6_pct'] = None
            product_master_sf['7_pct'] = None
            product_master_sf['8_pct'] = None
            product_master_sf['44_pct'] = None
            product_master_sf['45_pct'] = None
            product_master_sf['46_pct'] = None
        else:
            try:
                tp20_svc_grp['SVM_TYP_CD'] = tp20_svc_grp['SVC_TYP_CD']
                product_zw = zone_weight_table(home, zone_weight, tp20_svc_grp)
                product_master_sf = product_master_sf.merge(product_zw, how ='left')
            except:
                # pre-process class will insert average values for columns with 'none' data. will happen in the
                # econ model step

                product_master_sf['0_0_5_lbs_pct'] = None
                product_master_sf['0_5625_1_lbs_pct'] = None
                product_master_sf['2_2_lbs_pct'] = None
                product_master_sf['3_3_lbs_pct'] = None
                product_master_sf['4_4_lbs_pct'] = None
                product_master_sf['5_5_lbs_pct'] = None
                product_master_sf['6_10_lbs_pct'] = None
                product_master_sf['11_15_lbs_pct'] = None
                product_master_sf['16_20_lbs_pct'] = None
                product_master_sf['21_25_lbs_pct'] = None
                product_master_sf['26_30_lbs_pct'] = None
                product_master_sf['31_40_lbs_pct'] = None
                product_master_sf['41_50_lbs_pct'] = None
                product_master_sf['51_75_lbs_pct'] = None
                product_master_sf['76_100_lbs_pct'] = None
                product_master_sf['101_535068_lbs_pct'] = None
                product_master_sf['2_pct'] = None
                product_master_sf['3_pct'] = None
                product_master_sf['4_pct'] = None
                product_master_sf['5_pct'] = None
                product_master_sf['6_pct'] = None
                product_master_sf['7_pct'] = None
                product_master_sf['8_pct'] = None
                product_master_sf['44_pct'] = None
                product_master_sf['45_pct'] = None
                product_master_sf['46_pct'] = None
    except Exception:
        raise ValueError("Service group and zone weight creation error.")

    product_master_sf['Off_Net_Rev_wkly'] = 1
    product_master_sf['Overall_Incentive'] = 1
    product_master_sf['Incentive_Freight'] = 1
    product_master_sf['Act_Rev_Wkly'] = 1
    product_master_sf['Act_Vol_Wkly'] = 1

    logger.debug('******************Exiting function create_prod_base_tables')
    return final_output(bid_master_dd, product_master_sf)

@log_function
def generate_base_tables(home, bid_account_raw, bid_service_raw):
    input_dir = home + '/data/inputs/'
    #trace('Loading data')

    #load data first
    try:
        svc_to_prod = pd.read_csv(input_dir + 'svc_to_prod.csv')
        #crossware_pld_bid_list = pd.read_csv(input_dir + 'Crossware_PLD_Bid_List.csv')
    except Exception, e:
        print e
        svc_to_prod = pd.DataFrame()
        #crossware_pld_bid_list = pd.DataFrame()

    #trace('Entering _clean_bid_account')
    bid_account = clean_bid_account(bid_account_raw) # Do we need this step?
    #trace('Entering _clean_bid_service')
    bid_service = clean_bid_service(bid_service_raw, bid_account, svc_to_prod) # Do we need this step?

    try:
        #trace('Entering _generate_product_initial')
        product_initial = generate_product_initial(bid_service)

        #trace('Entering _generate_bid_master')
        bid_master = generate_bid_master(bid_account, product_initial)

        #trace('Entering _generate_product_master')
        product_master = product_initial
    except Exception as e:
        raise ValueError("Bid service groups contain incorrect data: %s" % e.message)
    logger.debug('******************Exiting function generate_base_tables')
    return bid_master, product_master

#<editor-fold desc="Step 1: Base table prep from TP data">
@log_function
def clean_bid_account(bid_account_raw):
    # Don't process table twice
    if 'Proposed_Lag' in bid_account_raw.columns:
        return bid_account_raw  # already processed
    if 'RequestReason' not in bid_account_raw.columns:
        #trace('Request Reason is not in bid_account_raw, imputed all by 1')
        bid_account_raw['RequestReason'] = 1
    # Ensure that expected columns are present and select
    ba_cols = ['BidNumber', 'Status', 'StatusDate', 'ProposedDate', 'InitiationDate', 'Category', 'Region', 'District',
               'RequestReason','LeadAccount']

    bid_account = bid_account_raw[ba_cols]

    # Coerce datetime columns
    # 'coerce' turns unparseable dts into NaT (some input dates out of bounds)
    dt_cols = ['InitiationDate', 'ProposedDate', 'StatusDate']
    bid_account[dt_cols] = bid_account[dt_cols].\
        apply(lambda x: pd.to_datetime(x, errors='coerce'))

    # Find latest date
    lastDate = bid_account.StatusDate.max(skipna=True)

    # Transform bid_account_raw
    ba = bid_account  # alias for brevity
    ba['Proposed_Lag'] = (lastDate - ba.ProposedDate).dt.days

    # most bids are not explicitly rejected, but rarely are bids won > 90 days
    ba['Status_Updated'] = np.where((ba.Status == 'P') & (ba.Proposed_Lag > 90),
                                    'R', ba.Status)
    ba['ProposalBid_Flag'] = np.where(ba.Status == 'P', 1, 0)
    win_flag_map = {'A': '1', #A = Accepted
                    'C': '1', #C = Cancelled, previously accepted
                    'O': '1', #O = Expired, previously accepted
                    'R': '0'} #R = Rejected

    ba['Win_Flag'] = ba.Status.map(win_flag_map).fillna('Unclassified')
    ba['Win_Flag_Updated'] = ba.Status_Updated.map(win_flag_map).fillna('Unclassified')
    # Corercing RequestReason to int first (dirty data)
    ba['Request_Reason_Descr'] = pd.to_numeric(ba.RequestReason,
                                               errors='coerce',
                                               downcast='integer').map({1: 'RETENTION',
                                                                        2: 'PENETRATION',
                                                                        3: 'CONVERSION'}).fillna('Other')
    ba['Wks_since_Init'] = (ba.StatusDate - ba.InitiationDate).dt.days / 7.
    ba['Month_of_Bid'] = ba.InitiationDate.dt.month
    logger.debug('******************Exiting fucntion clean_bid_account')
    return ba

@log_function
def clean_bid_service(bid_service_raw,  bid_account,  svc_to_prod):
    # Don't process table twice
    if 'Product' in bid_service_raw.columns:
        return bid_service_raw  # already processed

    bid_service = bid_service_raw

    # Trim whitespace from Competitor column
    bid_service['Competitor'] = bid_service.Competitor.str.strip()

    # Attach product info to service table
    stp_cols = ['MVM_DRC_CD', 'SVM_TYP_CD', 'SVC_FEA_TYP_CD',
                'Product']  # columns _required_ in svc_to_prod
    bid_service = pd.merge(bid_service, svc_to_prod[stp_cols],
                           on=['MVM_DRC_CD', 'SVM_TYP_CD', 'SVC_FEA_TYP_CD'],
                           how='left') #possible to-dos given time: make this inner join, so that the service line is ignored; create error msg

    # Filter out unreliable data
    bid_service = bid_service[~bid_service.Product.isin(['GND_Unclassified',
                                                         'Unknown'])]

    logger.debug('******************Exiting function clean_bid_service')
    return bid_service

@log_function
def generate_product_initial(bid_service):
    np.seterr(invalid='ignore')

    product_initial = bid_service[~pd.isnull(bid_service.Product)].copy()  # explicit copy to avoid warnings, could investigate avoiding copy in future
    product_initial['FedEx_Flag'] = np.where(product_initial.Competitor == 'FedEx', 1, 0)
    #trace('Starting main aggregation in _generate_product_initial')
    # Columns to aggregate
    #trace('Adding columns to aggregate')
    product_initial['List_Rev_Wkly'] = product_initial['List_Rev_PP'] * product_initial['Volume']

    product_initial.loc[product_initial['List_Rev_Wkly'] == 0, 'List_Rev_Wkly'] = 0.01

    product_initial['List_Rev_Freight_wkly'] = (product_initial['List_Rev_PP_Freight'] *
                                                product_initial['Volume'])

    product_initial.loc[product_initial['List_Rev_Freight_wkly'] == 0, 'List_Rev_Freight_wkly'] = 0.01

    product_initial['Pct_FedEx'] = (product_initial['FedEx_Flag'] *
                                    product_initial['List_Rev_Wkly'])
    product_initial['Marginal_Cost_wkly'] = (product_initial['Marginal_Cost_PP'] *
                                             product_initial['Volume'])
    product_initial['Total_Volume_wkly'] = product_initial['Volume']
    product_initial['Total_prior_Volume_wkly'] = product_initial['Prior_Volume']
    product_initial['Comp_Net_Rev_wkly'] = (product_initial['Comp_Net_Rev_PP'] *
                                            product_initial['Volume'])
    product_initial['Target_High_Rev_wkly'] = (product_initial['Target_High_Rev_PP'] *
                                               product_initial['Volume'])
    product_initial['Target_Low_Rev_wkly'] = (product_initial['Target_Low_Rev_PP'] *
                                              product_initial['Volume'])
    product_initial['PriorNetRevenue_wkly'] = product_initial['PriorNetRevenue']
    product_initial['PriorGrossRevenue_wkly'] = product_initial['PriorGrossRevenue']
    product_initial['Comp_Keyed_Rev_wkly'] = (product_initial['Comp_Keyed_Rev_PP'] *
                                              product_initial['Volume'])
    product_initial['Comp_Est_Rev_wkly'] = (product_initial['Comp_Est_Rev_PP'] *
                                            product_initial['Volume'])
    product_initial['Density_Sum'] = (product_initial['True_Density'] *
                                      product_initial['List_Rev_Wkly'])
    #empty rows are filled with zeros
    product_initial = product_initial.fillna(0)

    # Sum weekly columns
    #trace('Summing weekly columns')
    grouped = product_initial.groupby(['BidNumber', 'Product'])
    prod_summ = grouped[['List_Rev_Wkly',
                         'List_Rev_Freight_wkly',
                         'Pct_FedEx',
                         'Marginal_Cost_wkly',
                         'Total_Volume_wkly',
                         'Total_prior_Volume_wkly',
                         'Comp_Net_Rev_wkly',
                         'Target_High_Rev_wkly',
                         'Target_Low_Rev_wkly',
                         'PriorNetRevenue_wkly',
                         'PriorGrossRevenue_wkly',
                         'Comp_Keyed_Rev_wkly',
                         'Comp_Est_Rev_wkly',
                         'Density_Sum']].sum()

    # Compute columns with denominators
    #trace('Computing columns with denominators')
    prod_summ['Pct_FedEx'] = prod_summ['Pct_FedEx'] / prod_summ['List_Rev_Wkly']

    # Compute additional columns
    #trace('Computing additional columns')
    prod_summ['TargetHigh_Inc'] = 1. - prod_summ['Target_High_Rev_wkly']*1./prod_summ['List_Rev_Freight_wkly']
    prod_summ['TargetLow_Inc'] = 1. - prod_summ['Target_Low_Rev_wkly']*1./prod_summ['List_Rev_Freight_wkly']
    prod_summ['Prior_Incentive'] = 1. - prod_summ['PriorNetRevenue_wkly']*1./prod_summ['PriorGrossRevenue_wkly']
    prod_summ['Pct_New_Vol'] = np.maximum(1. - prod_summ['Total_prior_Volume_wkly']*1./prod_summ['Total_Volume_wkly'], 0.)
    prod_summ['Gross_OR_Ratio'] = prod_summ['Marginal_Cost_wkly']*1. / prod_summ['List_Rev_Wkly']
    prod_summ['Pct_Rev_Freight'] = prod_summ['List_Rev_Freight_wkly']*1. / prod_summ['List_Rev_Wkly']
    prod_summ['Pct_Rev_Freight_Centered'] = prod_summ['Pct_Rev_Freight'] - np.mean(prod_summ['Pct_Rev_Freight'])
    prod_summ['Target_High_Incentive'] = 1. - prod_summ['Target_High_Rev_wkly']*1./prod_summ['List_Rev_Freight_wkly']
    prod_summ['True_Density'] = prod_summ['Density_Sum'] / prod_summ['List_Rev_Wkly']

    # possible to-dos given time: Don't hard code threshold - create ETL section of TP2_Parameter_settings.xlsx
    prod_summ['Comp_FedEx'] = np.where(prod_summ['Pct_FedEx'] > 0.95, 1, 0)
    prod_summ['Comp_3rdparty'] = np.where(prod_summ['Pct_FedEx'] < 0.2, 1, 0)

    #Comp_Net_Rev_wkly is after reality check on comp AE keyed and requires values be within 20pts Comp Est Weekly
    prod_summ['Comp_Incentive'] = np.maximum(1. - prod_summ.Comp_Net_Rev_wkly*1./prod_summ.List_Rev_Freight_wkly, 0.)
    prod_summ['Comp_AE_Keyed_Flag'] = np.where(prod_summ.Comp_Keyed_Rev_wkly > 0, 1, 0)
    prod_summ['Comp_AE_Keyed_Incentive']= np.where(prod_summ.Comp_AE_Keyed_Flag == 1,
                                                   np.maximum(1.0 - prod_summ.Comp_Keyed_Rev_wkly*1./prod_summ.List_Rev_Freight_wkly, 0.), np.NaN)


    # Compute MajorCompetitor
    #trace('Computing Major_Competitor')
    comp_sums = product_initial.groupby(['BidNumber', 'Product',
                                         'Competitor'])['List_Rev_Wkly'].sum()
    comp_ixs = comp_sums.groupby(level=[0, 1]).idxmax()
    comp_df = pd.DataFrame(comp_ixs.tolist(), columns=['BidNumber', 'Product', 'Competitor'], index=comp_ixs.index)
    prod_summ['Major_Competitor'] = comp_df['Competitor']

    #trace('Finished computing Major_Competitor')

    prod_summ = prod_summ.reset_index()

    # Computer Billing Tier
    cwt_density_bt_prod = grouped.aggregate({
            'Billing_Tier' : lambda p: p.groupby(p).count().idxmax()}).reset_index()
    cwt_density_bt_prod = cwt_density_bt_prod[['BidNumber','Product','Billing_Tier']]

    # Merge Billing_Tier to product master
    prod_summ = pd.merge(prod_summ,
                         cwt_density_bt_prod,
                         on=['BidNumber','Product'],
                         how='left')

    product_initial = prod_summ.reset_index()

    # trace('Finished main aggregation in _generate_product_initial')

    # Bid-level revenue
    # trace('Computing bid-level revenue in _generate_product_initial')
    # Create Mode columns
    mode_df = product_initial[['BidNumber', 'Product', 'List_Rev_Wkly']].copy()  # explicit copy to avoid warning
    mode_df['Mode_GND_ListRev'] = np.where(
        mode_df.Product.isin(['GND_Com', 'GND_Resi', 'GND_USPS']),
        mode_df['List_Rev_Wkly'], 0.)
    mode_df['Mode_Air_ListRev'] = np.where(
        mode_df.Product.isin(['1DA', '2DA', '3DA']),
        mode_df['List_Rev_Wkly'], 0.)
    mode_df['Mode_Imp_Exp_ListRev'] = np.where(
        mode_df.Product.isin(['Import', 'Export']),
        mode_df['List_Rev_Wkly'], 0.)
    # Summarize and join Mode columns at bid level
    sum_cols = ['Mode_GND_ListRev', 'Mode_Air_ListRev', 'Mode_Imp_Exp_ListRev',
                'List_Rev_Wkly']
    sums_df = mode_df.groupby('BidNumber')[sum_cols].sum().reset_index()
    sums_df = sums_df.rename(columns={'List_Rev_Wkly': 'Bid_List_Rev_Wkly'})  # rename before join
    product_initial = pd.merge(product_initial, sums_df,
                               on='BidNumber')

    product_initial['Pct_Product_Rev'] = product_initial['List_Rev_Wkly'] / product_initial['Bid_List_Rev_Wkly']
    product_initial['Mode_Weight'] = np.where(product_initial.Product.isin(['GND_Com', 'GND_Resi', 'GND_USPS']), product_initial['List_Rev_Wkly']/product_initial['Mode_GND_ListRev'],
                                              np.where(product_initial.Product.isin(['1DA', '2DA', '3DA']), product_initial['List_Rev_Wkly']/product_initial['Mode_Air_ListRev'],
                                                       product_initial['List_Rev_Wkly']/product_initial['Mode_Imp_Exp_ListRev']))

    # Calculate product mode field:
    product_initial['Product_Mode'] = product_initial.Product.map({
        'GND_Com': 'GND',
        'GND_Resi': 'GND',
        'GND_USPS': 'GND',
        'GND_CWT': 'GND_CWT',
        '1DA': 'AIR',
        '2DA': 'AIR',
        '3DA': 'AIR',
        'Air_CWT': 'AIR_CWT',
        'Import': 'IE',
        'Export': 'IE'
    }).fillna('Unknown') #possible to-dos given time: Default for anything should be GND_Com
    # Create unique key
    product_initial['BidNumberProduct'] = product_initial['BidNumber'] + '_' + product_initial['Product']

    # Bid_List_Rev_Wkly will become redundant when merging with bid_master
    product_initial.drop('Bid_List_Rev_Wkly', axis=1, inplace=True)
    logger.debug('******************Exiting function clean_bid_service')
    return product_initial

@log_function
def generate_bid_master(bid_account, product_initial):
    bid_master = product_initial[~pd.isnull(product_initial.Product)]

    #trace('Starting main aggregation in _generate_bid_master')
    # Multiply some columns by revenue for summing
    bid_master['Pct_FedEx'] = bid_master['Pct_FedEx'] * bid_master['List_Rev_Wkly']
    # Creating mode percentage columns to sum
    bid_master['Dom_GND_Pct_Rev'] = np.where(bid_master.Product.isin(['GND_Com', 'GND_Resi', 'GND_USPS']), bid_master.Pct_Product_Rev, 0)
    bid_master['Dom_AIR_Pct_Rev'] = np.where(bid_master.Product.isin(['1DA', '2DA', '3DA']), bid_master.Pct_Product_Rev, 0)
    bid_master['Dom_GND_Resi_USPS_Pct_Rev'] = np.where(bid_master.Product.isin(['GND_Resi', 'GND_USPS']), bid_master.Pct_Product_Rev, 0)
    bid_master['Imp_Exp_Pct_Rev'] = np.where(bid_master.Product.isin(['Import', 'Export']), bid_master.Pct_Product_Rev, 0)
    bid_master['AIR_CWT_Pct_Rev'] = np.where(bid_master.Product.isin(['Air_CWT', ]), bid_master.Pct_Product_Rev, 0)
    bid_master['GND_CWT_Pct_Rev'] = np.where(bid_master.Product.isin(['GND_CWT', ]), bid_master.Pct_Product_Rev, 0)
    bid_master['Product_Concentration'] = bid_master['Pct_Product_Rev'] ** 2

    # Sum columns
    #trace('Summing columns')
    grouped = bid_master.groupby('BidNumber')
    bid_summ = grouped[['List_Rev_Wkly',
                        'List_Rev_Freight_wkly',
                        'Pct_FedEx',
                        'Marginal_Cost_wkly',
                        'Total_Volume_wkly',
                        'Total_prior_Volume_wkly',
                        'Comp_Net_Rev_wkly',
                        'Target_High_Rev_wkly',
                        'Target_Low_Rev_wkly',
                        'PriorNetRevenue_wkly',
                        'PriorGrossRevenue_wkly',
                        'Comp_Keyed_Rev_wkly',
                        'Comp_Est_Rev_wkly',
                        'Dom_GND_Pct_Rev',
                        'Dom_AIR_Pct_Rev',
                        'Dom_GND_Resi_USPS_Pct_Rev',
                        'Imp_Exp_Pct_Rev',
                        'AIR_CWT_Pct_Rev',
                        'GND_CWT_Pct_Rev',
                        'Product_Concentration']].sum()

    # Compute columns with denominators
    #trace('Computing columns with denominators')
    bid_summ['Pct_FedEx'] = bid_summ['Pct_FedEx'] / bid_summ['List_Rev_Wkly']

    # Compute additional columns
    #trace('Computing additional columns')
    bid_summ['Prior_Incentive'] = 1. - bid_summ['PriorNetRevenue_wkly']*1./bid_summ['PriorGrossRevenue_wkly']
    bid_summ['Pct_New_Vol'] = np.maximum(1. - bid_summ['Total_prior_Volume_wkly']*1./bid_summ['Total_Volume_wkly'], 0.)
    bid_summ['Product_ReqReason_Desc'] = np.where(bid_summ['Pct_New_Vol'] > 0.6, 'CONVERSION_Prod',
                                                  np.where(bid_summ['Pct_New_Vol'] <= 0.2, 'RETENTION_Prod',
                                                           'PENETRATION_Prod'))
    bid_summ['Gross_OR_Ratio'] = bid_summ['Marginal_Cost_wkly']*1. / bid_summ['List_Rev_Wkly']
    bid_summ['Pct_Rev_Freight'] = bid_summ['List_Rev_Freight_wkly']*1. / bid_summ['List_Rev_Wkly']

    #possible to-dos given time: hard code should go into INI file
    bid_summ['Comp_FedEx'] = np.where(bid_summ['Pct_FedEx'] > 0.95, 1, 0)
    bid_summ['Comp_3rdparty'] = np.where(bid_summ['Pct_FedEx'] < 0.2, 1, 0)
    bid_summ['Dom_GND_Centric_Flag'] = np.where(bid_summ['Dom_GND_Pct_Rev'] > 0.8, 1, 0)
    bid_summ['Dom_AIR_Centric_Flag'] = np.where(bid_summ['Dom_AIR_Pct_Rev'] > 0.6, 1, 0)
    bid_summ['Dom_GND_Resi_USPS_Centric_Flag'] = np.where(bid_summ['Dom_GND_Resi_USPS_Pct_Rev'] > 0.6, 1, 0)
    bid_summ['Imp_Exp_Centric_Flag'] = np.where(bid_summ['Imp_Exp_Pct_Rev'] > 0.4, 1, 0)

    #print bid_master[['BidNumber', 'Major_Competitor','List_Rev_Wkly']]

    # This is equivalent to
    #trace('Computing Major_Competitor')
    comp_sums = bid_master.groupby(['BidNumber', 'Major_Competitor'])['List_Rev_Wkly'].sum()
    comp_ixs = comp_sums.groupby(level=0).idxmax()
    comp_df = pd.DataFrame(comp_ixs.tolist(),
                           columns=['BidNumber', 'Major_Competitor'],
                           index=comp_ixs.index)

    bid_summ['Major_Competitor'] = comp_df['Major_Competitor']
    #trace('Finished computing Major_Competitor')

    bid_master = bid_summ.reset_index()

    #trace('Finished main aggregation in _generate_bid_master')

    # Join bid_master with bid_account
    # Join bid_master with bid_account
    bid_master = pd.merge(bid_master, bid_account,
                          on='BidNumber', how='left')

    bid_master['Comp_Rev_Keyed_Flag'] = np.where(pd.isnull(bid_master.Comp_Keyed_Rev_wkly), 0,
                                                 np.where((bid_master.Comp_Keyed_Rev_wkly == 0) &
                                                          (bid_master.Comp_Est_Rev_wkly > 1),
                                                          0, 1))

    #1.2 and 0.8 factors aligns with TP 1.0 factors
    bid_master['Comp_Rev_Keyed_Higher_Flag'] = np.where(bid_master.Comp_Keyed_Rev_wkly > bid_master.Comp_Est_Rev_wkly*1.2,
                                                        1, 0)
    bid_master['Comp_Rev_Keyed_Lower_Flag'] = np.where(bid_master.Comp_Keyed_Rev_wkly < bid_master.Comp_Est_Rev_wkly*0.8,
                                                       1, 0)
    bid_master['List_Rev_Wkly_LessThan200'] = np.where(bid_master.List_Rev_Wkly < 200,
                                                       1, 0)
    # Add prefix "Bid" to all bid-level features
    bid_master.rename(columns=lambda col: 'Bid_' + col if col != 'BidNumber' else col, inplace=True)
    logger.debug('******************generate_bid_master')
    return bid_master

@log_function
def final_output(bid_master_dd, product_master_sf):
    bid_master_final = bid_master_dd
    # Join product master with bid master
    product_master_final = pd.merge(product_master_sf, bid_master_final,
                                    on='BidNumber', how='inner')

    # Drop duplicated columns
    bid_master_final = bid_master_final.loc[:, ~bid_master_final.columns.duplicated()] # Keep the first column
    product_master_final = product_master_final.loc[:, ~product_master_final.columns.duplicated()] # Keep the first column

    ### Imputations
    # Prior Incentive
    product_master_final.loc[(product_master_final['Prior_Incentive'].isnull()) &
                             (product_master_final['Bid_Request_Reason_Descr'] == 'CONVERSION'),
                             'Prior_Incentive'] = 0
    bid_master_final.loc[(bid_master_final['Bid_Prior_Incentive'].isnull()) &
                         (bid_master_final['Bid_Request_Reason_Descr'] == 'CONVERSION'),
                         'Bid_Prior_Incentive'] = 0
    logger.debug('******************Exiting function final_output')
    return product_master_final

if __name__ == '__main__':
    from error import checkErrors
    cError, successMsg, errorMsg, logger, config, home = checkErrors()

    bidNumber = str(sys.argv[1])

    print "Bid acccording to randomizer: " + str(randomizer(bidNumber, config, home))
