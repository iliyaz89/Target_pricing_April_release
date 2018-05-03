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

import cPickle as pickle
import numpy as np
import pandas as pd


class cwt_production_class(object):
    """
    Preloads CWT incentive cutoffs and runs CWT incentive matches
    """

    def __init__(self, cwt_pickle, svc_to_prod, settings):
        with open(cwt_pickle, "rb") as pickle_file:
            self.air_bt_threshold, self.air_density_threshold, self.air_size_threshold, self.air_cohort_map, \
            self.air_incentive_map, self.gnd_bt_threshold, self.gnd_density_threshold, self.gnd_size_threshold, \
            self.gnd_cohort_map, self.gnd_incentive_map = pickle.load(pickle_file)

        self.svc_to_prod = svc_to_prod
        self.low_range = settings['POSTPROCESSING']['Inc_spread_low']
        self.high_range = settings['POSTPROCESSING']['Inc_spread_high']

    def scorer(self, input_data):
        """
        Scores the input data for CWT incentives
        
        :param input_data: pandas dataframe containing master dataset items related to Air/GND CWT
        :return: pandas dataframe with incnetive, target_high, target_low appended
        """
        input_data['Target_High'] = np.NaN
        input_data['Target_Low'] = np.NaN

        # Row Iteration starts there:
        for index, rows in input_data.iterrows():
            cwt_incentive = 0.0

            rows = pd.DataFrame(rows).transpose()
            prod_check = rows['Product'].values[0]
            #print prod_check
            #print rows['True_Density']
            #print rows['Billing_Tier']

            if (prod_check != 'Air_CWT') & (prod_check != 'GND_CWT'):
                continue
            else:
                # have defaults in case there is no density or billing tier
                rows['True_Density'] = rows['True_Density'].fillna(1)
                rows['Billing_Tier'] = rows['Billing_Tier'].fillna(6)

                if prod_check == "Air_CWT":
                    score_data = rows[rows['Product'] == 'Air_CWT']
                    # Merge BT_SEGMENT
                    score_data = pd.merge(score_data, self.air_bt_threshold[['COMMODITY TIER', 'BT_SEGMENT']],
                                          left_on='Billing_Tier',
                                          right_on='COMMODITY TIER',
                                          how='inner')
                    #print "Billing tier"

                    # Merge SIZE_SEGMENT
                    bid_list = score_data['Bid_List_Rev_Wkly'].values[0]
                    #print bid_list
                    #print self.air_size_threshold

                    score_data['SIZE_SEGMENT'] = \
                        self.air_size_threshold[(self.air_size_threshold['MIN VALUE'] <= bid_list) &
                                                (self.air_size_threshold['MAX VALUE'] >
                                                 bid_list)].loc[:, 'SIZE_SEGMENT'].values[0]

                    #print "Size"

                    # Merge DEN_SEGMENT
                    true_density = score_data['True_Density'].values[0]
                    score_data['DEN_SEGMENT'] = \
                        self.air_density_threshold[(self.air_density_threshold['MIN VALUE'] <= true_density) &
                                                   (self.air_density_threshold['MAX VALUE'] >
                                                    true_density)].loc[:, 'DEN_SEGMENT'].values[0]
                    #print "Density"

                    # Merge Cohort
                    cohort_cols = ['BT_SEGMENT', 'DEN_SEGMENT', 'SIZE_SEGMENT', 'COHORT']
                    score_data = pd.merge(score_data, self.air_cohort_map[cohort_cols],
                                          on=['BT_SEGMENT', 'SIZE_SEGMENT', 'DEN_SEGMENT'],
                                          how='inner')
                    #print "Cohort"

                    # Merge Incentive
                    score_data = pd.merge(score_data, self.air_incentive_map,
                                          on=['COHORT'],
                                          how='inner')
                    #print "Incentive"
                    cwt_incentive = score_data['INCENTIVE'].values[0]
                else:
                    score_data = rows[rows['Product'] == 'GND_CWT']

                    # Merge BT_SEGMENT
                    score_data = pd.merge(score_data, self.gnd_bt_threshold[['COMMODITY TIER', 'BT_SEGMENT']],
                                          left_on='Billing_Tier',
                                          right_on='COMMODITY TIER',
                                          how='inner')

                    # Merge SIZE_SEGMENT
                    bid_list = score_data['Bid_List_Rev_Wkly'].values[0]
                    score_data['SIZE_SEGMENT'] = \
                        self.gnd_size_threshold[(self.gnd_size_threshold['MIN VALUE'] <= bid_list) &
                                                (self.gnd_size_threshold['MAX VALUE'] >
                                                 bid_list)].loc[:, 'SIZE_SEGMENT'].values[0]

                    # Merge DEN_SEGMENT
                    true_density = score_data['True_Density'].values[0]
                    score_data['DEN_SEGMENT'] = \
                        self.gnd_density_threshold[(self.gnd_density_threshold['MIN VALUE'] <= true_density) &
                                                   (self.gnd_density_threshold['MAX VALUE'] >
                                                    true_density)].loc[:, 'DEN_SEGMENT'].values[0]

                    # Merge Cohort
                    cohort_cols = ['BT_SEGMENT', 'DEN_SEGMENT', 'SIZE_SEGMENT', 'COHORT']
                    score_data = pd.merge(score_data, self.gnd_cohort_map[cohort_cols],
                                          on=['BT_SEGMENT', 'SIZE_SEGMENT', 'DEN_SEGMENT'],
                                          how='inner')
                    # Merge Incentive
                    score_data = pd.merge(score_data, self.gnd_incentive_map,
                                          on=['COHORT'],
                                          how='inner')
                    cwt_incentive = score_data['INCENTIVE'].values[0]

            input_data.loc[index, ['Incentive_Freight']] = cwt_incentive
            input_data.loc[index, ['Target_High']] = cwt_incentive + self.high_range
            input_data.loc[index, ['Target_Low']] = cwt_incentive - self.low_range

        input_data = input_data.merge(self.svc_to_prod, on='Product', how='inner')
        input_data = input_data[['BidNumber', 'Product', 'Product_Mode', 'Incentive_Freight', 'Target_High',
                                 'Target_Low', 'MVM_DRC_CD', 'SVM_TYP_CD', 'SVC_FEA_TYP_CD']]

        return input_data
