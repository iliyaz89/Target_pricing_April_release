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
# 12/13/17     Mohammed B       Accessorial and Accessorial_map are being replaced with         */
#                               Eligilbe_accessorials and Accessorial_ceiling.                  */

import ConfigParser
import re
import os
import cPickle as pickle
import pandas as pd

def read_data(file_path, data_type_path):
    """
    Function to read data and set data types

    Args:
        file_path (str): file path for data
        data_type_path (str): path for data type

    Returns:
         (pandas data frame): data frame  with set data types
    """
    try:
        data_type = pd.read_csv(data_type_path, index_col='Feature')
        data_type_dict = data_type.T.to_dict(orient='record')[0]
    except IOError:
        print 'data type lookup file not in directory'
        raise
        
    try:
        return pd.read_csv(file_path, dtype=data_type_dict)
    except IOError:
        print 'input data file not found in directory'
        raise


def generate_cwt_pickel(input_dir):
    # Load attributes:
    csvopts = {'sep': ',',
               'header': 0,
               'skipinitialspace': True,
               'na_values': ['', 'NA', 'NULL']}
    norownames = {'index': False}
    # Specify resource list
    cwt_resdict = {
        # Air
        'air_bt_threshold': [input_dir, 'aircwt_output_bt_threshold.csv', csvopts, norownames],
        'air_density_threshold': [input_dir, 'aircwt_output_density_threshold.csv', csvopts, norownames],
        'air_size_threshold': [input_dir, 'aircwt_output_size_threshold.csv', csvopts, norownames],
        'air_cohort_map': [input_dir, 'aircwt_output_cohort_map.csv', csvopts, norownames],
        'air_incentive_map': [input_dir, 'aircwt_output_incentive_map.csv', csvopts, norownames],
        # Ground
        'gnd_bt_threshold': [input_dir, 'gndcwt_output_bt_threshold.csv', csvopts, norownames],
        'gnd_density_threshold': [input_dir, 'gndcwt_output_density_threshold.csv', csvopts, norownames],
        'gnd_size_threshold': [input_dir, 'gndcwt_output_size_threshold.csv', csvopts, norownames],
        'gnd_cohort_map': [input_dir, 'gndcwt_output_cohort_map.csv', csvopts, norownames],
        'gnd_incentive_map': [input_dir, 'gndcwt_output_incentive_map.csv', csvopts, norownames]}

    # Define load function (optional)
    def load(resource):
        """Helper function to load resources"""
        reslist = cwt_resdict[resource]
        filename = os.path.join(reslist[0], reslist[1])
        return pd.read_table(filename, **reslist[2])

    # Load 5 air tables
    air_bt_threshold = load('air_bt_threshold').rename(columns={'SEGMENT': 'BT_SEGMENT'})
    air_density_threshold = load('air_density_threshold').rename(columns={'SEGMENT': 'DEN_SEGMENT'})
    air_size_threshold = load('air_size_threshold').rename(columns={'SEGMENT': 'SIZE_SEGMENT'})
    air_cohort_map = load('air_cohort_map').rename(columns={'COMM TIER SEGMENT': 'BT_SEGMENT',
                                                            'DENSITY SEGMENT': 'DEN_SEGMENT',
                                                            'BID LIST REV WEEKLY SEGMENT': 'SIZE_SEGMENT'})
    air_incentive_map = load('air_incentive_map')
    # Load 5 ground tables
    gnd_bt_threshold = load('gnd_bt_threshold').rename(columns={'SEGMENT': 'BT_SEGMENT'})
    gnd_density_threshold = load('gnd_density_threshold').rename(columns={'SEGMENT': 'DEN_SEGMENT'})
    gnd_size_threshold = load('gnd_size_threshold').rename(columns={'SEGMENT': 'SIZE_SEGMENT'})
    gnd_cohort_map = load('gnd_cohort_map').rename(columns={'COMM TIER SEGMENT': 'BT_SEGMENT',
                                                            'DENSITY SEGMENT': 'DEN_SEGMENT',
                                                            'BID LIST REV WEEKLY SEGMENT': 'SIZE_SEGMENT'})
    gnd_incentive_map = load('gnd_incentive_map')

    # Generate pickle files
    cwt_pk_name = open(input_dir + '\\cwt_production' + '.p', 'wb')
    pickle.dump(
        [air_bt_threshold, air_density_threshold, air_size_threshold, air_cohort_map, air_incentive_map,
         gnd_bt_threshold, gnd_density_threshold, gnd_size_threshold, gnd_cohort_map, gnd_incentive_map, ],
        cwt_pk_name,
        pickle.HIGHEST_PROTOCOL
    )
    cwt_pk_name.close()

def create_variable_importance(df_variable_importance, variable_list, file_name=None):
    """
    A function to sum importance scores for variable importance dataframes when there is dummied columns.

    Note: This searches by name only. For example, it assumes product_1DA to be in the same class ?
    
    Args:
        df_variable_importance (pandas dataframe): importance score dataframe
        variable_list (dictionary): variable list dictionary in {variable: data type} format
        file_name (Optional)(default=None)(str): file name to save the output. Currently, does not write
        to file if a csv file with the same name exists. If no file name provided, aggregated variable
        importance score is returned.

    Returns:
        var_imp_df (pandas data frame): if a filename is provided
    """

    var_imp_dict = {}
    # sum up the importance score of dummied variables
    for i in variable_list.keys():
        # if the feature is in the category list, then sum the importance of the dummied data
            
        var_imp_dict.update({i: df_variable_importance[df_variable_importance.index.str.contains('^' + i)]['ImportanceScore'].sum()})
        
    var_imp_df = pd.DataFrame.from_dict(var_imp_dict, orient='index')
    var_imp_df.columns = ['ImportanceScore']
    
    if file_name is not None:
        file_path = './Reporting Support Files/Model Outputs/' + file_name + '_variable_importance_score.csv'
        if os.path.exists(file_path):
            raise EOFError('File already exists. Please remove ' + file_name + ' to avoid over writing files.')
        else:
            var_imp_df.to_csv('./Reporting Support Files/Model Outputs/' + file_name + '_variable_importance_score.csv')
    else:
        return var_imp_df
    
def variable_importance_market_pull_through(fit_object, variable_list, file_name=None):
    """
    A function to go through wide format variable importance and average importance scores. Calls create variable
    importance.

    Args:
        fit_object (dict): random forest object from the random forest class
        variable_list (dict): variable list
        file_name (str): file_name (Optional)(default=None)(str): file name to save the output. Currently, does not write
        to file if a csv file with the same name exists. If no file name provided, aggregated variable
        importance score is returned

    Returns:
        var_imp_df (pandas data frame): if a filename is provided
    """
    variable_importance = []
    for i in fit_object.fit_object.values():
        variable_importance.append(create_variable_importance(df_variable_importance=i['variable_importance'], 
                                                              variable_list=variable_list)
                                  )
    mpt_importance_list = pd.concat(variable_importance, axis=0)
    agg_mpt_importance_list = mpt_importance_list.groupby(mpt_importance_list.index).mean()
    create_variable_importance(df_variable_importance=agg_mpt_importance_list,
                               variable_list=variable_list, file_name=file_name)

def init_file_parser(init_path):
    """
    Function to open an init file and parse the content into variables.
    
    args:
        init_path (str): folder path of initiation file
    returns:
        settings_dict (dict): dictionary of settings parsed from the initiation file
    """
    Config = ConfigParser.RawConfigParser()
    Config.optionxform = str 
    Config.read(init_path)
    settings_dict = {}
    for section in Config.sections():
        options = Config.options(section)
        for option in options:
            try:
                settings_dict[option] = Config.get(section, option)
                if settings_dict[option] == -1:
                    print("skip: %s" % option)
            except:
                print("exception on %s!" % option)
                settings_dict[option] = None
    return settings_dict

def settings_from_init_file(init_dict):
    """
    Function to create settings from init file. Converts data into format used by all models. 
    Data types are set.
    
    args:
        init_path (dict): settings dictionary extracted from the initiation file
    returns:
        settings (dict): 
    """
    return({'AIR': {'u_MktPR_c' : float(init_dict['AIR_u_MktPR_c']),
                    'u_MktInc_c' : float(init_dict['AIR_u_MktInc_c']),
                    'l_MktPR_c' :float(init_dict['AIR_l_MktPR_c']),
                    'l_MktInc_c' : float(init_dict['AIR_l_MktInc_c']),
                    'l_Floor_c' : float(init_dict['AIR_l_Floor_c']),
                    'StrictRatio': bool(init_dict['AIR_StrictRatio']),
                    'AIR_Strict_PR1da2da': float(init_dict['AIR_Strict_PR1da2da']),
                    'AIR_Strict_PR2da3da': float(init_dict['AIR_Strict_PR2da3da']),
                    'AIR_Strict_PR1da3da': float(init_dict['AIR_Strict_PR1da3da']),
                    'AIR_Relaxed_PR1da2da': float(init_dict['AIR_Relaxed_PR1da2da']),
                    'AIR_Relaxed_PR2da3da':float(init_dict['AIR_Relaxed_PR2da3da']),
                    'AIR_Relaxed_PR1da3da': float(init_dict['AIR_Relaxed_PR1da3da']),
                    'list_PriceDiscip' : float(init_dict['AIR_list_PriceDiscip']),
                    'list_Volumeweight' : float(init_dict['AIR_list_Volumeweight']),
                    'PriorInctest': float(init_dict['AIR_PriorInctest']),
                    'MARKETINCENTIVE_MODEL': init_dict['MARKETINCENTIVE_AIRMODEL'],
                    'EFF_TO_OFFER': init_dict['EFF_TO_OFFER_AIR'],
                    'MPT_REG': init_dict['AIR_MPT_REG'],
                    'FL': init_dict['AIR_FL']},
            'GND': {'u_MktPR_c' : float(init_dict['GND_u_MktPR_c']),
                    'u_MktInc_c' :float(init_dict['GND_u_MktInc_c']),
                    'l_MktPR_c' :float(init_dict['GND_l_MktPR_c']),
                    'l_MktInc_c' : float(init_dict['GND_l_MktInc_c']),
                    'l_Floor_c' :float(init_dict['GND_l_Floor_c']),
                    'list_PriceDiscip' : float(init_dict['GND_list_PriceDiscip']),
                    'list_Volumeweight' :float(init_dict['GND_list_Volumeweight']),
                    'PriorInctest': float(init_dict['GND_PriorInctest']),
                    'MARKETINCENTIVE_MODEL': init_dict['MARKETINCENTIVE_GROUNDMODEL'],
                    'EFF_TO_OFFER': init_dict['EFF_TO_OFFER_GND'],
                    'MPT_REG': init_dict['GND_MPT_REG'],
                    'FL': init_dict['GND_FL']},
            'IE' : {'u_MktPR_c' :float(init_dict['IMPEXP_u_MktPR_c']),
                    'u_MktInc_c' :float(init_dict['IMPEXP_u_MktInc_c']),
                    'l_MktPR_c' : float(init_dict['IMPEXP_l_MktPR_c']),
                    'l_MktInc_c' :float(init_dict['IMPEXP_l_MktInc_c']),
                    'l_Floor_c' :float(init_dict['IMPEXP_l_Floor_c']),
                    'list_PriceDiscip' :float(init_dict['IMPEXP_list_PriceDiscip']),
                    'list_Volumeweight' :float(init_dict['IMPEXP_list_Volumeweight']),
                    'PriorInctest': float(init_dict['IMPEXP_PriorInctest']),
                    'MARKETINCENTIVE_MODEL': init_dict['MARKETINCENTIVE_IMPEXPMODEL'],
                    'EFF_TO_OFFER': init_dict['EFF_TO_OFFER_IMPEXP'],
                    'MPT_REG': init_dict['IMPEXP_MPT_REG'],
                    'FL': init_dict['IMPEXP_FL']},
           'MODELS':{'DATA_PREP': init_dict['DATA_PREP'],
                     'DATA_PREPROCESS': init_dict['DATA_PREPROCESS'],
                     'AIR_MPT_REG': init_dict['AIR_MPT_REG'],
                     'GND_MPT_REG': init_dict['GND_MPT_REG'],
                     'IMPEXP_MPT_REG': init_dict['IMPEXP_MPT_REG'],
                     'AIR_FL': init_dict['AIR_FL'],
                     'GND_FL': init_dict['GND_FL'],
                     'IMPEXP_FL': init_dict['IMPEXP_FL'],
                     'MARKETINCENTIVE_AIRMODEL': init_dict['MARKETINCENTIVE_AIRMODEL'],
                     'MARKETINCENTIVE_GOUNDMODEL': init_dict['MARKETINCENTIVE_GROUNDMODEL'],
                     'MARKETINCENTIVE_IMPEXPMODEL': init_dict['MARKETINCENTIVE_IMPEXPMODEL'],
                     'EFF_TO_OFFER_AIR': init_dict['EFF_TO_OFFER_AIR'],
                     'EFF_TO_OFFER_GND': init_dict['EFF_TO_OFFER_GND'],
                     'EFF_TO_OFFER_IMPEXP': init_dict['EFF_TO_OFFER_IMPEXP']},
           'CAPS': {'eff_off_relative_low': float(init_dict['eff_off_relative_low']),
                    'eff_off_relative_high': float(init_dict['eff_off_relative_high']),
                    'eff_off_absolute_low': float(init_dict['eff_off_absolute_low']),
                    'eff_off_absolute_high': float(init_dict['eff_off_absolute_high'])},
           'DATA': {'MARKETINCENTIVE_AIR_VARIABLES': init_dict['MARKETINCENTIVE_AIR_VARIABLES'],
                    'MARKETINCENTIVE_GND_VARIABLES': init_dict['MARKETINCENTIVE_GND_VARIABLES'],
                    'MARKETINCENTIVE_IMPEXP_VARIABLES': init_dict['MARKETINCENTIVE_IMPEXP_VARIABLES'],
                    'MARKETPULLTHRU_VARIABLES_BID': init_dict['MARKETPULLTHRU_VARIABLES_BID'],
                    'MARKETPULLTHRU_VARIABLES_PROD': init_dict['MARKETPULLTHRU_VARIABLES_PROD'],
                    'EFF_TO_OFFER_AIR_VARIABLES': init_dict['EFF_TO_OFFER_AIR_VARIABLES'],
                    'EFF_TO_OFFER_GND_VARIABLES': init_dict['EFF_TO_OFFER_GND_VARIABLES'],
                    'EFF_TO_OFFER_IMPEXP_VARIABLES': init_dict['EFF_TO_OFFER_IMPEXP_VARIABLES'],
                    'FL_AIR_VARIABLES': init_dict['FL_AIR_VARIABLES'],
                    'FL_GND_VARIABLES': init_dict['FL_GND_VARIABLES'],
                    'FL_IE_VARIABLES': init_dict['FL_IE_VARIABLES'],
                    'MASTER_DATASET_BID': init_dict['MASTER_DATASET_BID'],
                    'MASTER_DATASET_PROD': init_dict['MASTER_DATASET_PROD'],
                    'IWA_CEILING': init_dict['IWA_CEILING'],
                    'IWA_CEILING_PROD': init_dict['IWA_CEILING_PROD'],
                    'SVC_MATCHING': init_dict['SVC_MATCHING'],
                    'STRATEGIC_OVERLAY': init_dict['STRATEGIC_OVERLAY'],
                    'STRATEGIC_OVERLAY_CALIB': init_dict['STRATEGIC_OVERLAY_CALIB'],
                    'DATA_TYPE': init_dict['DATA_TYPE'],
                    #'ACCESSORIAL': init_dict['ACCESSORIAL'],
                    #'ACCESSORIAL_MAP': init_dict['ACCESSORIAL_MAP'],
                    'ELIGIBLE_ACCESSORIALS': init_dict['ELIGIBLE_ACCESSORIALS'],
                    'ACCESSORIAL_CEILING': init_dict['ACCESSORIAL_CEILING']},
            'POSTPROCESSING': {'GND_Resi_min_for_Resi_inc': float(init_dict['GND_Resi_min_for_Resi_inc']),
                               'GND_Resi_min_for_DAS_inc': float(init_dict['GND_Resi_min_for_DAS_inc']),
                               'Bid_List_Rev_Wkly_min_for_DAS_inc': float(init_dict['Bid_List_Rev_Wkly_min_for_DAS_inc']),
                               'Resi_inc_value': float(init_dict['Resi_inc_value']),
                               'DAS_inc_value': float(init_dict['DAS_inc_value']),
                               'Inc_spread_high': float(init_dict['Inc_spread_high']),
                               'Inc_spread_low': float(init_dict['Inc_spread_low']),
                               'Max_OR_Value': float(init_dict['Max_OR_Value']),
                               'IWA_range': float(init_dict['IWA_range'])},
            'PATHS': {'MODEL_OBJ_PATH': init_dict['OBJ'],
                      'INPUT_PATH': init_dict['INPUT'],
                      'MODEL_VARIABLES_PATH': init_dict['VARIABLES'],
                      'REPORTING_SUPPORT_FILES': init_dict['REPORTING_SUPPORT_FILES'],
                      'REPORTS': init_dict['REPORTS']},
            'DATES': {'TRAIN_PERIOD_START': init_dict['TRAIN_PERIOD_START'],
                      'TRAIN_PERIOD_END': init_dict['TRAIN_PERIOD_END']},
            'RF_SETTINGS':{'MARKETINCENTIVE_TREES': int(init_dict['MARKETINCENTIVE_TREES']),
                           'MARKETINCENTIVE_MIN_SAMPLE_SPLIT': int(init_dict['MARKETINCENTIVE_MIN_SAMPLE_SPLIT']),
                           'MARKETINCENTIVE_MX_FEATURES': init_dict['MARKETINCENTIVE_MX_FEATURES'],
                           'MARKETINCENTIVE_CORES': int(init_dict['MARKETINCENTIVE_CORES']),
                           'MARKETINCENTIVE_CROSS_VALIDATION_SCORE': bool(init_dict['MARKETINCENTIVE_CROSS_VALIDATION_SCORE']),
                           'MARKETPULLTHRU_TREES': int(init_dict['MARKETPULLTHRU_TREES']),
                           'MARKETPULLTHRU_MX_FEATURES': init_dict['MARKETPULLTHRU_MX_FEATURES'],
                           'MARKETPULLTHRU_CORES': int(init_dict['MARKETPULLTHRU_CORES']),
                           'WINLOSS_TREES' : int(init_dict['WINLOSS_TREES']),
                           'WINLOSS_MX_FEATURES': init_dict['WINLOSS_MX_FEATURES'],
                           'WINLOSS_CORES': int(init_dict['WINLOSS_CORES'])}})


def load_model_variables(model_variables_path, data_list):
    """
    Function to load data from input folder.
    
    args:
        data_path (str): folder path of data
    returns:
        data_objects (dict): dictionary with {'filename' (as found in the init file) : csv file}
    """
    data_objects = {}
    for data in os.listdir(model_variables_path):
        if re.sub(r'\.csv', "", data) in data_list:
            try:
                with open(model_variables_path + data, 'rb') as csv:
                    print(csv)
                    data_name = re.sub(r'\.csv', "", data)
                    var_list = pd.read_csv(csv, low_memory=False)
                    data_objects[data_name] = {i:j for i, j in zip(var_list['Feature'], var_list['Type'])}
            except EOFError:
                print(data + "does not exist in directory.")
    return data_objects

def load_model_objects(model_path, model_list):
    """
    Function to load data from input folder.
    
    args:
        model_path (str): folder path of data
    returns:
        model_objects (dict): dictionary with {'filename' (as found in the init file): pickle file}
    """
    model_objects = {}
    for model in os.listdir(model_path):
        if re.sub(r'\.p', "", model) not in model_list:
            pass
        else:
            print model
            try:
                with open(model_path + model, 'rb') as model_pickle:
                    print(model_pickle)
                    model_name = re.sub(r'\.p', "", model)
                    model_objects[model_name] = pickle.load(model_pickle)
            except EOFError:
                print(model + "does not exist in directory.")
    if not model_objects:
        print 'init file model objects not in the current folder.'
    return model_objects

def apply_preprocess_add_market_incentive(data, settings, model_objects):
    """
    A wrapper function that applies preprocessing and appends marekt incentive prediction of
    Normal_Incentive_Perf and Eff_Price_to_Market
    
    args:
        data (pandas dataframe)
        settings (dict): settings generated from the init file
        model_objects (dict): market incentive and data preprocess model objects
    """
    data = model_objects[settings['MODELS']['DATA_PREPROCESS']].transform(data, strategy='simple')
    append_data = []
    for k, group in data.groupby('Product_Mode'):
        pred_mi = model_objects[settings[k]['MARKETINCENTIVE_MODEL']].transform(group)
        append_data.append(pred_mi)
    return(pd.concat(append_data, axis=0))
