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
# 12/01/17     Mohammed B       The new Accessorial method has been designed and replaced the   */
#                               old one.                                                        */
# 12/07/17     Mohammed B       The Post_Processing method has been modified.                   */
# 12/14/17     Mohammed B       The deal_scoring_additional_output has been modified with the   */
#                               limit range of Incentives.                                      */
# 12/22/17     Mohammed B       Bid request reason has been corrected as it was not being       */
#                               recognized.                                                     */
# 01/10/18     Mohammed B       StrategicOverlay method has been modified and being called for  */
#                               Data-Processing.                                                */


import pandas as pd
import numpy as np
from scipy.optimize import minimize
from statsmodels.formula.formulatools import dmatrices
from logger_helper import log_function, logger

@log_function
class OptimalIncentives(object):
    """
    A class to estimate optimal incentives.

    """
    def __init__(self, ceilinglookup_file, svc_to_prod_file, model_objects, settings,
                 isProduction=False, strategicOverlay=None,
                 isOmitIWACeiling=False, industry_name_lookup=None,
                 accessorial_ceiling=None, eligible_accessorials=None):
        """
        Args:
            ceilinglookup_file (pandas data frame): IWA ceiling caps
            svc_to_prod_file (pandas data frame): service to product table
            model_objects (dict): dictionary of model objects from calibration
            settings (dict): dictionary created from parsed config file
            accessorials (pandas dataframe): accessorial
            accessorial_map (pandas dataframe): accessorial map
            isProduction (bool): whether code is being executed in production mode
            strategicOverlay (pandas dataframe): overlay dataframe. If no overlay dataframe detected, it defaults to
            None and no overlays are applied.
            isOmitIWACeiling (bool): whether to omit IWA ceilings
            industry_name_lookup (pandas dataframe): industry name lookup table
        """

        self.svc_to_prod_file = svc_to_prod_file
        self.ceilinglookup_file = ceilinglookup_file
        self.model_objects = model_objects
        self.isProduction = isProduction
        self.strategicOverlay = strategicOverlay
        self.isOmitIWACeiling = isOmitIWACeiling
        self.eligible_accessorials = eligible_accessorials
        self.accessorial_ceiling = accessorial_ceiling

        self.IWA_RANGE = settings['POSTPROCESSING']['IWA_range']
        self.MAX_OR_VALUE =  settings['POSTPROCESSING']['Max_OR_Value']

        self.DATA_PREPROCESS = settings['MODELS']['DATA_PREPROCESS']
        self.DATA_PREP = settings['MODELS']['DATA_PREP']
        self.MPT_REG = {'AIR': settings['MODELS']['AIR_MPT_REG'],
                        'GND': settings['MODELS']['GND_MPT_REG'],
                        'IE': settings['MODELS']['IMPEXP_MPT_REG']}

        self.AIR_FL = settings['MODELS']['AIR_FL']
        self.GND_FL = settings['MODELS']['GND_FL']
        self.IMPEXP_FL = settings['MODELS']['IMPEXP_FL']

        self.FL = {'AIR': settings['MODELS']['AIR_FL'],
                   'GND': settings['MODELS']['GND_FL'],
                   'IE': settings['MODELS']['IMPEXP_FL']}

        self.MARKETINCENTIVE_AIRMODEL = {'AIR': settings['MODELS']['MARKETINCENTIVE_AIRMODEL'],
                                         'GND': settings['MODELS']['MARKETINCENTIVE_GOUNDMODEL'],
                                         'IE': settings['MODELS']['MARKETINCENTIVE_IMPEXPMODEL']}

        self.EFF_TO_FREIGHT = {'AIR': settings['MODELS']['EFF_TO_OFFER_AIR'],
                               'GND': settings['MODELS']['EFF_TO_OFFER_GND'],
                               'IE': settings['MODELS']['EFF_TO_OFFER_IMPEXP']}

        self.AIR_STRICT_RATIO = settings['AIR']['StrictRatio']
        self.STRICT_PR1da2da = settings['AIR']['AIR_Strict_PR1da2da']
        self.STRICT_PR2da3da = settings['AIR']['AIR_Strict_PR2da3da']
        self.STRICT_PR1da3da = settings['AIR']['AIR_Strict_PR1da3da']
        self.RELAXED_PR1da2da = settings['AIR']['AIR_Relaxed_PR1da2da']
        self.RELAXED_PR2da3da = settings['AIR']['AIR_Relaxed_PR2da3da']
        self.RELAXED_PR1da3da = settings['AIR']['AIR_Relaxed_PR1da3da']

        # u_MktPR_c
        self.map_u_MktPR_c = {'AIR': settings['AIR']['u_MktPR_c'],
                              'GND': settings['GND']['u_MktPR_c'],
                              'IE': settings['IE']['u_MktPR_c']}
        # u_mktInc
        self.map_u_MktInc_c = {'AIR': settings['AIR']['u_MktInc_c'],
                               'GND': settings['GND']['u_MktInc_c'],
                               'IE': settings['IE']['u_MktInc_c']}
        # l_MktPR
        self.map_l_MktPR_c = {'AIR': settings['AIR']['l_MktPR_c'],
                              'GND': settings['GND']['l_MktPR_c'],
                              'IE': settings['IE']['l_MktPR_c']}
        # l_MktInc
        self.map_l_MktInc_c = {'AIR': settings['AIR']['l_MktInc_c'],
                               'GND': settings['GND']['l_MktInc_c'],
                               'IE': settings['IE']['l_MktInc_c']}
        # l_Floor
        self.map_l_Floor_c = {'AIR': settings['AIR']['l_Floor_c'],
                              'GND': settings['GND']['l_Floor_c'],
                              'IE': settings['IE']['l_Floor_c']}

        self.map_PriorInctest = {'AIR': settings['AIR']['PriorInctest'],
                                 'GND': settings['GND']['PriorInctest'],
                                 'IE': settings['IE']['PriorInctest']}

        self.Weight_PriceDiscip_map = {'AIR': settings['AIR']['list_PriceDiscip'],
                                       'GND': settings['GND']['list_PriceDiscip'],
                                       'IE': settings['IE']['list_PriceDiscip']}

        self.Weight_Volume_map = {'AIR': settings['AIR']['list_Volumeweight'],
                                  'GND': settings['GND']['list_Volumeweight'],
                                  'IE': settings['IE']['list_Volumeweight']}

        self.post_process_settings = {'Inc_spread_high': settings['POSTPROCESSING']['Inc_spread_high'],
                                      'Inc_spread_low': settings['POSTPROCESSING']['Inc_spread_low']}

        # effective to freight caps
        self.apply_eff_to_freight_caps = {'eff_off_relative_low': settings['CAPS']['eff_off_relative_low'],
                                          'eff_off_relative_high': settings['CAPS']['eff_off_relative_high'],
                                          'eff_off_absolute_low': settings['CAPS']['eff_off_absolute_low'],
                                          'eff_off_absolute_high': settings['CAPS']['eff_off_absolute_high']}

        # conditional inputs
        if self.isProduction is True:
            self.industry_name_lookup = industry_name_lookup

        ################################################################################################################
        # test if IWA table or overlay table are available
        if strategicOverlay is not None:
            if strategicOverlay.empty:
                raise Exception('Strategic overlay production table is empty')
            elif strategicOverlay.isnull().any().any():
                raise Exception('Strategic overlay production table includes null values.')

        if ceilinglookup_file.empty:
            raise Exception('IWA production table is empty')
        elif ceilinglookup_file.isnull().any().any():
            raise Exception('IWA production table includes nulls')
        ################################################################################################################

    @log_function
    def add_ceiling(self, data):
        """
        A function to add IWA comparator bounds
        Args:
            data (pandas dataframe): data with Bid_List_Rev_Wkly, Product, and BidNumber
        Returns:
            data (pandas dataframe): input data updated with incentive ceiling `ceiling`
        """
        ceiling_lookup = self.ceilinglookup_file

        # toggle incentive caps on and off for production
        if self.isOmitIWACeiling is True:
            ceiling_lookup['Off_Inc_Cap'] = 1

        ceil = data[['Bid_List_Rev_Wkly', 'Product']]
        ceil = pd.merge(ceil.reset_index(), ceiling_lookup, left_on='Product',
                        right_on='Product').set_index('BidNumber')
        ceil = ceil[(ceil['Bid_List_Rev_Wkly'] > ceil['Min_List_Rev_Wkly']) &
                    (ceil['Bid_List_Rev_Wkly'] < ceil['Max_List_Rev_Wkly'])]

        ceil.rename(columns={'Off_Inc_Cap': 'ceiling'},
                    inplace=True)
        ceil.drop('Bid_List_Rev_Wkly', axis=1, inplace=True)

        logger.debug('******************Exiting function add_Ceiling')
        return (pd.merge(data.reset_index(), ceil.reset_index(),
                         left_on=['Product', 'BidNumber'],
                         right_on=['Product', 'BidNumber']).set_index('BidNumber'))

    @log_function
    def apply_bounds_and_check_feasibility(self, data):
        """
        A function to add bounds to the data
        """
        def MakeList(x):
            T = tuple(x)
            if len(T) > 1:
                return T
            else:
                return T

        data['u_MktPR_c'] = data['Product_Mode'].map(self.map_u_MktPR_c)
        data['u_MktInc_c'] = data['Product_Mode'].map(self.map_u_MktInc_c)
        data['l_MktPR_c'] = data['Product_Mode'].map(self.map_l_MktPR_c)
        data['l_MktInc_c'] = data['Product_Mode'].map(self.map_l_MktInc_c)
        data['l_Floor_c'] = data['Product_Mode'].map(self.map_l_Floor_c)
        data['PriorInctest'] = data['Product_Mode'].map(self.map_PriorInctest)
        # the following populate variables of interest. There is a possibility of an NA due to an NA in
        # market incentive, which is filled with a -1
        data["u_MktPR"] = (data['u_MktPR_c'] * (1 - data['Normal_Incentive_Perf']) - 1).fillna(0)  # <Market Price
        data["u_Incentive"] = -1
        data["u_MktInc"] = (data['u_MktInc_c'] * data['Normal_Incentive_Perf']).fillna(-1)
        data["u_Ceiling"] = (-data['ceiling']).fillna(0)
        data.drop(['u_MktPR_c', 'u_MktInc_c', 'l_MktPR_c', 'l_MktInc_c', 'l_Floor_c'], axis=1)
        # lower bounds
        data["l_Prior"] = np.where(data['Prior_Incentive'].isnull(), 0,
                                   np.where(data['Bid_Request_Reason_Descr'] != "RETENTION",
                                            data.Prior_Incentive, data.Prior_Incentive - data['PriorInctest']))

        ####################################################
        # clean up bad import export prior incentives. Refer to **DOC** for description
        def fix_prior_incentive_IE(x):
            Pct_New_Vol = x['Pct_New_Vol']
            Product_Mode = x['Product_Mode']
            Bid_Imp_Exp_Centric_Flag = x['Bid_Imp_Exp_Centric_Flag']
            l_Prior = x['l_Prior']
            List_Rev_Wkly = x['List_Rev_Wkly']
            if (Bid_Imp_Exp_Centric_Flag != 1 and
                            List_Rev_Wkly * (1 - Pct_New_Vol) <= 20 and
                        Product_Mode == 'IE'):
                l_Prior = 0
            return l_Prior

        data['l_Prior'] = data[['Pct_New_Vol', 'Product_Mode', 'Bid_Imp_Exp_Centric_Flag',
                                'l_Prior', 'List_Rev_Wkly']].apply(lambda x: fix_prior_incentive_IE(x), axis=1)
        #####################################################
        data["l_MktPR"] = np.where(data['Normal_Incentive_Perf'].isnull(), 0,
                                   data['l_MktPR_c'] * (1 - data['Normal_Incentive_Perf']) + 1)  # >Market Price
        data["l_MktInc"] = np.where(data['Normal_Incentive_Perf'].isnull(), 0,
                                    data['l_MktInc_c'] * data['Normal_Incentive_Perf'])  # Mkt Incentive
        data["l_Floor"] = data['l_Floor_c']

        # Create lowerbound and upper bound constraints. Keep track of the name of the upperbound and the lowerbound.
        data['UpperBound'] = -data[["u_MktPR", "u_Incentive", "u_MktInc", "u_Ceiling"]].max(axis=1)
        data['LowerBound'] = data[["l_MktPR", "l_Prior", "l_MktInc", "l_Floor"]].max(axis=1)
        data["UpperBoundName"] = data[["u_MktPR", "u_Incentive", "u_MktInc", "u_Ceiling"]].idxmax(axis=1)
        data["LowerBoundName"] = data[["l_MktPR", "l_Prior", "l_MktInc", "l_Floor"]].idxmax(axis=1)

        # Identify and manage infeasible constraints.
        data["Indvl_constraint"] = np.where(data['LowerBound'] >= data['UpperBound'], False, True)
        data.LowerBoundName = np.where(data.Indvl_constraint, data.LowerBoundName, "Resetinfeasible")
        data.UpperBoundName = np.where(data.Indvl_constraint, data.UpperBoundName, "Resetinfeasible")
        data.LowerBound = np.where(data.Indvl_constraint, data.LowerBound, data['l_Floor'] + 0.001)
        data.UpperBound = np.where(data.Indvl_constraint, data.UpperBound, 1.000 - 0.001)
        data.UpperBound = np.where(abs(data.UpperBound - 1) < 0.000001, 1.000 - 0.001, data.UpperBound)

        # create constraint lists for optimization.
        air_product_list = \
            pd.DataFrame(data[data['Product_Mode'] == 'AIR'].reset_index().groupby(
                ['BidNumber'])[['Product']].aggregate(lambda x: MakeList(x)))
        air_product_list.columns = ['Air_Products']
        ub_list = pd.DataFrame(data.reset_index().groupby(['BidNumber', 'Product_Mode']
                                                          )['UpperBound'].aggregate(lambda x: MakeList(x)))
        ub_list.columns = ['UpperBound_List']
        lb_list = \
            pd.DataFrame(data.reset_index().groupby(['BidNumber', 'Product_Mode']
                                                    )['LowerBound'].aggregate(lambda x: MakeList(x)))
        lb_list.columns = ['LowerBound_List']
        data = pd.merge(data, air_product_list, left_index=True, right_index=True,
                        how='left')
        data = pd.merge(data.reset_index(), ub_list.reset_index(),
                        left_on=['BidNumber', 'Product_Mode'],
                        right_on=['BidNumber', 'Product_Mode']).set_index('BidNumber')
        data = pd.merge(data.reset_index(), lb_list.reset_index(),
                        left_on=['BidNumber', 'Product_Mode'],
                        right_on=['BidNumber', 'Product_Mode']).set_index('BidNumber')
        logger.debug('******************Exiting function apply_bounds_and_check_feasibility ')
        return data

    @log_function
    def add_market_incentive_and_base_market_pullthru(self, data):
        """
        A function to add market incentive and market base pull through to the data
        """
        # Apply data preprocess
        data = self.model_objects[self.DATA_PREPROCESS].transform(data, strategy='simple')

        # Create wide format data
        model_data = self.model_objects[self.DATA_PREP].transform(data)

        # Predict market incentive and base market pullthru for the bid
        append_data = []
        for k, group in data.groupby('Product_Mode'):
            pred_mi = self.model_objects[self.MARKETINCENTIVE_AIRMODEL[k]].transform(group)
            pred_pullthru = self.model_objects[self.MPT_REG[k]].predict(model_data, pred_mi)
            append_data.append(pred_pullthru)
        logger.debug('******************Exiting function add_market_incentive_and_base_market_pullthru')
        return pd.concat(append_data, axis=0).set_index('BidNumber')

    def create_constraints(self, data):
        """
        This function creates constraints for optimization.  It creates upper/lower bounds, price ladder constraints,
        and starting values.
        """

        Product_Mode = data['Product_Mode']
        ub = np.array(data['UpperBound_List'])
        lb = np.array(data['LowerBound_List'])
        Air_Products = data['Air_Products']
        relconstr = ()

        x0 = (ub + lb) / 2
        tol = 1e-5
        bound = zip(lb, ub)
        if Product_Mode == 'AIR':
            ube = ub - np.repeat(tol, len(Air_Products))
            StrictRatio = self.AIR_STRICT_RATIO
            if StrictRatio is True:
                PR1da2da = self.STRICT_PR1da2da
                PR2da3da = self.STRICT_PR2da3da
                PR1da3da = self.STRICT_PR1da3da
            else:
                PR1da2da = self.RELAXED_PR1da2da
                PR2da3da = self.RELAXED_PR2da3da
                PR1da3da = self.RELAXED_PR1da3da
            CC1da2da = 1 - PR1da2da
            CC2da3da = 1 - PR2da3da
            CC1da3da = 1 - PR1da3da
            if ('1DA' in Air_Products) and ('2DA' in Air_Products) and not ('3DA' in Air_Products):
                relconstr = ({'type': 'ineq', 'fun': lambda x: np.array([PR1da2da * x[1] + CC1da2da - x[0]]),
                              'jac': lambda x: np.array([-1.0, PR1da2da])},)
                x0 = (min(CC1da2da + PR1da2da * ube[1] - tol, ube[0]), ube[1])
            if ('2DA' in Air_Products) and ('3DA' in Air_Products) and not ('1DA' in Air_Products):
                relconstr = ({'type': 'ineq', 'fun': lambda x: np.array([PR2da3da * x[1] + CC2da3da - x[0]]),
                              'jac': lambda x: np.array([-1.0, PR2da3da])},)
                x0 = (min(CC2da3da + PR2da3da * ube[1] - tol, ube[0]), ube[1])
            if ('1DA' in Air_Products) and ('2DA' in Air_Products) and not ('3DA' in Air_Products):
                relconstr = ({'type': 'ineq', 'fun': lambda x: np.array([PR1da3da * x[1] + CC1da3da - x[0]]),
                              'jac': lambda x: np.array([-1.0, PR1da3da])},)
                x0 = (min(CC1da3da + PR1da3da * ube[1] - tol, ube[0]), ube[1])
            if ('1DA' in Air_Products) and ('2DA' in Air_Products) and ('3DA' in Air_Products):
                relconstr = ({'type': 'ineq', 'fun': lambda x: np.array([PR1da2da * x[1] + CC1da2da - x[0]]),
                              'jac': lambda x: np.array([-1.0, PR1da2da, 0.0])},
                             {'type': 'ineq', 'fun': lambda x: np.array([PR2da3da * x[2] + CC2da3da - x[1]]),
                              'jac': lambda x: np.array([0.0, -1.0, PR2da3da])})
                x0[2] = ube[2]
                x0[1] = min(CC2da3da + PR2da3da * x0[2] - tol, ube[1])
                x0[0] = min(CC1da2da + PR1da2da * x0[1] - tol, ube[0])

        # test for validity of constraints
        if all(x0 < ub) and all(x0 > lb):
            valid = True
        else:
            valid = False
            x0 = (ub + lb) / 2
        return pd.Series({'relconstr': relconstr, 'x0': tuple(x0), 'valid': valid, 'bound': bound})

    @log_function
    def add_elasticity_alpha_gamma(self, input_vector):
        """
        Function adds alpha and gamma to the data. Alpha and gamma are components of elasticity.
        """

        # create a dummy Eff_Price_to_Market value to compute Gamma
        input_vector['Eff_Price_to_Market'] = 2

        alpha_list = []
        for k, group in input_vector.groupby('Product_Mode'):

            model_obj = self.model_objects[self.FL[k]]
            for i in model_obj.categorical_variables.keys():
                group[i] = pd.Categorical(group[i], categories=model_obj.categorical_variables[i])
            group['Alpha'] = (model_obj.coef_list.ix[['Intercept']].values +
                              group['Base_PT_Pred_tfed'].values *
                              model_obj.coef_list.ix[['Base_PT_Pred_tfed']].values)[0]


            outcome, predictors = dmatrices('1~' + model_obj.model, group)
            coef_matrix = model_obj.coef_list
            design_matrix = pd.DataFrame(np.asarray(predictors), columns=model_obj.variable_names)
            not_alpha_col = [i for i in model_obj.variable_names if i not in ['Intercept', 'Base_PT_Pred_tfed']]
            group['Gamma'] = (design_matrix[not_alpha_col].dot(coef_matrix.ix[not_alpha_col])['values'].values) / \
                             np.log(group['Eff_Price_to_Market'].values)

            alpha_list.append(group)
        logger.debug('******************Exiting function add_elasticity_alpha_gamma ')
        return pd.concat(alpha_list, axis=0)

    def optfun(self, UPS_Incentive, data, d=True):
        """
        Optimizer function.

        Args:
            UPS_Incentive (numpy array): array of ups incentives
            data (pandas dataframe): features used in optimization
            d (bool): whether to use derivative for optimization
        Return:
            (dict):
                {"Obj": objective function value,
                "dObj": derivative of objective function value,
                "BidProfit": total profit at incentive,
                "ProductProfit": product profit at incentive,
                "dProductProfit": derivative of product profit at incentive,
                "PTWonbid": pull through at incentive
        """
        if len(UPS_Incentive) != data.shape[0]:
            raise NameError("The number of rows in data must equal to the length of UPS_Incentive")
        Alpha = data['Alpha'].values
        Gamma = data['Gamma'].values
        ListRev = data['List_Rev_Wkly'].values
        MarginalCost = data['Marginal_Cost_wkly'].values
        Mkt_Inc_Perf = data['Normal_Incentive_Perf'].values
        Weight_Volume = data['Weight_Volume'].values
        Weight_PriceDiscip = data['Weight_PriceDiscip'].values

        PTWonbid = 1 / (1 + np.exp(-Alpha - Gamma * np.log((1 - UPS_Incentive) / (1 - Mkt_Inc_Perf))))
        phi = np.exp(-Alpha - Gamma * np.log((1 - UPS_Incentive) / (1 - Mkt_Inc_Perf)))
        # Objective Function
        ExpectedProfit = (ListRev * (1 - UPS_Incentive) - MarginalCost) * PTWonbid
        PriceDiscip = (Weight_PriceDiscip / (1 - Weight_PriceDiscip)) * ListRev * \
                      (UPS_Incentive - Mkt_Inc_Perf) ** 2
        # when Weight_Volume=1, become gross revenue maximization
        VolumeGoal = Weight_Volume * (PTWonbid * ListRev - ExpectedProfit)

        # Derivatives
        if d:
            dExpectedProfit = -ListRev / (1 + phi) - \
                              (ListRev * (1 - UPS_Incentive) - MarginalCost) * \
                              Gamma / (1 - UPS_Incentive) * phi / (1 + phi) ** 2
            dPriceDiscip = 2 * (Weight_PriceDiscip / (1 - Weight_PriceDiscip)) * \
                           ListRev * (UPS_Incentive - Mkt_Inc_Perf)
            dVolumeGoal = Weight_Volume * (-Gamma / (1 - UPS_Incentive) * \
                                           phi / (1 + phi) ** 2 * ListRev - dExpectedProfit)
            dObj = (dExpectedProfit - dPriceDiscip + dVolumeGoal)/np.sum(ListRev)
        else:
            dExpectedProfit = 0
            dPriceDiscip = 0
            dVolumeGoal = 0
            dObj = 0

        return {"Obj": sum(ExpectedProfit - PriceDiscip + VolumeGoal)/np.sum(ListRev),
                "dObj": dObj,
                "BidProfit": sum(ExpectedProfit),
                "ProductProfit": ExpectedProfit,
                "dProductProfit": dExpectedProfit,
                "PTWonbid": PTWonbid,
                "Price_Goal": PriceDiscip,
                "dPrice_Goal": dPriceDiscip,
                "Volume_Goal": VolumeGoal,
                "dVolume_Goal": dVolumeGoal,
                "MarginalCost": MarginalCost}

    def optimal_incentive_calculator(self, input_vector):
        """
        Function to determine the optimal incentive.

        Args:
            input_vector (pandas data frame): dataframe with the following columns:
                Normal_Incentive_Perf, x0, Bound, relconstr, Product_Mode, valid, Indvl_constraint,
                UpperBound, LowerBound, Weight_PriceDiscip, Weight_Volume, Alpha, Gamma,
                List_Rev_Wkly, Marginal_Cost_wkly
        Returns:
            (pandas dataframe): dataframe with the following columns in production environment:

        Notes:

            1) flag=0: everything is fine
            2) flag=-1: optimization returns a nonzero status (something is wrong)
            3) flag=1: no initial value can be found, set incentives to market incentive

        """
        Normal_Incentive_Perf = input_vector['Normal_Incentive_Perf'].values
        x0 = input_vector['x0'].values[0]
        bound = input_vector['Bound'].values[0]
        relconstr = input_vector['relconstr'].values[0]
        Product_Mode = input_vector['Product_Mode'].values[0]
        valid = input_vector['valid'].values[0]
        Indvl_constraint = input_vector['Indvl_constraint'].values
        ub = input_vector['UpperBound'].values
        lb = input_vector['LowerBound'].values
        ub_name = input_vector['UpperBoundName'].values
        lb_name = input_vector['LowerBoundName'].values
        floor = input_vector['l_Floor'].values
        ceiling = input_vector['ceiling'].values

        fun_list = []
        result_list = []
        status_list = []

        # add data types
        #model_obj = self.model_objects[self.FL[Product_Mode]]
        #for feature in model_obj.categorical_variables.keys():
        #    input_vector[feature] = pd.Categorical(input_vector[feature],
        #                                          categories=model_obj.categorical_variables[feature])

        # for weight price discip of 1 set all incentives to the market incentive
        if all(input_vector['Weight_PriceDiscip'] == 1) is True:
            result_x = np.array(np.minimum(np.maximum(Normal_Incentive_Perf, floor), ceiling))
            result_status = 0
        else:

            if valid:
                # The majority of time, the model converges for x0. Sometimes it helps to choose
                # another starting values refer to page xx of docs.
                starting_values = [x0, tuple(ub), tuple(lb)]

                for val in starting_values:
                    result = minimize(lambda x: -self.optfun(x, input_vector, False)['Obj'],
                                      val, method='SLSQP',
                                      jac=lambda x: -self.optfun(x, input_vector, True)['dObj'],
                                      options={'disp': False,
                                               'maxiter': 200},
                                      bounds=bound,
                                      constraints=relconstr)

                    fun_list.append(result['fun'])
                    result_list.append(result['x'])
                    status_list.append(result['status'])

                fun_list = np.array(fun_list)
                status_list = np.array(status_list)

                if all(status_list) != 0:
                    result_status = -1  # optimization fails for all starting values

                else:
                    succ_optimization = np.where(status_list == 0)

                    succ_fun = fun_list[succ_optimization]
                    succ_result = [result_list[i] for i in succ_optimization[0]]

                    index_of_optim = np.argmin(succ_fun)

                    result_x = succ_result[index_of_optim]
                    result_status = 0
            else:
                result_status = 1

        if result_status != 0:
            result_x = np.array(np.minimum(np.maximum(Normal_Incentive_Perf, floor), ceiling))

        if all(Indvl_constraint) is False:
            result_x_update = []
            for i, j in enumerate(result_x):
                if not Indvl_constraint[i]:
                    result_x_update.append(np.minimum(np.maximum(Normal_Incentive_Perf[i], floor[i]), ceiling[i]))
                else:
                    result_x_update.append(j)
            result_x = np.array(result_x_update)

        # check which constraints are binding
        bindub = (ub - result_x < 1e-5) * 1
        bindlb = (result_x - lb < 1e-5) * 1

        if Product_Mode == 'AIR':
            bindrl = sum(([k["fun"](result_x)[0] < 1e-5 for k in relconstr]) * \
                         (10 ** np.array(range(input_vector.shape[0] - 1))))
        else:
            bindrl = 0

        if self.isProduction is True:
            outbest = self.optfun(result_x, input_vector, False)

            return pd.DataFrame({'Product': input_vector['Product'],
                                 'Optimal_UPSInc': result_x,
                                 'OptimalProfit': outbest["ProductProfit"],
                                 'PTWonbid_optimal': outbest['PTWonbid'],
                                 'MarginalCost': outbest['MarginalCost'],
                                 'Constraints': Indvl_constraint,
                                 'UpperBoundBind': bindub,
                                 'UpperBoundValue': ub,
                                 'LowerBoundBind': bindlb,
                                 'LowerBoundValue': lb,
                                 'UpperBoundType': ub_name,
                                 'LowerBoundType': lb_name,
                                 'MktInc': Normal_Incentive_Perf,
                                 'RelBoundBind': bindrl,
                                 'Flag': result_status})
        else:
            outbest = self.optfun(result_x, input_vector, False)
            outcur = self.optfun(input_vector['Overall_Incentive'].values,
                                 input_vector, False)
            output = pd.DataFrame({'Product': input_vector['Product'],
                                   'Optimal_UPSInc': result_x,
                                   'OptimalProfit': outbest["ProductProfit"],
                                   'PTWonbid_current': outcur["PTWonbid"],
                                   'PTWonbid_optimal': outbest['PTWonbid'],
                                   'ExistingProfit': outcur["ProductProfit"],
                                   'UpperBoundBind': bindub,
                                   'LowerBoundBind': bindlb,
                                   'RelBoundBind': bindrl,
                                   'flag': result_status,
                                   'valid': valid})
        return output

    @log_function
    def applyStrategicOverlay(self, data):
        """
        A function to add strategic overlay to model. Strategic overlay works by adjusting the volume growth factor.

        Args:
            data (pandas dataframe): data including bounds and volume growth factor

        Returns:
            (pandas dataframe): updated data with adjusted volume growth factor
        """
        overlay = self.strategicOverlay[['Bid_District', 'DominantIndustry', 'Product', 'VolIncrement', 'Box']].dropna()
        #type cast variables to make sure they merge
        overlay['Bid_District'] = overlay['Bid_District'].astype(int)
        data['Bid_District'] = data['Bid_District'].astype(int)

        #merge data with overlay increment dataframe
        updated_data = pd.merge(data.reset_index(), overlay,
                                left_on=['Bid_District', 'Ind_Dominant_Industry', 'Product'],
                                right_on=['Bid_District', 'DominantIndustry', 'Product'], how='left')

        # Volume growth factor adjusted
        updated_data['VolIncrement'] = updated_data['VolIncrement'].fillna(0)
        updated_data['Weight_Volume'] = updated_data['Weight_Volume'] + \
                                        updated_data['VolIncrement']

        logger.debug('******************Exiting function applyStrategicOverlay ')
        return updated_data.set_index('BidNumber')

    @log_function
    def run_calculator(self, data):
        """
        A function to run optimizer
        """
        # add market incentives and ceilings
        try:
            data = self.add_market_incentive_and_base_market_pullthru(data)
            data = self.add_ceiling(data)

            # add elasticity alpha and gamma
            data = self.add_elasticity_alpha_gamma(data)

            # create bounds and feasibility checks
            data = self.apply_bounds_and_check_feasibility(data)

            # create bounds for optimization
            data[['Bound', 'relconstr', 'valid', 'x0']] = data.apply(self.create_constraints, axis=1)
            data['Weight_PriceDiscip'] = data['Product_Mode'].map(self.Weight_PriceDiscip_map)
            data['Weight_Volume'] = data['Product_Mode'].map(self.Weight_Volume_map)


            # if strategic overlay is provided apply it
            if self.strategicOverlay is not None:
                data = self.applyStrategicOverlay(data)

            opt_incentive = data.groupby([data.index, 'Product_Mode']).apply(lambda x:
                                                                             self.optimal_incentive_calculator(x))

            data_columns = ['BidNumber', 'Product', 'LowerBoundName','UpperBoundName', 'PriorInctest',
                            'Normal_Incentive_Perf', 'Indvl_constraint',
                            'Alpha', 'Gamma', 'ceiling', 'Weight_Volume', 'Weight_PriceDiscip']

            output_file = pd.merge(opt_incentive.reset_index(), data.reset_index()[data_columns],
                                   left_on=['BidNumber', 'Product'],
                                   right_on=['BidNumber', 'Product'])

            logger.debug('******************Exiting function run_calculator')
            return output_file
        except Exception, e:
            raise ValueError("Run_calculator calculation unsuccessful")

    @log_function
    def apply_eff_to_freight(self, data, opt_incentive):
        """
        Function to turn effective incentives into freight incentives.

        Args:
            data (pandas dataframe): feature input
            opt_incentive (pandas dataframe): output of optimal incentive calculator

        Returns:
            (pandas dataframe):
        """
        data = self.model_objects[self.DATA_PREPROCESS].transform(data, strategy='simple')

        merged_data = pd.merge(opt_incentive.reset_index(), data.reset_index(),
                               left_on=['BidNumber', 'Product'],
                               right_on=['BidNumber', 'Product']).set_index('BidNumberProduct')

        # In production mode Overall_Incentive and Bid_Overall_Incentive are intermediate variables. They are updated
        # with optimal effective incentive from the model
        if self.isProduction is True:
            merged_data['Overall_Incentive'] = merged_data['Optimal_UPSInc']
            overall_list = []
            for k, group in merged_data.groupby('BidNumber'):
                group['Bid_Overall_Incentive'] = np.sum(group['Overall_Incentive'] * group['List_Rev_Wkly'])/ \
                                                 group['Bid_List_Rev_Wkly']
                overall_list.append(group)
            merged_data = pd.concat(overall_list)

        eff_to_freight = []
        for k, group in merged_data.groupby('Product_Mode'):
            pred = self.model_objects[self.EFF_TO_FREIGHT[k]].transform(group)

            # Apply relative cap
            pred['Eff_Off_Incentive_Diff_Estimate'] = np.minimum(pred['Eff_Off_Incentive_Diff_Estimate'],
                                                                 self.apply_eff_to_freight_caps['eff_off_relative_low'])
            pred['Eff_Off_Incentive_Diff_Estimate'] = np.maximum(pred['Eff_Off_Incentive_Diff_Estimate'],
                                                                 self.apply_eff_to_freight_caps['eff_off_relative_high'])

            pred['Incentive_Freight_Estimated'] = pred['Overall_Incentive'] - pred['Eff_Off_Incentive_Diff_Estimate']

            #apply absolute cap
            pred['Incentive_Freight_Estimated'] = np.minimum(pred['Incentive_Freight_Estimated'],
                                                             self.apply_eff_to_freight_caps['eff_off_absolute_high'])
            pred['Incentive_Freight_Estimated'] = np.maximum(pred['Incentive_Freight_Estimated'],
                                                             self.apply_eff_to_freight_caps['eff_off_absolute_low'])

            eff_to_freight.append(pred)

        logger.debug('******************Exiting function apply_eff_to_freight')
        return pd.concat(eff_to_freight, axis=0)

    ####################################################################################################################
    # Calibration
    ####################################################################################################################
    def run_calculator_all_scenarios(self, data, product_mode, min_PriceDiscip, max_PriceDiscip, step_PriceDiscip,
                                     min_Volume, max_Volume, step_Volume):
        """
        A function to run scenarios of the economic model.

        Args:
            data (pandas data frame): input feature/constraint data frame
            product_mode (str): product mode
            product_mode:
            min_PriceDiscip:
            max_PriceDiscip:
            step_PriceDiscip:
            min_Volume:
            max_Volume:
            step_Volume:

        Returns:
            writes scenario results to csv
        """

        if data[data['Product_Mode'] == product_mode].shape == 0:
            print 'Data does not include ' + product_mode + ' to train'
            raise

        Weight_PriceDiscip = np.arange(max(min_PriceDiscip, 0), min(max_PriceDiscip + .1, 1.1), step_PriceDiscip)
        Weight_Volume = np.arange(max(min_Volume, 0), min(max_Volume + .1, 1.1), step_Volume)

        # add market incentives and ceilings
        data = self.add_market_incentive_and_base_market_pullthru(data)
        data = self.add_ceiling(data)

        # add elasticity alpha and gamma
        data = self.add_elasticity_alpha_gamma(data)
        # create bounds and feasibility checks
        data = self.apply_bounds_and_check_feasibility(data)

        # create bounds for optimization
        data[['Bound', 'relconstr', 'valid', 'x0']] = \
            data.apply(self.create_constraints, axis=1)

        # subset the mode
        data = data[data['Product_Mode'] == product_mode]

        # loop through the scenarios for
        scenario_list = []
        for price in Weight_PriceDiscip:
            print 'price ' + str(price)
            if price != 1:
                for volume in Weight_Volume:
                    data['Weight_PriceDiscip'] = price
                    data['Weight_Volume'] = volume
                    opt_incentive = data.groupby([data.index]).apply(lambda x:
                                                                     self.optimal_incentive_calculator(x))
                    scenario_list.append(opt_incentive)
            else:
                data['Weight_PriceDiscip'] = 1
                data['Weight_Volume'] = 0
                opt_incentive = data.groupby([data.index]).apply(lambda x:
                                                                 self.optimal_incentive_calculator(x))
                scenario_list.append(opt_incentive)
        # data_columns has been appended with the Bid_Request_Reason_Descr
        data_columns = ['BidNumber', 'Product', 'Product_Mode', 'Bid_Region',
                        'Bid_Request_Reason_Descr', 'List_Rev_Wkly',
                        'Prior_Incentive', 'Overall_Incentive',
                        'Bid_District', 'Bid_List_Rev_Wkly', 'Ind_Dominant_Industry', 'LowerBoundName',
                        'UpperBoundName', 'PriorInctest', 'Normal_Incentive_Perf', 'Indvl_constraint']

        output_file = pd.concat(scenario_list, axis=0)
        output_file = pd.merge(output_file.reset_index(), data.reset_index()[data_columns],
                               left_on=['BidNumber', 'Product'],
                               right_on=['BidNumber', 'Product'])

        # write file to disk
        filename = "./Output Files/" + "complete_scenario_list_" + product_mode + ".csv"
        output_file.to_csv(filename)

    def run_calculator_calibration(self, data, WriteToDiskFileName=None):
        """
        A function that returns freight incentives.
        """
        if data[data['Product_Mode'].isin(['AIR', 'GND', 'IE'])].shape[0] == 0:
            print 'No AIR, GND, or IE bids to train model on.'
            raise

        opt_incentive = self.run_calculator(data)
        opt_incentive_offered = self.apply_eff_to_freight(data, opt_incentive)

        if WriteToDiskFileName is None:
            return opt_incentive_offered
        else:
            for k, group in opt_incentive_offered.groupby('Product_Mode'):
                group['Weight_Volume'] = self.Weight_Volume_map[k]
                group.to_csv('./Reporting Support Files/Model Outputs/' + WriteToDiskFileName + k + '.csv')

    def run_calculator_calibration_effective(self, data, WriteToDiskFileName=None):
        if data[data['Product_Mode'].isin(['AIR', 'GND', 'IE'])].shape[0] == 0:
            print 'No AIR, GND, or IE bids to train model on.'
            raise

        opt_incentive = self.run_calculator(data)
        merged_data = pd.merge(opt_incentive.reset_index(), data, left_on=['BidNumber', 'Product'],
                               right_on=['BidNumber', 'Product']).set_index('BidNumberProduct')

        if WriteToDiskFileName is None:
            return merged_data
        else:
            for k, group in merged_data.groupby('Product_Mode'):
                group['Weight_Volume'] = self.Weight_Volume_map[k]
                group.to_csv('./Reporting Support Files/Model Outputs/' + WriteToDiskFileName + k + '.csv')

    def run_calculator_production_sample(self, data):
        # check to see if there are bids to score.
        try:
            if data[data['Product_Mode'].isin(['AIR', 'GND', 'IE'])].shape[0] != 0:
                # compute optimal industry
                opt_incentive = self.run_calculator(data)

                # compute effective to freight
                opt_incentive_offered = self.apply_eff_to_freight(data, opt_incentive)
                deal_scoring_integration = self.deal_scoring_additional_output(opt_incentive_offered)

                # return post processed data
                logger.debug('******************Exiting function run_calculator_production_sample')
                return self.post_processing(deal_scoring_integration)
            # otherwise, return an empty dataframe
            else:
                return pd.DataFrame()

        except Exception, e:
            raise ValueError("Model calculation unsuccessful")

    ####################################################################################################################
    # Production
    ####################################################################################################################
    @log_function
    def post_processing(self, master_data):
        """
        The following function creates post processing and service spreading.
        """
        Inc_spread_high = self.post_process_settings['Inc_spread_high']
        Inc_spread_low = self.post_process_settings['Inc_spread_low']

        # svc matching input data
        svc_matching = self.svc_to_prod_file

        ### Compute accessorials incentives
        # Note: Incentive_Freight here refers specifically to GND_Resi only
        # Accessorials_results gives you the accessorials incentives by bids

        # No more the master_data is processed through the accessorials
        #accessorials_results = self.accessorials(master_data)

        ### Incentive ranges
        # by_product_results gives you the offered incentive ranges by each bid-product
        by_product_results = master_data[['BidNumber',
                                          'Bid_Region',
                                          'Bid_District',
                                          'Bid_Request_Reason_Descr',
                                          'Bid_List_Rev_Wkly',
                                          'Product',
                                          'Product_Mode',
                                          'Ind_Dominant_Industry',
                                          'List_Rev_Wkly',
                                          'Marginal_Cost_wkly',
                                          'Alpha',
                                          'Gamma',
                                          'Gross_OR_Ratio',
                                          'Comp_Incentive',
                                          'Pct_Rev_Freight',
                                          'Incentive_Freight_Estimated',
                                          'Prior_Incentive',
                                          'Overall_Incentive',
                                          'Eff_Off_Incentive_Diff_Estimate',
                                          'UpperBoundBind',
                                          'UpperBoundValue',
                                          'UpperBoundType',
                                          'LowerBoundBind',
                                          'LowerBoundValue',
                                          'LowerBoundType',
                                          'RelBoundBind',
                                          'Flag',
                                          'PriorInctest',
                                          'MktInc',
                                          'Constraints',
                                          'ceiling',
                                          'Weight_Volume',
                                          'Weight_PriceDiscip',
                                          'PTWonbid_optimal',
                                          'Optimal_UPSInc_-0.02',
                                          'Optimal_UPSInc_-0.01',
                                          'Optimal_UPSInc_0',
                                          'Optimal_UPSInc_0.01',
                                          'Optimal_UPSInc_0.02',
                                          'Obj_-0.02',
                                          'Obj_-0.01',
                                          'Obj_0',
                                          'Obj_0.01',
                                          'Obj_0.02',
                                          'PTWonBid_-0.02',
                                          'PTWonBid_-0.01',
                                          'PTWonBid_0',
                                          'PTWonBid_0.01',
                                          'PTWonBid_0.02',
                                          'Profit_-0.02',
                                          'Profit_-0.01',
                                          'Profit_0',
                                          'Profit_0.01',
                                          'Profit_0.02',
                                          'NetRev_-0.02',
                                          'NetRev_-0.01',
                                          'NetRev_0',
                                          'NetRev_0.01',
                                          'NetRev_0.02']]
        #Columns in by_product_results has been lowered according to the requirement
        by_product_results['Target_High'] = by_product_results['Incentive_Freight_Estimated'] + Inc_spread_high
        by_product_results['Target_Low'] = 0.0
        by_product_results.loc[by_product_results['Incentive_Freight_Estimated'] >= Inc_spread_low,
                               'Target_Low'] = by_product_results[by_product_results['Incentive_Freight_Estimated'] \
                                                                  >= Inc_spread_low].Incentive_Freight_Estimated - Inc_spread_low

        #IWA Caps
        # initialize a target high value
        by_product_results['Target_High_Shown_IWA'] = by_product_results['Target_High']

        for index, rows in by_product_results.iterrows():
            # if rows['List_Rev_Wkly'] == 0:
            #     rows['List_Rev_Wkly'] = 0.01

            max_lrm_or = 1 - rows['Marginal_Cost_wkly'] / (self.MAX_OR_VALUE * rows['List_Rev_Wkly'])
            target_high_iwa = min(rows['ceiling'], max(rows['Target_High'], min(max_lrm_or, rows['Prior_Incentive'])))
            by_product_results.loc[index, 'Target_High_Shown_IWA'] = target_high_iwa

        ### Service spreading
        # by_service_results gives you the offered incentive ranges by each bid-service
        by_service_results = pd.merge(by_product_results, svc_matching, how='inner').drop_duplicates()
        #by_service_results = by_service_results.merge(accessorials_results, how='outer')
        by_service_results = by_service_results.rename(columns={'Incentive_Freight_Estimated': 'Incentive_Freight'})
        logger.debug('******************Exiting function post_processing')
        return by_service_results

    def create_industry_from_mapping(self, data):
        industry_name_lookup = self.industry_name_lookup

        name_map = {'Apparel and Consumer Goods': 'Ind_Apparel_and_Consumer_Goods_Pct',
                    'Diversified Vehicles & Parts': 'Ind_Diversified_Vehicles_and_Parts_Pct',
                    'Government': 'Ind_Government_Pct',
                    'Healthcare': 'Ind_Healthcare_Pct',
                    'Industrial Products': 'Ind_Industrial_Products_Pct',
                    'Professional Services': 'Ind_Professional_Services_Pct',
                    'High Tech': 'Ind_High_Tech_Pct',
                    'Other': 'Ind_Other_Pct',
                    'Unclassified': 'Ind_Unclassified_Pct'}

        dom_ind_map = {'Ind_Apparel_and_Consumer_Goods_Pct': 'Apparel and Consumer Goods',
                       'Ind_Diversified_Vehicles_and_Parts_Pct': 'Diversified Vehicles & Parts',
                       'Ind_Government_Pct': 'Government',
                       'Ind_Healthcare_Pct': 'Healthcare',
                       'Ind_High_Tech_Pct': 'High Tech',
                       'Ind_Industrial_Products_Pct': 'Industrial Products',
                       'Ind_Other_Pct': 'Other',
                       'Ind_Professional_Services_Pct': 'Professional Services',
                       'Ind_Unclassified_Pct': 'Unclassified',
                       'Ind_NA_Pct': 'Unclassified'}

        industry_name_lookup['SIC_Industry'] = \
            industry_name_lookup['Business Defined SIC Industry Segment Descr'].map(name_map)

        mapped_data = pd.merge(data, industry_name_lookup,
                               left_on='SIC_CD', right_on='Business Defined SIC Industry 4 Cd',
                               how='left').fillna('Ind_NA_Pct')

        mapped_data_pivot = mapped_data.groupby(['NVP_BID_NR', 'SIC_Industry']) \
            ['SHR_AC_NR'].count().unstack().reindex(columns=name_map.values() + ['Ind_NA_Pct']).fillna(0)

        mapped_data_pivot_normalized = mapped_data_pivot.apply(lambda x: x / sum(x), axis=1)
        mapped_data_pivot_normalized['DominantIndustry'] = mapped_data_pivot_normalized.idxmax(axis=1)

        mapped_data_pivot_normalized['Ind_Dominant_Industry'] = mapped_data_pivot_normalized['DominantIndustry'].map(
            dom_ind_map)
        mapped_data_pivot_normalized.drop(['DominantIndustry'], axis=1, inplace=True)
        return mapped_data_pivot_normalized.reset_index()

    @log_function
    def deal_scoring_additional_output(self, offered_incentive):
        opt = []
        for k, group in offered_incentive.groupby('Product_Mode'):
            # The range has been modified according to the required columns.
            for i in range(-2,3,1):
                inc_val = i/100.0

                if inc_val == 0:
                    str_inc_val = "0"
                else:
                    str_inc_val = str("%.2f" % round(inc_val,2))

                opt_output = self.optfun(group['Optimal_UPSInc'] + inc_val, group, False)

                group['Optimal_UPSInc_' + str_inc_val] = group['Optimal_UPSInc'] + inc_val
                group['Obj_' + str_inc_val] = opt_output['Obj']

                group['PTWonBid_' + str_inc_val] = opt_output['PTWonbid']
                group['Profit_' + str_inc_val] = opt_output['BidProfit']
                group['NetRev_' + str_inc_val] = opt_output['PTWonbid'] * \
                                            group['List_Rev_Wkly'] * (1 - group['Optimal_UPSInc'] + inc_val)
            opt.append(group)
        logger.debug('******************Exiting function deal_scoring_additional_output')
        return pd.concat(opt, axis=0)

    @log_function
    def run_calculator_production(self, data, data_industry):
        # check to see if there are bids to score.
        try:
            if data[data['Product_Mode'].isin(['AIR', 'GND', 'IE'])].shape[0] != 0:

                # compute industry
                industry = self.create_industry_from_mapping(data_industry)
                data = pd.merge(data, industry, left_on='BidNumber', right_on='NVP_BID_NR')

                # compute optimal incentive
                opt_incentive = self.run_calculator(data)

                # compute effective to freight
                opt_incentive_offered = self.apply_eff_to_freight(data, opt_incentive)
                deal_scoring_integration = self.deal_scoring_additional_output(opt_incentive_offered)

                # return post processed data
                logger.debug('******************Exiting function run_calculator_production')
                return self.post_processing(deal_scoring_integration)
            # otherwise, return an empty dataframe
            else:
                return pd.DataFrame()

        except Exception, e:
            raise ValueError("Model calculation unsuccessful:" + e.message)

    def accessorials_original(self, master_data):
        # filter data into modes
        grouped_data = master_data.groupby(['BidNumber', 'Product_Mode',
                                             'Product', 'Bid_List_Rev_Wkly']).agg({'Optimal_UPSInc':'max'})

        # preset incentive values
        master_data['RESI'] = 0.0
        master_data['DAS'] = 0.0

        grouped_data = grouped_data.reset_index()
        filtered = grouped_data[grouped_data['Product_Mode'].isin(['GND', 'AIR', 'AIR_CWT', 'GND_CWT'])]

        # ie = master_data[master_data['Product_Mode'] == 'IE']

        # split RESI from DAS
        resi = self.accessorial[self.accessorial['Type'] == 'RES'].reset_index(drop=True)
        das = self.accessorial[self.accessorial['Type'] == 'GDL'].reset_index(drop=True)

        # iter through products. incentives coming is the discount value: 80% = 20% incentive
        resi_rows = resi.shape[0]
        das_rows = das.shape[0]

        for index, rows in filtered.iterrows():
            # iter through RESI
            for i, r in resi.iterrows():
                threshold = (rows['Optimal_UPSInc'] <= r['Breaks'])

                if threshold:
                    filtered.loc[index, 'RESI'] = r['Incentive']
                    break  # end early if found
                elif resi_rows == i + 1:  # last row, then set to this incentive
                    filtered.loc[index, 'RESI'] = r['Incentive']

            # iter through DAS
            for i, r in das.iterrows():
                threshold = (rows['Bid_List_Rev_Wkly'] <= r['Breaks'])

                if threshold:
                    filtered.loc[index, 'DAS'] = r['Incentive']
                    break  # end early if found
                elif das_rows == i + 1:  # last row, then set to this incentive
                    filtered.loc[index, 'DAS'] = r['Incentive']

        filtered = filtered.merge(self.accessorial_map, how='inner')
        filtered['Product_Mode'] = 'ACY'
        filtered = filtered.rename(columns={'Optimal_UPSInc' : 'Optimal_UPSInc_0'})
        return filtered

    @log_function
    def f_Get_Ceiling(self, MVM_DRC_CD, SVM_TYP_CD, ASY_SVC_TYP_CD, NCV_DTR_DAT_TYP_CD, band):
        # Calling the new accessorials input
        inc_ceilings = self.accessorial_ceiling
        ceiling = -1
        df = inc_ceilings[
            (inc_ceilings['MVM_DRC_CD'] == MVM_DRC_CD) &
            (inc_ceilings['SVM_TYP_CD'] == SVM_TYP_CD) &
            (inc_ceilings['ASY_SVC_TYP_CD'] == ASY_SVC_TYP_CD) &
            (inc_ceilings['NCV_DTR_DAT_TYP_CD'] == NCV_DTR_DAT_TYP_CD) &
            (inc_ceilings['LOW_BAND'] <= band) &
            (band < inc_ceilings['HIGH_BAND'])
            ]
        if not df.empty:
            ceiling = list(df['CEILING'])[0]

        if ceiling == -1:
            df = inc_ceilings[
                (inc_ceilings['MVM_DRC_CD'] == MVM_DRC_CD) &
                (inc_ceilings['SVM_TYP_CD'] == 'ALL') &
                (inc_ceilings['ASY_SVC_TYP_CD'] == ASY_SVC_TYP_CD) &
                (inc_ceilings['NCV_DTR_DAT_TYP_CD'] == NCV_DTR_DAT_TYP_CD) &
                (inc_ceilings['LOW_BAND'] <= band) &
                (band < inc_ceilings['HIGH_BAND'])
                ]
            if not df.empty:
                ceiling = list(df['CEILING'])[0]
        return ceiling

    @log_function
    def accessorials(self, master_data):

        # get the bid no
        bid_no = master_data['BidNumber'].unique()[0]
        Bid_List_Rev_Wkly = master_data['Bid_List_Rev_Wkly'].unique()[0]

        # use it to get the tp2 accessorials data
        from etl import connect
        db = connect('.oracledb.ups.com')
        squery = "SELECT * FROM V_TP20_ACCESSORIAL WHERE NVP_BID_NR = '" + bid_no + "'"
        tp2 = pd.read_sql(squery, con=db)

        if tp2.empty:
            # returns empty DataFrame if the bid has no accessorial data
            logger.debug('******************Exiting function accessorials with empty tp2')
            return pd.DataFrame()
        cols_l = ['ASY_LIS_RVN_A', 'ASY_UNT_QY', 'CCY_CD', 'CNY_CD']
        tp2 = tp2.drop(cols_l, axis=1)
        eligible = self.eligible_accessorials
        acc_to_score = pd.merge(eligible, tp2, how='inner',
                                on=['MVM_DRC_CD', 'SVM_TYP_CD', 'ASY_SVC_TYP_CD'])
        cols = ['MVM_DRC_CD', 'SVM_TYP_CD', 'ASY_SVC_TYP_CD', 'RULE_TYPE', 'Product']
        # Considering the required columns for scoring process
        acc_to_score = acc_to_score[cols]
        acc_to_score = acc_to_score.drop_duplicates(cols)
        acc_to_score['band'] = 0.0
        acc_to_score['Optimal_UPSInc_0'] = 'null'
        # assigning accessorial_ceiling
        inc_ceilings = self.accessorial_ceiling
        inc_typ = inc_ceilings['NCV_DTR_DAT_TYP_CD'].drop_duplicates().reset_index()

        for index, row in acc_to_score.iterrows():
            tmpprod = row['Product']
            #tmpstat = the_row['RULE_TYPE']

            for i, r in master_data.iterrows():
                if tmpprod == 'ANY' or row['Product'] == r['Product']:
                    row['band'] = r[row['RULE_TYPE']]
                    acc_to_score.loc[index, 'band'] = row['band']
                    break

        acc_to_score['tmp'] = 1
        inc_typ['tmp'] = 1
        # merge both the data-frames
        acc = pd.merge(acc_to_score, inc_typ, on='tmp')
        acc = acc.drop(['index', 'tmp'], axis=1)
        acc['DAS'] = 0.0
        acc['RESI'] = 0.0
        acc['BidNumber'] = bid_no
        acc['Bid_List_Rev_Wkly'] = Bid_List_Rev_Wkly
        acc['Optimal_UPSInc_0'] = acc.apply(
            lambda row: self.f_Get_Ceiling(row['MVM_DRC_CD'], row['SVM_TYP_CD'], row['ASY_SVC_TYP_CD'],
                                           row['NCV_DTR_DAT_TYP_CD'], row['band']), axis=1
        )
        acc['DAS'] = acc.apply(
            lambda row: row['Optimal_UPSInc_0'] if row['ASY_SVC_TYP_CD'] =='GLD' else row['DAS'], axis=1
        )
        acc['RESI'] = acc.apply(
            lambda row: row['Optimal_UPSInc_0'] if row['ASY_SVC_TYP_CD'] == 'RES' else row['RESI'], axis=1
        )
        acc = acc[acc['Optimal_UPSInc_0'] != -1]
        acc['Product_Mode'] = 'ACY'
        # Drop columns which are not required
        acc.drop(['Product', 'RULE_TYPE', 'band'], axis=1, inplace=True)
        logger.debug('******************Exiting function accessorials')
        return acc