#  SCCS Keywords  "%Z% %M%  %I%  %H%"

# -*- coding: utf-8 -*-

from __future__ import division
from itertools import chain
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import KFold

class MarketingIncentiveCategoricalTransformer(object):
    """
    Class to prep data for market incentive
    """
    def __init__(self, variable_list):
        self.variable_list = variable_list
        self.columns_ = None
        self.cat_columns_ = None
        self.non_cat_columns_ = None
        self.cat_map_ = None
        self.ordered_ = None
        self.dummy_columns_ = None
        self.transformed_columns_ = None

    def datatype(self, data):
        """
        A function to set data types
        """
        cat_columns = np.unique([i for i in self.variable_list.keys()
                                 if self.variable_list[i] == 'str'])
        for i in cat_columns:
            data[i] = data[i].astype('str').astype('category')
        return data

    def fitcatvar(self, data):
        """
        A function to create the market incentive variables
        Args:
            data (pandas dataframe)
        """
        model_data = data[self.variable_list.keys()]
        model_data = self.datatype(model_data)
        self.columns_ = model_data.columns
        self.cat_columns_ = model_data.select_dtypes(include=['category']).columns
        self.non_cat_columns_ = model_data.columns.drop(self.cat_columns_)
        self.cat_map_ = {col: model_data[col].cat.categories
                         for col in self.cat_columns_}
        self.ordered_ = {col: model_data[col].cat.ordered
                         for col in self.cat_columns_}
        self.dummy_columns_ = {col: ["_".join([col, v])
                                     for v in self.cat_map_[col]]
                               for col in self.cat_columns_}
        self.transformed_columns_ = pd.Index(
            self.non_cat_columns_.tolist() + \
            list(chain.from_iterable(self.dummy_columns_[k]
                                     for k in self.cat_columns_)))

    def transformcatvar(self, data):
        """
        A class to turn data into market incentive dummied data
        Args:
            data (pandas dataframe)
        """
        model_data = data[self.variable_list.keys()]
        model_data = self.datatype(data)
        return (pd.get_dummies(model_data)
                .reindex(columns=self.transformed_columns_)
                .fillna(0))


class OOBMarketPriceRegressor(MarketingIncentiveCategoricalTransformer):
    """
    The OOBMarketPriceRegressor class transforms the input data and fits a random forest model.
    
    Overview:
        The class is made up of two methods, fit and transform. The `fit` method transforms the data using
        MarketingIncentiveCategoricalTransformer class, fits a random forest model to the complete data, 
        fits random forests models to random subsets of the data (3 fold cross validation), and returns a 
        dictionary including model objects, insample OOB estimates, insample CV estimates, and the 
        variable importance data frame.
        
    For more information on the random forest regression package used, please refer to the 
    RandomForestRegressor documentation found in Sklearn Ensemble section of scikit-learn.org 
    (http://scikit-learn.org). 

    """
    def __init__(self, variable_list):
        """
        Args:
            variable_list (dict): dict of variable names and variable types
        """
        self.variable_list = variable_list
        self.fit_object = None

    def fit(self, data, cores=-1, trees=500, mx_features='auto', min_sample_split=30, perfoming_bid_cutoff=0.75):
        """
        A function to fit market incentive.
        Args:
            data (pandas dataframe): input data.
            cores (int): number of cores available to fit the data. 
            The default is -1 (all cores).
            trees (int): number of trees to grow in the random forest.
            mx_features (str or int): 'auto' (all features), 
            'sqrt' (square root of all features),
            or number of trees.
            min_sample_split (int): minimum # of samples needed for a split
            performing_bid_cutoff (int): pull through cut off performing bids
        Returns:
            An object.
            The fit object is a dictionary with the following element:
                1) rf_regressor: sklearn random forest fit object
                2) inbag (pandas dataframe): insample OOB dataframe
                3) inbag_crossval (pandas dataframe): insample 3 fold CV
                4) variable_importance (pandas dataframe): variable importance score
                    extracted from the rf_regressor by `rf_regressor.feature_importances_`.
                    Note: the higher, the more important the feature.
        """

        data = data.sort_index()
        self.fitcatvar(data)
        model_data = self.transformcatvar(data)
        rf_regressor = RandomForestRegressor(n_jobs=cores, 
                                             n_estimators=trees,
                                             oob_score=True,
                                             max_features=mx_features,
                                             min_samples_split=min_sample_split)
        #build cross validation model
        indep_var = model_data.columns
        merged_data = pd.merge(data[['BidNumber', 'Overall_Incentive', 'PullThruRevWonBids']], 
                               model_data, left_index=True, right_index=True)
        ################################################################################
        #Cross validation train set
        cross_val_predict = np.array([])
        kf = KFold(n_splits=3) #3 fold cross validation. 
        for train_index, test_index in kf.split(merged_data,
                                                groups=merged_data['BidNumber']):
            train, test = merged_data.ix[train_index], merged_data.ix[test_index]
            # subset performing bids
            # setting this parameter to 0 will include all bids.
            train = train[train['PullThruRevWonBids'] >= perfoming_bid_cutoff]
            rf_regressor.fit(X=train[indep_var], 
                             y=train['Overall_Incentive'])
            ypred = rf_regressor.predict(test[indep_var])
            cross_val_predict = np.append(cross_val_predict, ypred)
        ################################################################################
        train = merged_data
        train = train[train['PullThruRevWonBids'] >= perfoming_bid_cutoff]
        rf_regressor.fit(X=train[indep_var], 
                         y=train['Overall_Incentive'])
        inbag = pd.DataFrame(rf_regressor.oob_prediction_)
        inbag['BidNumberProduct'] = train.index
        inbag.columns = ['Normal_Incentive_Perf', 'BidNumberProduct']
        inbag = inbag.set_index('BidNumberProduct').dropna()
        inbag_crossval = pd.DataFrame(cross_val_predict)
        inbag_crossval['BidNumberProduct'] = merged_data.index
        inbag_crossval.columns = ['Normal_Incentive_Perf_CV', 'BidNumberProduct']
        inbag_crossval = inbag_crossval.set_index('BidNumberProduct')
        #store random forest inbag, 3 fold cross validation score, and variable importance
        self.fit_object = {'rf_regressor': rf_regressor, 
                           'inbag': inbag,
                           'inbag_crossval': inbag_crossval,
                           'variable_importance': pd.DataFrame(rf_regressor.feature_importances_, 
                                                               index=indep_var, columns=['ImportanceScore'])}
    def transform(self, data):
        """
        A function to create in and out sample prediction for market incentive. 
        
        The function returns the data with two additional columns, Normal_Incentive_Perf 
        and Normal_Incentive_Perf_CV.  For an insample bid, a bid in the train set,
        Normal_Incentive_Perf will be the random forest OOB estimates and the 
        Normal_Incentive_Perf_CV will be the insample 3 fold cross validation (CV) estimates.
        For and out of sample bid, a bid not in the train set, the Normal_Incentive_Perf will
        bet the out of sample prediction of the random forest model while the 
        Normal_Incentive_Pref_CV will return NA since no CV estimates are available for out of 
        sample bids.
        
        Args:
            data (pandas dataframe): input data.
        Returns:
            df (pandas dataframe): same as the input data with an additional 
            column Normal_Incentive_Perf.    
        """
        df_temp = pd.merge(data, 
                           self.fit_object['inbag'], left_index=True,
                           right_index=True, how='left')
        ###########################################################################
        # Test if any of the data was in sample and grab it from the insample list.
        df_insample = df_temp.dropna(subset=['Normal_Incentive_Perf'])
        df_outsample = df_temp[df_temp['Normal_Incentive_Perf'].isnull()]
        ###########################################################################
        if df_temp.shape[0] != df_insample.shape[0]:
            df_outsample = df_outsample.sort_index()
            df_outsample = df_outsample.drop('Normal_Incentive_Perf', axis=1)
            model_data = self.transformcatvar(df_outsample)
            df_outsample['Normal_Incentive_Perf'] = \
                        self.fit_object['rf_regressor'].predict(model_data)
        df_pred = pd.concat([df_insample, df_outsample], axis=0)
        ###########################################################################
        # Add cross validation to the data for reporting
        df_pred = pd.merge(df_pred, self.fit_object['inbag_crossval'],
                           left_index=True, right_index=True, how='left')
        ###########################################################################
        return df_pred
