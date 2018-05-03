#  SCCS Keywords  "%Z% %M%  %I%  %H%"

# -*- coding: utf-8 -*-

from __future__ import division
from itertools import chain
import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import KFold

class CategoricalTransformer(object):
    """
    Class to turn data into eff to freight ready data.
    """
    def __init__(self, variable_list):
        self.columns_ = None
        self.cat_columns_ = None
        self.non_cat_columns_ = None
        self.cat_map_ = None
        self.ordered_ = None
        self.dummy_columns_ = None
        self.transformed_columns_ = None
        self.variable_list = variable_list

    def datatype(self, data):
        """
        Function to create data type changes
        """
        cat_columns = np.unique([i for i in self.variable_list.keys()
                                 if self.variable_list[i] == 'str'])
        for i in cat_columns:
            data[i] = data[i].astype('str').astype('category')
        return data

    def fitcatvar(self, data):
        """
        Function to create categorical variable typs
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
            self.non_cat_columns_.tolist() +
            list(chain.from_iterable(self.dummy_columns_[k]
                                     for k in self.cat_columns_)))
    def transformcatvar(self, data):
        """
        Function to transform input data into eff to freight format data
        """
        model_data = self.datatype(data)
        return (pd.get_dummies(model_data)
                .reindex(columns=self.transformed_columns_)
                .fillna(0))

class EffToOffered(CategoricalTransformer):
    def __init__(self, variable_list):
        self.variable_list = variable_list

        # define all self variables in init
        self.cross_val = None
        self.variable_importance = None

    def fit(self, data, product_mode,trees=500, interaction_depth=5):
        data = data[data['Product_Mode'] == product_mode]
        self.fitcatvar(data)
        model_data = self.transformcatvar(data)
        indep_var = model_data.columns
        merged_data = pd.merge(data[['BidNumber', 'Eff_Off_Incentive_Diff']], 
                               model_data, left_index=True, right_index=True)
        regressor = GradientBoostingRegressor(n_estimators=trees, max_depth=interaction_depth)

        #Cross validation train set
        cross_val_predict = np.array([])
        kf = KFold(n_splits=3)
        for train_index, test_index in kf.split(merged_data,
                                                groups=merged_data['BidNumber']):
            train, test = merged_data.ix[train_index], merged_data.ix[test_index]
            regressor.fit(X=train[indep_var], y=train['Eff_Off_Incentive_Diff'])
            ypred = regressor.predict(test[indep_var])
            cross_val_predict = np.append(cross_val_predict, ypred)

        regressor.fit(X=model_data, y=data['Eff_Off_Incentive_Diff'])
        self.fit_obj = regressor
        self.cross_val = pd.DataFrame(cross_val_predict, index=model_data.index)
        self.cross_val.columns = ['Eff_Off_Incentive_Diff_Estimate']
        self.variable_importance = pd.DataFrame(regressor.feature_importances_,
                                                index=[indep_var], columns=['ImportanceScore'])
        
    def transform(self, data):
        model_data = self.transformcatvar(data)
        fit_object = self.cross_val
        df_temp = pd.merge(data, fit_object, left_index=True, right_index=True, how='left')
        df_insample = df_temp.dropna(subset=['Eff_Off_Incentive_Diff_Estimate'])

        #between in sample and out of sample data for reporting
        df_outsample = df_temp[df_temp['Eff_Off_Incentive_Diff_Estimate'].isnull()]

        if df_temp.shape[0] != df_insample.shape[0]:
            df_outsample = df_outsample.sort_index()
            df_outsample = df_outsample.drop('Eff_Off_Incentive_Diff_Estimate', axis=1)
            df_outsample['Eff_Off_Incentive_Diff_Estimate'] = \
                self.fit_obj.predict(model_data[model_data.index.isin(df_outsample.index)])

        df_pred = pd.concat([df_insample, df_outsample], axis=0)

        return df_pred