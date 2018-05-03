#  SCCS Keywords  "%Z% %M%  %I%  %H%"

# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from sklearn.preprocessing import Imputer
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from itertools import chain
from statsmodels.formula.formulatools import dmatrices
from sklearn.model_selection import KFold
from sklearn.utils import shuffle
import statsmodels.api as sm

class CategoricalTransformer():
    """
    Class to create volume model wide format data
    """
    def __init__(self, bid_var_list, prod_var_list):
        """
        Args:
            bid_var_list (dict): bid level variable lists
            prod_var_list (dict): product level variable list
        """
        self.bid_var_list = bid_var_list
        self.prod_var_list = prod_var_list

    def datatype(self, data):
        """
        Function to turn object variables into categorical variables

        Args:
            data (pandas data frame): input data
        Returns:
            (pandas data frame):
        """
        cat_col = [i for i in data.columns if i in self.cat_col_list]

        for i in cat_col:
            data[i] = data[i].astype('str').astype('category')
        return data
    
    def dummier(self, data):
        """
        Function to save dummied column names

        Args:
            data (pandas data frame): input data
        Returns:
            (pandas data frame):
        """
        cat_columns_ = data.select_dtypes(include=['category']).columns
        non_cat_columns_ = data.columns.drop(cat_columns_)
        cat_map_ = {col: data[col].cat.categories
                         for col in cat_columns_}        
        dummy_columns_ = {col: ["_".join([col, v])
                                     for v in cat_map_[col]]
                               for col in cat_columns_} 
        transformed_columns_ = pd.Index(non_cat_columns_.tolist() +
                    list(chain.from_iterable(dummy_columns_[k] for k in cat_columns_)))
        return transformed_columns_

    def fit_data_prep(self, data):
        """
        A function to create the volume model variables

        Args:
            data (pandas dataframe)
        Returns:
            (pandas dataframe):
        """
        self.cat_col_list = np.unique([i for i in self.bid_var_list.keys() 
                                 if self.bid_var_list[i] == 'str'] + 
                                [i for i in self.prod_var_list.keys()
                                 if self.prod_var_list[i] == 'str'])
        data = data.set_index('BidNumber')
        df_bid = data[self.bid_var_list.keys()].groupby(data.index).first()
        df_prod = data[self.prod_var_list.keys()]
        df_bid = self.datatype(df_bid)
        df_prod = self.datatype(df_prod)
        transformed_columns_prod_dict = {}
        for k, gp in df_prod.groupby('Product'):
            transformed_columns_prod_dict.update({k:self.dummier(data=gp.drop('Product', axis = 1))})
        self.transformed_columns_bid = self.dummier(data=df_bid)
        self.transformed_columns_prod_dict = transformed_columns_prod_dict
        transformed_col = []
        for i in transformed_columns_prod_dict.keys():
            transformed_col.append([x + '_' + i for x in transformed_columns_prod_dict[i]])
        self.transformed_col = list(chain.from_iterable(transformed_col)) +\
                                [i + '_ALL' for i in self.transformed_columns_bid]

    def transform_data_prep(self, data):
        """
        A class to turn data into volume model dummied data
        Args:
            data (pandas dataframe)
        Returns:
            (pandas data frame)
        """
        data = data.set_index('BidNumber')
        df_bid = data[self.bid_var_list.keys()].groupby(data.index).first()
        df_prod = data[self.prod_var_list.keys()]

        df_bid = self.datatype(df_bid)
        df_prod = self.datatype(df_prod)
        append_data = []

        # add ALL suffix to bid level features
        append_data.append(pd.get_dummies(df_bid)
                           .reindex(columns=self.transformed_columns_bid)
                           .add_suffix('_' + "ALL"))
        # create dummy data for
        for k, group in df_prod.groupby('Product'):
            append_data.append(pd.get_dummies(group)
                               .reindex(columns=self.transformed_columns_prod_dict[k])
                               .add_suffix('_' + k))

        return(pd.concat(append_data, axis = 1, join = 'outer')
               .reindex(columns = self.transformed_col)
               .fillna(-1000)) # when a product is not available fill the data with -1000


class PreProcessData():
    """
    Class to preprocess data before modeling step
    """
    def __init__(self, variable_list):
        """
        Args:
            variable_list (dict): input variable list
        """
        self.variable_list = variable_list

        #variable types
        self.variable_list = self.concat_dict(self.variable_list)

        self.cat_variables = [i for i in self.variable_list.keys()
                              if self.variable_list[i] == 'str']
        self.cont_variables = [i for i in self.variable_list.keys()
                               if self.variable_list[i] != 'str']

    def concat_dict(self, data):
        """
        Concat dictionaries
        """
        all_data = {}
        for i in data:
            all_data.update(i)
        return all_data

    def clean_data(self, data):
        """
        Function to add preprocessing to the data

        Args:
            data (pandas data frame):
        Returns:
            (pandas data frame)
        """
        # set prior incentive to 0 when the bid is conversion and prior incentive is missing
        data.loc[(data['Prior_Incentive'].isnull()) &
                 (data['Bid_Request_Reason_Descr'] == 'CONVERSION'),
                 'Prior_Incentive'] = 0

        #add unique index of bidnumber product
        data['BidNumberProduct'] = data['BidNumber'] + '_' + data['Product']

        #filter mode
        data = data[data['Product_Mode'].isin(['GND', 'AIR', 'IE'])]

        #fill missing pullthru
        return data.set_index('BidNumberProduct').sort_index()

    def fit(self, data, strategy):
        """
        Function to fit preprocess. Pre process function imputes missing

        Args:
            data (pandas data frame): input data
            strategy (str): 'simple' is the only type implemented. It imputes the missing continuous data with its
            mean.
        Returns:
        """
        data = self.clean_data(data)
        if strategy == 'simple':
            impute = Imputer(strategy='mean')
            impute.fit(data[self.cont_variables])
            self.impute_rule = impute
        else:
            pass
    def transform(self, data, strategy):
        """
        Function to transform data using preprocess.

        Args:
            data (pandas data frame): input data
            strategy (str): 'simple'
        Returns:
            (pandas data frame)
        """
        data = self.clean_data(data)
        output_data = pd.DataFrame()
        if strategy == 'simple':
            cols = [i for i in data.columns if i not in self.cont_variables]
            output_data = pd.concat([data[cols],
                                     pd.DataFrame(self.impute_rule.transform(data[self.cont_variables]),
                                                  index=data.index, columns=self.cont_variables)], axis=1)
        else:
            output_data = data
        return output_data.sort_index()


class WinLossOOBRandomForest(CategoricalTransformer):
    """
    Class to create win loss variable
    """
    def __init__(self, bid_var_list, prod_var_list):
        """
        Args:
            bid_var_list (dict): bid level variables
            prod_var_list (dict): product level variables
        """
        self.bid_var_list = bid_var_list
        self.prod_var_list = prod_var_list

        # define all class variables in init
        self.inbag = None
        self.rf_classifier = None
        self.variable_importance = None

    def fit(self, data, cores, trees, mx_features):
        """
        Fit function for random forest. This is written based on RandomForestClassifier in sklearn. Please refer to
        scikit-learn.org for full documentation.

        Args:
            data (pandas data frame): input data
            cores (int): number of cores to use. -1 is max
            trees (int): number of trees
            mx_features (str):

        Return:
            sklearn random forest classifier object. It includes oob estimates, fit function, etc
        """
        #create data
        self.fit_data_prep(data)
        model_data = self.transform_data_prep(data)
        indep_var_list = model_data.columns
        dep_var = data.groupby('BidNumber').first()[['Bid_Win_Flag_Updated']]        
        model_data_merged = pd.merge(dep_var, model_data, left_index=True,
                                     right_index=True)
        rf_classifier = RandomForestClassifier(n_jobs=cores, n_estimators=trees,
                                               oob_score=True, max_features=mx_features)

        rf_classifier.fit(X=model_data_merged[indep_var_list],
                          y = model_data_merged['Bid_Win_Flag_Updated'])
        self.rf_classifier = rf_classifier
        inbag = pd.DataFrame(rf_classifier.oob_decision_function_,
                             index=model_data_merged.index)[[0]]
        inbag.columns = ['Bid_Win']
        inbag = inbag.dropna().sort_index()
        self.inbag = inbag
        self.variable_importance = pd.DataFrame(rf_classifier.feature_importances_, 
                                                index=indep_var_list, columns=['ImportanceScore'])

    def transform(self, data):
        """
        Function to create win loss variable using input data. It returns that data with an additional column, Bid_Win,
        probability of winning

        Args:
            data (pandas data frame): input data
        Returns:
            (pandas data frame):
        """
        model_data = self.transform_data_prep(data)    
        df_temp = pd.merge(model_data, 
                           self.inbag,
                           left_index=True, 
                           right_index=True,
                           how='left')
        df_insample = df_temp.dropna(subset=['Bid_Win'])
        df_outsample = df_temp[df_temp['Bid_Win'].isnull()]
        if df_temp.shape[0] != df_insample.shape[0]:
            df_outsample = df_outsample.sort_index()
            df_outsample = df_outsample.drop('Bid_Win', axis=1)
            df_outsample['Bid_Win'] = \
                        self.rf_classifier.predict_proba(df_outsample)[:,0]
        df_pred = pd.concat([df_insample, df_outsample], axis=0)
        return df_pred.sort_index()


class OOBRandomForestRegressor():
    """
    Class to fit base market pull through. The random forest model utilizes the sklearn RandomForestRegressor function.
    For more information refer to scikit-learn.org.
    """
    def __init__(self):
        # define all class variables in the init
        self.fit_object = None

    def logit(self, data):
        """
        Function to apply logistic transformation
        """
        data = [.0001 if i == 0 else i for i in data]
        data = [.9999 if i == 1 else i for i in data]
        data = np.array(data)
        return np.log(data/(1 - data))

    def fit(self, X, data, cores, trees, mx_features):
        """
        Function to fit market base pull through regression

        Args:
            X (pandas data frame): wide format data
            data (pandas data frame): input data
            cores (int): number of cores to use. -1 is all cores.
            trees (int): number of trees to use
            mx_features (int or str): number of features to use. Refer to sklearn RandomForestRegression manual.
        Returns:
            fit object including fitted model, variable importance, in bag estimators, cross validation estimators
        """
        indep = X.columns
        fit_object = {}
        for k, group in data.groupby('Product'):
            df = pd.merge(group[['BidNumber', 'PullThruRevWonBids']], X, left_on=['BidNumber'], right_index=True)
            df = shuffle(df)
            rf = RandomForestRegressor(n_jobs=cores, n_estimators=trees, oob_score=True, max_features=mx_features)

            ##########################################################
            # cross validation estimator
            cross_val_predict = np.array([])
            kf = KFold(n_splits=3)
            for train_index, test_index in kf.split(df):
                train, test = df.ix[train_index], df.ix[test_index]
                rf.fit(X=train[indep], y=train['PullThruRevWonBids'])
                ypred = rf.predict(test[indep])
                cross_val_predict = np.append(cross_val_predict, ypred)
            ##########################################################

            rf.fit(X=df[indep], y=df['PullThruRevWonBids'])
            inbag = pd.DataFrame(rf.oob_prediction_, index=df['BidNumber']).dropna()
            inbag.columns = ['Base_PT_Pred']
            inbag['Base_PT_Pred_CV'] = cross_val_predict
            fit_object.update({k:{'rf_regressor': rf, 'inbag': inbag, 
                                  'variable_importance': pd.DataFrame(rf.feature_importances_, 
                                                                      index=indep, columns=['ImportanceScore'])}})
        self.fit_object = fit_object

    def predict(self, model_data, data):
        """
        Function to predict base market pull through. When trained data is present, the function returns the out of bag
        estimates. Otherwise, predictions are returned.
        """
        append_data = []
        for k, gp in data.groupby('Product'):
            fit_object = self.fit_object[k]
            df_temp = pd.merge(gp, fit_object['inbag'], 
                               left_on=['BidNumber'], 
                               right_index=True, how='left')
            df_insample = df_temp.dropna(subset=['Base_PT_Pred'])
            #between in sample and out of sample data for reporting
            df_outsample = df_temp[df_temp['Base_PT_Pred'].isnull()]
            if df_temp.shape[0] != df_insample.shape[0]:
                df_outsample = df_outsample.sort_index()
                df_outsample = df_outsample.drop('Base_PT_Pred', axis=1)
                df_outsample['Base_PT_Pred'] = \
                    fit_object['rf_regressor'].predict(model_data[model_data.index.isin(df_outsample['BidNumber'])])
            df_pred = pd.concat([df_insample, df_outsample], axis=0)
            append_data.append(df_pred)

        df_pred_all = pd.concat(append_data, axis=0)
        df_pred_all['Base_PT_Pred_tfed'] = self.logit(df_pred_all['Base_PT_Pred'])
        return df_pred_all

    def transform(self, data, isCrossval):
        """
        This function is mainly used for calibration reporting. It returns inbag or cross validation estimates.
        """
        append_data = []
        for k, gp in data.groupby('Product'):
            fit_object = self.fit_object[k]
            df_insample = pd.merge(gp, fit_object['inbag'], left_on=['BidNumber'], right_index=True)
            append_data.append(df_insample)

        df_pred = pd.concat(append_data, axis=0)

        if isCrossval:
            df_pred['Base_PT_Pred_tfed'] = self.logit(df_pred['Base_PT_Pred_CV'])
        else:
            df_pred['Base_PT_Pred_tfed'] = self.logit(df_pred['Base_PT_Pred'])
        return df_pred
    
class MarketPullThruFractionalModel():
    """
    Class to fit market incentive logistic regression.
    """
    def __init__(self, variable_list):
        self.variable_list = variable_list

        # creating variables using the formula style. Refer to the user manual for more information.
        interactions = ['np.log(Eff_Price_to_Market)' + ':' + i for i in variable_list.keys()]
        if len(interactions) == 0:
            self.model = 'Base_PT_Pred_tfed + np.log(Eff_Price_to_Market)'
        else:
            self.model = 'Base_PT_Pred_tfed + np.log(Eff_Price_to_Market) + ' + ' + '.join(interactions)

        self.params = None
        self.fit_obj = None
        self.variable_names = None
        self.coef_list = None
        self.categorical_variables = None

    def fit(self, data):
        """
        Funtion to fit market incentive logistic regression.
        """
        outcome, predictors = dmatrices("PullThruRevWonBids ~" + self.model, data)
        fit = sm.Logit(endog=np.array(outcome), exog=np.array(predictors)).fit(cov_type='HC0')
        self.params = fit.params.tolist()
        self.fit_obj = fit
        self.variable_names = predictors.design_info.column_names
        self.coef_list = pd.DataFrame(self.params, index=self.variable_names, columns=['values'])
        design_info = {}
        for i in predictors.design_info.factor_infos.keys():
            feature = predictors.design_info.factor_infos[i]
            if feature.type == 'categorical':
                design_info.update({feature.state['eval_code']: feature.categories})
        self.categorical_variables = design_info

    def predict(self, newdata):
        """
        Function to predict market pull through.
        """
        for i in self.categorical_variables.keys():
            newdata[i] = pd.Categorical(newdata[i], categories=self.categorical_variables[i])
        outcome, predictors = dmatrices('1~' + self.model, newdata)
        fit = self.fit_obj

        return fit.predict(np.array(predictors))