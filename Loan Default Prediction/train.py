import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from xgboost.callback import  EarlyStopping
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, f1_score
import gc
import json
from utils import *

config = json.loads( open("config.json","r").read() )

home_path = config['path']
seed = config['seed']
n_split = config['n_splits']


train = pd.read_csv( home_path + "train.csv" )

    
handling_isnum(train)

preprocessing(train)



for target_col in ["final_close_flag","primary_close_flag"]:

    cat_cols = list(train.columns[ train.nunique() == 2 ])
    exclude_cols = [ "customer_id", "firstname", "lastname",  "final_close_flag", "primary_close_flag" ]

    cat_cols.remove(target_col)

    for x in exclude_cols:
        try:
            cat_cols.remove(x)
        except ValueError :
            pass

    exclude_cols.remove(target_col)

    folds = StratifiedKFold( n_split, shuffle= True , random_state= seed )

    fit_data = train.drop(exclude_cols, axis =1 )


    def get_model(  ):


        # for xgboost 
        early_ = EarlyStopping(rounds =config['early_stoppping_params']['nrounds'],
                                metric_name=config['early_stoppping_params']['metric'], save_best =True, maximize =False )


        return XGBClassifier( tree_method ="hist", n_estimators = 10000,
                            max_leaves = 0,max_bin =512,grow_policy ="depthwise",learning_rate = 0.01,verbosity= 1,booster= "gbtree"
                            ,n_jobs= 6,min_child_weight =1,subsample =0.1,
                            colsample_bytree= 1,colsample_bylevel= 1,colsample_bynode = 1,
                            scale_pos_weight = 100*(1 - (train[target_col].sum() / len(train))) ,random_state = seed,
                            eval_metric= ["logloss","auc"],  callbacks = [early_] ) 
    
    score = 0
    ensemble = 0
    for fit_index, valid_index in folds.split( train.drop( exclude_cols, axis = 1 ), train[target_col] ):
        fit_data_x = fit_data.drop(target_col,axis=1).iloc[fit_index]
        valid_x = fit_data.drop(target_col,axis=1).iloc[valid_index]

        fit_data_y = fit_data[target_col].iloc[fit_index]
        valid_y = fit_data[target_col].iloc[valid_index]


        model = get_model()

        # for xgboosting 
        model = model.fit( fit_data_x, fit_data_y, eval_set=[ ( valid_x, valid_y ) ], verbose = True )
        model.save_model( f"xgboost_model_ensemble_{ensemble}_{target_col}.json" )
        ensemble +=1
        pred = model.predict_proba(valid_x, iteration_range=[0,model.best_ntree_limit])[:,1]
        score+=roc_auc_score( valid_y, pred ) 
        pred[pred>=0.5] =1
        pred[pred<0.5] = 0
        print( f1_score( valid_y, pred ,average = "weighted")  )
        del model, pred, valid_x, valid_y, fit_data_x, fit_data_y
        gc.collect()
    print("final overall score at {target_col}".upper(),score / n_split)