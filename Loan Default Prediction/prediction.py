import json
import os
from xgboost import XGBClassifier
from utils import *

config = json.loads( open("config.json","r").read() )
os.makedirs( f"temp_{config['n_splits']}", exist_ok= True )
data = pd.read_csv( config["prediction_file_path"]  )
folds = config['n_splits']

handling_isnum(data)

preprocessing(data)

exclude_cols = [ "customer_id", "firstname", "lastname" ]
target_cols = ["primary_close_flag","final_close_flag"]
prepared_data = data.drop(exclude_cols, axis =1 )

for target_col in target_cols:

    preds = np.zeros(len(data), dtype = np.float64)
    for i in range(folds):
        model_path  = f"xgboost_model_ensemble_{i}_{target_col}.json"
        model = XGBClassifier()
        model.load_model(model_path)
        print(model.best_ntree_limit)
        preds +=model.predict_proba(prepared_data, iteration_range=[0, model.best_ntree_limit])[:,1]
    preds /= folds
    data[target_col] = preds.round(0)
data = data[ target_cols ]
data.to_csv( f"./temp_{config['n_splits']}/submission.csv", index =False )