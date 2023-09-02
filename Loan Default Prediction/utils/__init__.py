import pandas as pd
import numpy as np

def one_hot_encoder( df:pd.DataFrame, col, unique_count ):
    temp = np.zeros( (len(df), unique_count), dtype = np.int8 )
    temp[ np.arange(len(df)), df[col].values ] = 1
    df.loc[: ,[ f"{col}_{_}" for _ in range(unique_count) ] ] = temp
    df.drop( col, axis=1, inplace=True )
    
def handling_isnum(df: pd.DataFrame):
    df.reset_index(drop=True, inplace=True)
    for i in [ "encoded_payment_24", "encoded_payment_20", "encoded_payment_11"]:
        df[i].fillna( 0, inplace = True )  
    for i in [ f'encoded_payment_{_}' for _ in range(25) ]:
        df[i] =df[i].fillna( 4).astype(np.int8)
        
        one_hot_encoder( df, i, 5 )

def preprocessing(df:pd.DataFrame):

    columns = [x for x in df.columns if "is" in x]
    for x in columns:

        df[x] = df[x].str.replace( "Yes", "1" ).str.replace("No", "0").astype(np.int8)
        one_hot_encoder( df, x, 2 )

    col = "encoded_loans_account_currency"
    one_hot_encoder( df, col, 4 )

    col = "encoded_loans_credit_type"
    one_hot_encoder( df, col,8 )

    col = "encoded_loans_account_holder_type"
    one_hot_encoder( df, col, 7 )

    col = "encoded_loans_credit_status"
    one_hot_encoder( df, col, 7 )
