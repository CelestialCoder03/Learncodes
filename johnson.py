
# coding: utf-8

# In[1]:


from sklearn.preprocessing import OneHotEncoder
import pandas as pd
import numpy as np
import operator
import sklearn
import os


def cd(path):
    os.chdir(os.path.expanduser(path))


def get_cols(k, L):
    return [l for l in L if k == l[0]]

def d_n(n, cpu_count, base=10**6):
    if isinstance(n, int):
        return (int(n / base / cpu_count) + 1) * cpu_count
    elif isinstance(n, list):
        return sum([d_n(nn) for nn in n])
    else:
        try:
            return int(n.size / 1024 ** 2 / cpu_count + 1) * cpu_count
        except:
            print('WARNING: Defaultly define the workers as %s' %
                  (cpu_count * 20))
            return cpu_count * 20

def cal_exp(df, N):
    Res = []

    if isinstance(N, int):
        N = [N]

    for dt in df.loc[:, 'date_x'].drop_duplicates():
        tmp_df = df.loc[(df.date_x == dt) & (df.date_y <= dt), :]
        if len(tmp_df) <= 0:
            continue

        exp_list = list(zip(tmp_df.date_x - tmp_df.date_y, tmp_df.exposure))
        res = []
        for n in N:
            s = sum([
                exp * 0.5**(gap.days / n) for gap, exp in exp_list
                if gap.days <= 45
            ])
            res.append((n, s))

        res = pd.DataFrame(res)
        res.index = [dt] * len(res)
        Res.append(res)

    if Res == []:
        return None
    else:
        return pd.concat(Res).reset_index()


def prepare_mat(df , cate_cols = None):
    numeric_sig = [np.issubdtype(d, np.number) for d in df.dtypes]
    numeric_mat = df.loc[:, numeric_sig]
    zero_cols = numeric_mat.columns[numeric_mat.sum() == 0]
    numeric_mat.drop(columns=zero_cols, inplace=True)
    no_variance = numeric_mat.max() == numeric_mat.min()
    numeric_mat = numeric_mat.loc[:,~no_variance]

    if cate_cols == []:
        return numeric_mat
    elif cate_cols is None:
        cate_mat = df.loc[:, df.dtypes == 'object']
    else:
        cate_mat = df.loc[: , cate_cols]
    cate_mat_df = onehot_cate_mat(cate_mat)
    cate_mat_df.index = numeric_mat.index
    return pd.concat([numeric_mat , cate_mat_df] , axis=1)

def onehot_cate_mat(cate_mat):
    enc = OneHotEncoder().fit(cate_mat)
    cate_mat_transformed = enc.transform(cate_mat).toarray()
    cate_mat_df = pd.DataFrame(cate_mat_transformed)
    cate_mat_df.columns = enc.get_feature_names()
    cache = []; rm_cols = []
    for col in cate_mat_df.columns:
        n = col.split('_')[0]
        if n in cache:
            continue
        else:
            rm_cols.append(col)
            cache.append(n)
    return cate_mat_df.drop(columns=rm_cols)

def backwards_stepwise(func, x, y, keep_list, b=0.1):
    sig = 'Done'
    remained_cols = list(x.columns)

    while True:
        try:
            model = func(y, x.loc[:, remained_cols]).fit()
        except:
            model = func(y, x.loc[:, remained_cols]).fit(method='powell')
        pvals = model.pvalues.reset_index().values
        pvals2 = [(col, p) for col, p in pvals if col not in keep_list + ['intercept']]
        col, p = max(pvals2, key=operator.itemgetter(1))
        if p > b:
            print('%s has been removed, due to p-val is %s larger than %s'
                  % (col, p, b))
            remained_cols.remove(col)
        elif np.isnan(p):
            sig = 'Fail'
            return model, remained_cols, sig
        else:
            return model, remained_cols, sig


def sign_correction(func, x, y, sign_d, keep_list, ad_cols , **kwargs):

    if isinstance(func, sklearn.linear_model.logistic.LogisticRegression):
        is_sklearn = True
        kwargs = {**func.get_params() , **kwargs}
        func.set_params(**kwargs)
    else:
        is_sklearn = False

    pos_list = sign_d['pos'] + ad_cols
    neg_list = sign_d['neg']

    remained_cols = list(x.columns)
    Len = len(remained_cols)

    while True:
        tmp = x.loc[:,remained_cols]
        if is_sklearn == True:
            model = func
            model.fit(tmp, y)
            params = model.params = list(zip(tmp.columns, model.coef_[0]))
        else:
            try:
                model = func(y, tmp).fit()
            except:
                model = func(y, tmp).fit(method='powell')
            model.params = model.params.reset_index().values
        for col, param in params:
            if col in keep_list:
                continue
            elif param > 0 and col in neg_list:
                print('%s has been removed, due to params is %s'%(col , param))
                remained_cols.remove(col)
            elif param < 0 and col in pos_list:
                print('%s has been removed, due to params is %s' % (col, param))
                remained_cols.remove(col)
            else:
                continue

        if Len == len(remained_cols):
            break
        else:
            Len = len(remained_cols)

    return model, remained_cols

def combine_params(model_d={}):
    L = []
    for key , model in model_d.items():
        if isinstance(model, sklearn.linear_model.logistic.LogisticRegression):
            p_df = pd.DataFrame(model.params)
            p_df.columns = ['index' , 'Params']
            p_df['AIC'] = model.faked_aic
            L.append(p_df)
        else:
            p_df = pd.DataFrame(model.params)
            p_df.columns = ['Params']
            p_df['AIC'] = model.aic
            L.append(p_df.reset_index())

    DF = pd.concat(L , axis = 0)
    DF['mid'] = DF.AIC * DF.Params
    Res = DF.groupby('index').agg(sum)
    Res['Params'] = Res.mid / Res.AIC
    return Res.reset_index().loc[:,[ 'index' , 'Params']]


# In[2]:


# -*- coding: UTF-8 -*-
from mta_funs import *
from sklearn.linear_model import LogisticRegression as LR
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report
import statsmodels.discrete.discrete_model as sm
from dask.distributed import Client, LocalCluster
from sklearn.metrics import f1_score
import dask.dataframe as ddf
import pandas as pd
import numpy as np
import operator
# import swifter
from psutil import cpu_count
from functools import partial
import time

#########################
# # User Defined Part # #
#########################

PATH = 'E:/JD_MTA/2019/2019-01-Johnson'
nmws_path = "./05_Model/non_media_with_seasonality_day.csv"
impd_path = "./05_Model/impression_digital.csv"
dims_path = "./05_Model/impression_digital_map.csv"

# Define Retention Days
ret_list = [7, 14, 21]

# Define
sign_dict = {'pos': ['lnppi','brand_pref_trip'], 'neg': ['lnbp']}

# Useless Columns
useless_cols = ['ahhldid_nbr', 'hh_dma', 'skuid', 'store_dma' , 'purch_qty_tgt' , 'sale_amt' ,
                 'age_1', 'FW', 'PurPower_1','net_extended_price_amt_tgt' ,'volume_tgt','july4th',
                 'pod_id','pseudo' ,'pct_feat','pct_disp','pct_fdisp','female','age_2','age_3',
                 'age_4','age_5','age_6','E','S','W','N','PurPower_2','PurPower_3','PurPower_4','PurPower_5']

# Useful Categorical Columns
cate_cols = []

# Define Columns Must Kept
keep_list = []

#########################
# # # Default Confs # # #
#########################
# total = 'total'
cpu_count = cpu_count()
dims_nm = ["X", "K", "D", "Y", "M"]
exposure = 'exposure'
ad_type = 'ad_type'
ID = 'id'
date = 'date'
target = 'target'
retention = 'retention'
pk_cols = [ID, date]

d_n = partial(d_n, cpu_count=cpu_count)
cal_exp = partial(cal_exp, N=ret_list)

#########################
# # # Data Reading  # # #
#########################

cd(PATH)

nmws = pd.read_csv(nmws_path)
impd = pd.read_csv(impd_path)
dims = pd.read_csv(dims_path)

nmws.date = pd.to_datetime(nmws.date)
impd.date = pd.to_datetime(impd.date)

nmws_id_date = nmws.loc[:, pk_cols].drop_duplicates()

tmp_list = [(impd[pk_cols + [col.lower(), exposure]]).rename(
    columns={col.lower(): ad_type}) for col in dims_nm]

impd_reshaped = pd.concat(tmp_list)

impd_event_dim = impd_reshaped.groupby(by=pk_cols + [ad_type]).aggregate({
    exposure: sum
})

impd_event_dim = impd_event_dim.reset_index()

t = time.time()
exp_pivoted = pd.merge(nmws_id_date, impd_event_dim, on=['id'])

# Build Local Dask Cluster
c = LocalCluster(n_workers=cpu_count , memory_limit= 16 * 1024**3)
client = Client(c)

exp_pivoted = ddf.from_pandas(exp_pivoted.sort_values([ID , ad_type]).reset_index(drop=True)
                              , npartitions = max( d_n(exp_pivoted), 128 ))
exp_pivoted_res = exp_pivoted.groupby([ID, ad_type]).apply(
    cal_exp, N=ret_list).compute()

client.cluster.close()

exp_pivoted_df = exp_pivoted_res.    reset_index().    drop(columns = 'level_2').    rename(columns={'index':date,0:retention,1:exposure})

##############################
# PYTHON PART
##############################

exp_wide_df = exp_pivoted_df.pivot_table(
    index=['id', date, retention],
    columns=[ad_type],
    values=[exposure],
    aggfunc='sum')

ad_cols = sorted(pd.unique(exp_pivoted_df.loc[:, ad_type]))
exp_wide_df.columns = ad_cols

exp_wide_df.reset_index(inplace = True)
Data = exp_wide_df.merge(nmws, how='right')


# In[30]:


if len(Data) <10**7 and np.mean(Data.target) >0.3 :
    is_sklearn = False
else:
    is_sklearn = True
    lr_d = {'class_weight':'balanced', 'n_jobs':-1}

# Selecting the best retention
aic_list = []
for ret in ret_list:
    sig = Data.loc[:, retention] == ret
    x = Data.loc[sig, ad_cols].fillna(0)
    y = Data.loc[sig, target]
    zero_cols = x.columns[x.sum() == 0]
    x.drop(columns = zero_cols , inplace = True)
    x['intercept'] = 1
    if is_sklearn == True:
        model = LR(**lr_d)
        model.fit(x , y)
        score = model.score(x,y)
        aic_list.append((ret, -score , model))
        print('Score of model is %s' %score)
    else:
        try:
            model = sm.Logit(y, x).fit()
        except:
            model = sm.Logit(y, x).fit(method='powell')
        aic_list.append((ret, model.aic, model))
        print('F1 Score is: ', f1_score(y, model.predict() >= 0.5))

ret, aic, model = min(aic_list, key=operator.itemgetter(1))
print(ret, aic)

# Slicing Based on Retention Period and Cleaning Columns
ret_Data_df = Data.loc[(Data.loc[:, retention] == ret) | Data.retention.isna(), :].fillna(0)

rm_cols = set(pk_cols + useless_cols).intersection(ret_Data_df.columns)
ret_Data_df.drop(columns = list(rm_cols) , inplace = True)

org_cols = list(set(ret_Data_df.columns).intersection(nmws.columns))
part1 = prepare_mat(ret_Data_df.loc[:,org_cols] , cate_cols = cate_cols)

# Prepare Partial Functions
backwards_stepwise = partial(backwards_stepwise, keep_list=keep_list)
sign_correction = partial(sign_correction, keep_list=keep_list, sign_d=sign_dict, ad_cols=ad_cols)

# Running model for each ad_type
y = ret_Data_df.loc[:, target]
sm_dict = {}

for tar in dims_nm:
    print('---\n\n%s  ===========' % tar)
    part2 = ret_Data_df.loc[:, get_cols(tar, ad_cols)]
    x = pd.concat([part1, part2], axis=1).drop(columns = target)
    x['intercept'] = 1
    if is_sklearn == True:
        model, remained_cols = sign_correction(LR(), x, y ,**lr_d)
        model.faked_aic = model.score(x.loc[:,remained_cols], y)
        y_pred = model.predict(x.loc[:, remained_cols])
        print(confusion_matrix(y, y_pred))
        print(classification_report(y, y_pred))
        print(model.faked_aic)
        print('F1 Score is: ', f1_score(y, y_pred))
        print(list(model.params))
    else:
        model, remained_cols, sig = backwards_stepwise(sm.Logit, x, y)
        model, remained_cols = sign_correction(sm.Logit, x.loc[:, remained_cols], y)
        if sig == 'Fail':
            model, remained_cols, sig = backwards_stepwise(
                sm.Logit, x.loc[:, remained_cols], y)
        print(model.pred_table())
        print('F1 Score is: ', f1_score(y, model.predict() >= 0.5))

    sm_dict[tar] = model

combine_params(sm_dict)
results = combine_params(sm_dict)
results.to_csv('E:/Users/Zuo/test_using_2019_01_johnson_balanced.csv')


# In[21]:


adstock = pd.read_csv("E:/JD_MTA/2019/2019-01-Johnson/05_Model/transformed_adstock.csv")


# In[20]:


np.mean(y_pred)


# In[4]:


lr_d = {'n_jobs':-1}

# Selecting the best retention
aic_list = []
for ret in ret_list:
    sig = Data.loc[:, retention] == ret
    x = Data.loc[sig, ad_cols].fillna(0)
    y = Data.loc[sig, target]
    zero_cols = x.columns[x.sum() == 0]
    x.drop(columns = zero_cols , inplace = True)
    x['intercept'] = 1
    if is_sklearn == True:
        model = LR(**lr_d)
        model.fit(x , y)
        score = model.score(x,y)
        aic_list.append((ret, -score , model))
        print('Score of model is %s' %score)
    else:
        try:
            model = sm.Logit(y, x).fit()
        except:
            model = sm.Logit(y, x).fit(method='powell')
        aic_list.append((ret, model.aic, model))
        print('F1 Score is: ', f1_score(y, model.predict() >= 0.5))

ret, aic, model = min(aic_list, key=operator.itemgetter(1))
print(ret, aic)

# Slicing Based on Retention Period and Cleaning Columns
ret_Data_df = Data.loc[(Data.loc[:, retention] == ret) | Data.retention.isna(), :].fillna(0)

rm_cols = set(pk_cols + useless_cols).intersection(ret_Data_df.columns)
ret_Data_df.drop(columns = list(rm_cols) , inplace = True)

org_cols = list(set(ret_Data_df.columns).intersection(nmws.columns))
part1 = prepare_mat(ret_Data_df.loc[:,org_cols] , cate_cols = cate_cols)

# Prepare Partial Functions
backwards_stepwise = partial(backwards_stepwise, keep_list=keep_list)
sign_correction = partial(sign_correction, keep_list=keep_list, sign_d=sign_dict, ad_cols=ad_cols)

# Running model for each ad_type
y = ret_Data_df.loc[:, target]
sm_dict = {}

for tar in dims_nm:
    print('---\n\n%s  ===========' % tar)
    part2 = ret_Data_df.loc[:, get_cols(tar, ad_cols)]
    x = pd.concat([part1, part2], axis=1).drop(columns = target)
    x['intercept'] = 1
    if is_sklearn == True:
        model, remained_cols = sign_correction(LR(), x, y ,**lr_d)
        model.faked_aic = model.score(x.loc[:,remained_cols], y)
        y_pred = model.predict(x.loc[:, remained_cols])
        print(confusion_matrix(y, y_pred))
        print(classification_report(y, y_pred))
        print(model.faked_aic)
        print('F1 Score is: ', f1_score(y, y_pred))
        print(list(model.params))
    else:
        model, remained_cols, sig = backwards_stepwise(sm.Logit, x, y)
        model, remained_cols = sign_correction(sm.Logit, x.loc[:, remained_cols], y)
        if sig == 'Fail':
            model, remained_cols, sig = backwards_stepwise(
                sm.Logit, x.loc[:, remained_cols], y)
        print(model.pred_table())
        print('F1 Score is: ', f1_score(y, model.predict() >= 0.5))

    sm_dict[tar] = model

combine_params(sm_dict)
results = combine_params(sm_dict)
results.to_csv('E:/Users/Zuo/test_using_2019_01_johnson_no_rebalancing.csv')


# In[22]:


python_adstock = Data.loc[(Data.loc[:, retention] == ret) | Data.retention.isna(), :].fillna(0)


# In[25]:


adstock = pd.read_csv("E:/JD_MTA/2019/2019-01-Johnson/05_Model/transformed_adstock.csv")
adstock = adstock.rename(columns = {'transformed_adstock.data_date':'transformed_adstock.data_d_date','transformed_adstock.data_id':'transformed_adstock.data_d_id'})
col = adstock.columns.str.replace('transformed_adstock.','')
col = col.str.replace('data_','')
col = col.str.replace('digital_','')
adstock.columns = col


# In[26]:


check_adstock = adstock.groupby(['target','date']).sum()


# In[27]:


check_adstock.columns


# In[28]:


check_adstock.to_csv("E:/Users/Zuo/check_adstock.csv")


# In[29]:


check_python_adstock = python_adstock.groupby(['target','date']).sum()
check_python_adstock.to_csv("E:/Users/Zuo/check_python_adstock.csv")


# In[88]:


check_adstock = pd.read_excel("E:/Users/Zuo/check_adstock.xlsx")
check_python_adstock = pd.read_excel("E:/Users/Zuo/check_python_adstock.xlsx")


# In[92]:


adstock_inputs = pd.read_csv("E:/JD_MTA/2019/2019-01-Johnson/05_Model/ad")


# In[5]:


#bma_output
L = []
for key , model in sm_dict.items():
    if isinstance(model, sklearn.linear_model.logistic.LogisticRegression):
        p_df = pd.DataFrame(model.params)
        p_df.columns = ['index' , 'Params']
        p_df['AIC'] = model.faked_aic
        L.append(p_df)
    else:
        p_df = pd.DataFrame(model.params)
        p_df.columns = ['Params']
        p_df['AIC'] = model.aic
        L.append(p_df.reset_index())

DF = pd.concat(L , axis = 0)
DF['mid'] = DF.AIC * DF.Params
Res = DF.groupby('index').agg(sum)
Res['Params'] = Res.mid / Res.AIC
Res.to_csv("E:/Users/Zuo/coef_with_aic.csv")


# In[15]:


bma=impd[['x','k','d','y','m']].drop_duplicates().reset_index(drop=True)


# In[7]:


Res = Res.drop(columns='mid')
Res['tag'] = Res.index


# In[ ]:


Res = pd.read_csv('E:/Users/Zuo/coef_with_aic.csv')


# In[17]:


dim = ['x','k','d','y','m']
for i in dim:
    bma = pd.merge(bma,Res,how='left',left_on=i,right_on='tag',sort=False)
    bma = bma.rename(columns={'Params':('Params_'+i),'AIC':('AIC'+i)})


# In[18]:


bma = bma.fillna(0)


# In[11]:


bma['bma_output_weight'] = (bma['Params_x']*bma['AICx']+
                            bma['Params_k']*bma['AICk']+
                            bma['Params_d']*bma['AICd']+
                            bma['Params_y']*bma['AICy']+
                            bma['Params_m']*bma['AICm'])/(bma['AICx']+bma['AICk']+bma['AICd']+bma['AICy']+bma['AICm'])


# In[12]:


bma['bma_output_key'] = bma['x'] + '_' + bma['k'] + '_'+bma['d']+ '_' +bma['y']+ '_'+bma['m']


# In[13]:


bma_output = bma[['bma_output_key','bma_output_weight']]


# In[14]:


bma_output.to_csv("E:/Users/Zuo/bma_output_python.csv")


# In[159]:


python_adstock.to_csv("E:/Users/Zuo/python_test/python_transformed_adstock.csv")


# In[150]:


DF.to_csv("E:/Users/Zuo/python_bma_models_final.csv")


# In[158]:


col = col.str.replace('D','digital_d')
col = col.str.replace('X','digital_x')
col = col.str.replace('K','digital_k')
col = col.str.replace('Y','digital_y')
col = col.str.replace('M','digital_m')
python_adstock.columns = col
python_adstock.columns


# In[166]:


col = python_adstock.columns
col = 'transformed_adstock.' + col
col = col.str.replace('D','digital_d')
col = col.str.replace('X','digital_x')
col = col.str.replace('K','digital_k')
col = col.str.replace('Y','digital_y')
col = col.str.replace('M','digital_m')
python_adstock.columns = col


# In[175]:


media = pd.read_csv("E:/JD_MTA/2019/2019-01-Johnson/06_Dashboard/project_27_table_1_media_overview_d68f82146cc589ab16fe61f50a7bc619.csv")
media = media.groupby(['cost_type','retrieval_type','loc','device_type_updated','position_type']).sum()


# In[176]:


media.to_csv("E:/Users/Zuo/media.csv")


# In[177]:


dims


# In[178]:


check = adstock.loc[adstock.target == 1]


# In[32]:


#bma_output_balanced
L = []
for key , model in sm_dict.items():
    if isinstance(model, sklearn.linear_model.logistic.LogisticRegression):
        p_df = pd.DataFrame(model.params)
        p_df.columns = ['index' , 'Params']
        p_df['AIC'] = model.faked_aic
        L.append(p_df)
    else:
        p_df = pd.DataFrame(model.params)
        p_df.columns = ['Params']
        p_df['AIC'] = model.aic
        L.append(p_df.reset_index())

DF = pd.concat(L , axis = 0)
DF['mid'] = DF.AIC * DF.Params
Res = DF.groupby('index').agg(sum)
Res['Params'] = Res.mid / Res.AIC
Res.to_csv("E:/Users/Zuo/balanced_coef_with_aic.csv")
bma=impd[['x','k','d','y','m']].drop_duplicates().reset_index(drop=True)
Res = Res.drop(columns='mid')
Res['tag'] = Res.index
dim = ['x','k','d','y','m']
for i in dim:
    bma = pd.merge(bma,Res,how='left',left_on=i,right_on='tag',sort=False)
    bma = bma.rename(columns={'Params':('Params_'+i),'AIC':('AIC'+i)})
bma = bma.fillna(0)
bma['bma_output_weight'] = (bma['Params_x']*bma['AICx']+
                            bma['Params_k']*bma['AICk']+
                            bma['Params_d']*bma['AICd']+
                            bma['Params_y']*bma['AICy']+
                            bma['Params_m']*bma['AICm'])/(bma['AICx']+bma['AICk']+bma['AICd']+bma['AICy']+bma['AICm'])
bma['bma_output_key'] = bma['x'] + '_' + bma['k'] + '_'+bma['d']+ '_' +bma['y']+ '_'+bma['m']
bma_output = bma[['bma_output_key','bma_output_weight']]
bma_output.to_csv("E:/Users/Zuo/balanced_bma_output_python.csv")


# In[37]:


grouped_adstock = adstock.groupby(['id','date']).sum().reset_index()
grouped_python_adstock = python_adstock.groupby(['id','date']).sum().reset_index()


# In[38]:


grouped_adstock = grouped_adstock.drop(columns={'ahhldid_nbr', 'pod_id', 'pseudo', 'store_dma', 'hh_dma',
       'purch_qty_tgt', 'volume_tgt', 'net_extended_price_amt_tgt', 'target',
       'd_id', 'pct_feat', 'pct_disp', 'pct_fdisp', 'lnppi',
       'lnbp', 'seasindex', 'skuid', 'sale_amt', 'exposure','paidsearch_exposure',
       'paidsearch_d001', 'paidsearch_k001', 'paidsearch_m001',
       'paidsearch_s001', 'paidsearch_s002', 'tv_exposure', 'tv_p001',
       'tv_c003'})


# In[40]:


grouped_python_adstock = grouped_python_adstock.drop(columns={'ahhldid_nbr', 'pod_id', 'pseudo',
       'store_dma', 'hh_dma', 'purch_qty_tgt', 'volume_tgt',
       'net_extended_price_amt_tgt', 'target', 'pct_feat', 'pct_disp',
       'pct_fdisp', 'lnppi', 'lnbp', 'seasindex', 'skuid',
       'sale_amt','retention'})


# In[46]:


grouped_adstock['date'] = pd.to_datetime(grouped_adstock['date'])
grouped_python_adstock['date'] = pd.to_datetime(grouped_python_adstock['date'])


# In[63]:


check = pd.merge(grouped_adstock, grouped_python_adstock, how='outer',on = ['id','date'],sort=False)


# In[64]:


check['D041'] = 0
check['D043'] = 0
check['D045'] = 0
check['D046'] = 0


# In[65]:


dic = ['d001', 'd002', 'd003', 'd004', 'd005',
       'd006', 'd007', 'd008', 'd009', 'd010', 'd011', 'd012', 'd013', 'd014',
       'd015', 'd016', 'd017', 'd018', 'd019', 'd020', 'd021', 'd022', 'd023',
       'd024', 'd025', 'd026', 'd027', 'd028', 'd029', 'd030', 'd031', 'd032',
       'd033', 'd034', 'd035', 'd036', 'd037', 'd038', 'd039', 'd040', 'd041',
       'd042', 'd043', 'd044', 'd045', 'd046', 'd047', 'k001', 'k002', 'm001',
       'm002', 'm003', 'm004', 'm005', 'x001', 'x002', 'y001', 'y002', 'y003',
       'y004']
for i in dic:
    check[i+'_diff'] = check[i] - check[i.upper()]
check['brand_diff'] = check['brand_pref_trip_x'] - check['brand_pref_trip_y']


# In[68]:


col = check.columns[check.columns.str.contains('diff')==True]


# In[71]:


diff = check.loc[:,col]


# In[86]:


diff.to_csv("E:/Users/Zuo/transformed_adstock_id_date_diff.csv")


# In[88]:


diff.max().max()


# In[89]:


diff.min().min()


# In[91]:


len(adstock[['id']].drop_duplicates())


# In[95]:


np.sum(adstock['sale_amt'])


# In[96]:


np.sum(adstock['sale_amt'][adstock['target']==1])

