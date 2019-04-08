# -*- coding: utf-8 -*-
"""
Created on Tue Mar 26 16:57:43 2019

@author: Zuo
"""

import pandas as pd
import numpy as np
import os
PATH = 'E:/JD_MTA/2019/2019-01-Johnson'
os.chdir(PATH)
dims_path = "./05_Model/impression_digital_map.csv"
dims = pd.read_csv(dims_path)
imp = pd.read_csv("E:/JD_MTA/2019/2019-01-Johnson/02_Cleaned_Data/imp_cleaned.csv")
imp_backup = imp.copy()
imp = imp_backup.copy()
clk = pd.read_csv("E:/JD_MTA/2019/2019-01-Johnson/02_Cleaned_Data/clk_cleaned.csv")
8
imp = imp.groupby(['user_log_acct_jm','loc','retrieval_type','cost_type','position_type','date','device_type_updated','is_brand_hit_key','competitor_hit_key','is_cate_hit_key','function_hit_key']).agg({'impress_time':'count','cost':'sum'}).rename(columns={'impress_time':'impression','cost':'cpm_cost'}).reset_index()
clk = clk.groupby(['user_log_acct_jm','loc','retrieval_type','cost_type','position_type','date','device_type_updated','is_brand_hit_key','competitor_hit_key','is_cate_hit_key','function_hit_key']).agg({'click_time':'count','cost':'sum'}).rename(columns={'click_time':'click','cost':'cpc_cost'}).reset_index()
media = pd.merge(imp,clk,how='left',on=['user_log_acct_jm','loc','retrieval_type','cost_type','position_type','date','device_type_updated','is_brand_hit_key','competitor_hit_key','is_cate_hit_key','function_hit_key'],sort=False)

media['d'] =media['loc'] + '_' + media['position_type']
media['y'] = 0
media['y'][(media['is_brand_hit_key']==0)&(media['competitor_hit_key']==0)] = 'Not_mention_brand'
media['y'][(media['is_brand_hit_key']==0)&(media['competitor_hit_key']==1)] = 'Mention_competitor'
media['y'][(media['is_brand_hit_key']==1)&(media['competitor_hit_key']==0)] = 'Mention_own_brand'
media['y'][media['retrieval_type']=='Display'] = 'Display'
media['m'] = 0
media['m'][(media['is_cate_hit_key']==0)&(media['function_hit_key']==0)] = 'No_cate_no_function'
media['m'][(media['is_cate_hit_key']==0)&(media['function_hit_key']==1)] = 'No_cate_mention_function'
media['m'][(media['is_cate_hit_key']==1)&(media['function_hit_key']==0)] = 'Mention_cate_no_function'
media['m'][(media['is_cate_hit_key']==1)&(media['function_hit_key']==1)] = 'Mention_cate_function'
media['m'][media['retrieval_type']=='Display'] = 'Display'

dims = dims[['label','id']]
dims = dims.rename(columns={'id':'x_dim'})
media = pd.merge(media,dims,how='left',left_on='device_type_updated',right_on='label')
dims = dims.rename(columns={'x_dim':'k_dim'})
media = pd.merge(media,dims,how='left',left_on='cost_type',right_on='label')
dims = dims.rename(columns={'k_dim':'d_dim'})
media = pd.merge(media,dims,how='left',left_on='d',right_on='label')
dims = dims.rename(columns={'d_dim':'y_dim'})
dims_y = dims.loc[51:54,:]
media = pd.merge(media,dims_y,how='left',left_on='y',right_on='label')
dims = dims.rename(columns={'y_dim':'m_dim'})
dims_m = dims.loc[55:59,:]
media = pd.merge(media,dims_m,how='left',left_on='m',right_on='label')
media['key'] = media['x_dim'] +'_'+ media['k_dim']+'_' + media['d_dim'] +'_'+ media['y_dim']+'_' +media['m_dim']
media['ttl_cost'] = media['cpm_cost'] + media['cpc_cost']
media_cost = media.groupby('key').sum().reset_index()
media_cost = media_cost[['key','impression','click','cpm_cost','cpc_cost','ttl_cost']]
media_cost.to_csv("E:/Users/Zuo/media_cost.csv")

# =============================================================================
# imp['d'] =imp['loc'] + '_' + imp['position_type']
# imp['y'] = 0
# imp['y'][(imp['is_brand_hit_key']==0)&(imp['competitor_hit_key']==0)] = 'Not_mention_brand'
# imp['y'][(imp['is_brand_hit_key']==0)&(imp['competitor_hit_key']==1)] = 'Mention_competitor'
# imp['y'][(imp['is_brand_hit_key']==1)&(imp['competitor_hit_key']==0)] = 'Mention_own_brand'
# imp['y'][imp['retrieval_type']=='Display'] = 'Display'
# imp['m'] = 0
# imp['m'][(imp['is_cate_hit_key']==0)&(imp['function_hit_key']==0)] = 'No_cate_no_function'
# imp['m'][(imp['is_cate_hit_key']==0)&(imp['function_hit_key']==1)] = 'No_cate_mention_function'
# imp['m'][(imp['is_cate_hit_key']==1)&(imp['function_hit_key']==0)] = 'Mention_cate_no_function'
# imp['m'][(imp['is_cate_hit_key']==1)&(imp['function_hit_key']==1)] = 'Mention_cate_function'
# imp['m'][imp['retrieval_type']=='Display'] = 'Display'
# 
# dims = dims[['label','id']]
# dims = dims.rename(columns={'id':'x_dim'})
# imp = pd.merge(imp,dims,how='left',left_on='device_type_updated',right_on='label')
# dims = dims.rename(columns={'x_dim':'k_dim'})
# imp = pd.merge(imp,dims,how='left',left_on='cost_type',right_on='label')
# dims = dims.rename(columns={'k_dim':'d_dim'})
# imp = pd.merge(imp,dims,how='left',left_on='d',right_on='label')
# dims = dims.rename(columns={'d_dim':'y_dim'})
# dims_y = dims.loc[51:54,:]
# imp = pd.merge(imp,dims_y,how='left',left_on='y',right_on='label')
# dims = dims.rename(columns={'y_dim':'m_dim'})
# dims_m = dims.loc[55:59,:]
# imp = pd.merge(imp,dims_m,how='left',left_on='m',right_on='label')
# 
# imp.columns
# 
# imp['key'] = imp['x_dim'] +'_'+ imp['k_dim']+'_' + imp['d_dim'] +'_'+ imp['y_dim']+'_' +imp['m_dim']
# imp.head()
# clk = pd.read_csv("E:/JD_MTA/2019/2019-01-Johnson/02_Cleaned_Data/clk_cleaned.csv")
# clk['d'] =clk['loc'] + '_' + clk['position_type']
# clk['y'] = 0
# clk['y'][(clk['is_brand_hit_key']==0)&(clk['competitor_hit_key']==0)] = 'Not_mention_brand'
# clk['y'][(clk['is_brand_hit_key']==0)&(clk['competitor_hit_key']==1)] = 'Mention_competitor'
# clk['y'][(clk['is_brand_hit_key']==1)&(clk['competitor_hit_key']==0)] = 'Mention_own_brand'
# clk['y'][clk['retrieval_type']=='Display'] = 'Display'
# clk['m'] = 0
# clk['m'][(clk['is_cate_hit_key']==0)&(clk['function_hit_key']==0)] = 'No_cate_no_function'
# clk['m'][(clk['is_cate_hit_key']==0)&(clk['function_hit_key']==1)] = 'No_cate_mention_function'
# clk['m'][(clk['is_cate_hit_key']==1)&(clk['function_hit_key']==0)] = 'Mention_cate_no_function'
# clk['m'][(clk['is_cate_hit_key']==1)&(clk['function_hit_key']==1)] = 'Mention_cate_function'
# clk['m'][clk['retrieval_type']=='Display'] = 'Display'
# 
# dims = dims.rename(columns={'m_dim':'x_dim'})
# clk = pd.merge(clk,dims,how='left',left_on='device_type_updated',right_on='label')
# dims = dims.rename(columns={'x_dim':'k_dim'})
# clk = pd.merge(clk,dims,how='left',left_on='cost_type',right_on='label')
# dims = dims.rename(columns={'k_dim':'d_dim'})
# clk = pd.merge(clk,dims,how='left',left_on='d',right_on='label')
# dims = dims.rename(columns={'d_dim':'y_dim'})
# dims_y = dims.loc[51:54,:]
# clk = pd.merge(clk,dims_y,how='left',left_on='y',right_on='label')
# dims = dims.rename(columns={'y_dim':'m_dim'})
# dims_m = dims.loc[55:59,:]
# clk = pd.merge(clk,dims_m,how='left',left_on='m',right_on='label')
# clk['key'] = clk['x_dim'] +'_'+ clk['k_dim']+'_' + clk['d_dim'] +'_'+ clk['y_dim']+'_' +clk['m_dim']
# test = clk.loc[1:10,:]
# imp_cost = imp.groupby("key",sort=False).agg({'impress_time': 'count','cost':'sum'}).rename(columns={'impress_time':'impression'}).reset_index()
# clk_cost = clk.groupby("key",sort=False).agg({'click_time': 'count','cost':'sum'}).rename(columns={'click_time':'click'}).reset_index()
# cost = pd.merge(imp_cost,clk_cost,how='outer',on='key')
# cost = cost.rename(columns={'cost_x':'imp_cost','cost_y':'clk_cost'})
# cost['ttl_cost'] = cost['imp_cost'] + cost['clk_cost']
# cost.to_csv("E:/Users/Zuo/cost.csv")
# clk.columns
# test = clk[clk['position_type'] == 'APP专属推荐']
# grouped_test = test.groupby('date').agg({'click_time':'count'}).reset_index()
# 
# 
# =============================================================================
