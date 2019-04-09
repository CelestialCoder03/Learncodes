# -*- coding: utf-8 -*-
"""
Created on Tue Feb 12 10:26:11 2019

@author: Zuo
"""
#coding = utf-8
import pandas as pd
import numpy as np
import os
from IPython.display import display
import locale
import chardet
import re
import datetime
import itertools
os.getcwd()
os.chdir("E:/Users/Zuo/Test")

NA_Strings = ["NA","Na","na","NULL","Null","null",""]

ContrastWindow = ["2017-05-01","2018-04-30"]
ContrastWindow = pd.to_datetime(ContrastWindow)
CampaignWindow = ["2018-05-01","2018-07-31"]
CampaignWindow = pd.to_datetime(CampaignWindow)

locale.setlocale(locale.LC_ALL,'zh_CN')

# 设定模型(non_media_with_seasonality)的颗粒度。可选值为'day'与'second'。默认为"day" 。
NMWS_Granularity = "day"
pd.set_option('display_width',200)

# 设定文件路径
clk_raw_path = "E:/JD_MTA/2018/2018-09-Nestle/01_Raw_Data/nestle_click_20180910"
ord_raw_path = "E:/JD_MTA/2018/2018-09-Nestle/01_Raw_Data/nestle_order_20180910"
usr_raw_path = "E:/JD_MTA/2018/2018-09-Nestle/01_Raw_Data/nestle_user_20180910"
imp_raw_path = "E:/JD_MTA/2018/2018-09-Nestle/01_Raw_Data/nestle_impression_20180910"
sku_raw_path = "E:/JD_MTA/2018/2018-09-Nestle/01_Raw_Data/nestle_sku_20180910.xlsx"

#sku_cleaned_xlsx_path = "./01_Raw_Data/sku_with_imp_clk_sales_updated_-_FOR_CODE_RESULT_ONLY.xlsx" # Data Review时使用的清理完毕之后的SKU
sku_cleaned_xlsx_path = "E:/JD_MTA/2018/2018-09-Nestle/01_Raw_Data/sku_with_imp_clk_sales_updated_-_FOR_CODE_RESULT_ONLY-import_updated.xlsx"# Data Review之后修正import标签之后的SKU

# hit key map for impresssion and click
# 注意：JD是在我们提供的keywor打标规则的基础上打标签。并直接将hit_key_jm与4四个标签直接输出。
#       因此在连接hit key map与impression与click时需要先去重再合并

imp_hkm_path = "E:/JD_MTA/2018/2018-09-Nestle/01_Raw_Data/hit_key_map/nestle_impression_20180912_4_n"
clk_hkm_path = "E:/JD_MTA/2018/2018-09-Nestle/01_Raw_Data/hit_key_map/nestle_click_20180912_4_n"


os.makedirs("E:/Users/Zuo/Test/01_Raw_Data",exist_ok=True)
os.makedirs("E:/Users/Zuo/Test/02_Cleaned_Data",exist_ok=True)
os.makedirs("E:/Users/Zuo/Test/03_Data_Review",exist_ok=True)
os.makedirs("E:/Users/Zuo/Test/04_Descriptive",exist_ok=True)
os.makedirs("E:/Users/Zuo/Test/05_Model",exist_ok=True)

#import data
clk = pd.read_csv(clk_raw_path,sep="\t",encoding="utf-8",na_values=NA_Strings,keep_default_na=False)
ord = pd.read_csv(ord_raw_path,sep="\t",encoding="utf-8",na_values=NA_Strings,keep_default_na=False)
usr = pd.read_csv(usr_raw_path,sep="\t",encoding="utf-8",na_values=NA_Strings,keep_default_na=False)
imp = pd.read_csv(imp_raw_path,sep="\t",encoding="utf-8",na_values=NA_Strings,keep_default_na=False)
#NULL from SKU not read as NA
sku = pd.read_excel(sku_raw_path,encoding="utf-8",na_values=["na","","-1.#IND","1.#QNAN","1.#IND","-1.#QNAN","#N/A N/A","#N/A","N/A","NA","#NA","NaN","-NaN","nan","-nan"],keep_default_na=False)

                                                            
imp_hkm = pd.read_csv(imp_hkm_path,sep="\t",encoding="utf-8",na_values=NA_Strings,keep_default_na=False)
imp_hkm = imp_hkm.drop_duplicates()

imp_uni_hit_key = imp[["hit_key_jm"]].drop_duplicates().dropna().sort_values("hit_key_jm")
imp_uni_hit_key = imp_uni_hit_key.reset_index(drop = True)
imp_hkm_uni_hit_key = imp_hkm[["hit_key_jm"]].drop_duplicates().dropna().sort_values("hit_key_jm")
imp_hkm_uni_hit_key = imp_hkm_uni_hit_key.reset_index(drop = True)

imp_uni_hit_key.equals(imp_hkm_uni_hit_key)

#change NA into 0
imp_hkm = imp_hkm.reset_index(drop = True).fillna({"hit_key_ownbrand":0,"hit_key_competitor":0,"hit_key_category":0,"hit_key_function":0})
imp = pd.merge(imp,imp_hkm,how = "left",on = "hit_key_jm",sort=False)
imp.hit_key_ownbrand[imp.retrieval_type == 1] = np.nan
imp.hit_key_competitor[imp.retrieval_type == 1] = np.nan
imp.hit_key_category[imp.retrieval_type == 1] = np.nan
imp.hit_key_function[imp.retrieval_type == 1] = np.nan
imp.to_csv(path_or_buf = "E:/Users/Zuo/Test/nestle_impression_20180910with_hit_key.csv", sep='\t', encoding="utf-8",index = False)

clk_hkm = pd.read_csv(clk_hkm_path,sep="\t",encoding = "utf-8",na_values = NA_Strings, keep_default_na = False)
clk_hkm = clk_hkm.drop_duplicates()

clk_uni_hit_key = clk[["hit_key_jm"]].drop_duplicates().dropna().sort_values("hit_key_jm")
clk_uni_hit_key = clk_uni_hit_key.reset_index(drop = True)
clk_hkm_uni_hit_key = clk_hkm[["hit_key_jm"]].drop_duplicates().dropna().sort_values("hit_key_jm")
clk_hkm_uni_hit_key = clk_hkm_uni_hit_key.reset_index(drop=True)

clk_uni_hit_key.equals(clk_hkm_uni_hit_key)

clk_hkm = clk_hkm.reset_index(drop = True).fillna({"hit_key_ownbrand":0,"hit_key_competitor":0,"hit_key_category":0,"hit_key_function":0})
clk = pd.merge(clk,clk_hkm,how = "left",on = "hit_key_jm",sort = False)
clk.hit_key_ownbrand[clk.retrieval_type == 1] = np.nan
clk.hit_key_competitor[clk.retrieval_type == 1] = np.nan
clk.hit_key_category[clk.retrieval_type == 1] = np.nan
clk.hit_key_function[clk.retrieval_type == 1] = np.nan
clk.to_csv(path_or_buf = "E:/Users/Zuo/Test/nestle_click_20180910with_hit_key.csv", sep='\t', encoding="utf-8",index = False)

sku["wt"] = pd.to_numeric(sku["wt"])

# STEP 2 DATA QC
clk_chk_na = clk.isna().sum()
order_chk_na = order.isna().sum()
user_chk_na = user.isna().sum()
imp_chk_na = imp.isna().sum()
sku_chk_na = sku.isna().sum()

usr_usr = usr[["user_log_acct_jm"]].drop_duplicates().dropna().reset_index(drop = True)
sku_sku = sku[["item_sku_id_jm"]].drop_duplicates().dropna().reset_index(drop = True)
ord_usr = ord[["user_log_acct_jm"]].drop_duplicates().dropna().reset_index(drop = True)
ord_sku = ord[["item_sku_id_jm"]].drop_duplicates().dropna().reset_index(drop = True)
imp_usr = imp[["user_log_acct_jm"]].drop_duplicates().dropna().reset_index(drop = True)
imp_sku = imp[["item_sku_id_jm"]].drop_duplicates().dropna().reset_index(drop = True)
clk_usr = clk[["user_log_acct_jm"]].drop_duplicates().dropna().reset_index(drop = True)
clk_sku = clk[["item_sku_id_jm"]].drop_duplicates().dropna().reset_index(drop = True)


#crosscheck files
cross_check(usr_usr,sku_sku,ord_usr,ord_sku,imp_usr,imp_sku,clk_usr,clk_sku)


#STEP 3 DATA CLEAN

#clean SKU files
#   1. Please use [01_Raw_Data/sku_w_sales.csv] to create a final sku list (under [Include] column, 1=keep, 0=drop);
#   2. Please update which SKUs are client's SKUs under [if_client] column
#   3. Please update sub-category information udner [sub_cate] column if needed
#   4. After updated, please rename the file as [sku_w_sales_updated.csv]
clean_sku(sku,ord)

# EXTRA SUMMARIZING BY SKU ID FOR SKU CLEANING
# re-read sku table
sku = pd.read_excel(sku_raw_path,sheet_name =0,encoding="utf-8",na_values=["na","","-1.#IND","1.#QNAN","1.#IND","-1.#QNAN","#N/A N/A","#N/A","N/A","NA","#NA","NaN","-NaN","nan","-nan"],keep_default_na=False)
sku["wt"] = pd.to_numeric(sku["wt"])

# aggregate impression, click, and order by sku id 
imp__sid = imp[["item_sku_id_jm","cost","user_log_acct_jm"]]
imp__sid = imp__sid.groupby("item_sku_id_jm",sort=False).agg({'item_sku_id_jm':'count','cost':'sum','user_log_acct_jm':'nunique'}).rename(columns={'cost':'imp_cost','item_sku_id_jm':'imp_vol','user_log_acct_jm':'unique_imp_audience'}).reset_index()

clk__sid = clk[["item_sku_id_jm","cost","user_log_acct_jm"]]
clk__sid = clk__sid.groupby("item_sku_id_jm",sort=False).agg({'item_sku_id_jm':'count','cost':'sum','user_log_acct_jm':'nunique'}).rename(columns={'cost':'clk_cost','item_sku_id_jm':'clk_vol','user_log_acct_jm':'unique_clk_audience'}).reset_index()

ord__sid = ord[["item_sku_id_jm","sale_qtty","before_prefr_amount","after_prefr_amount","user_log_acct_jm"]]
ord__sid = ord__sid.groupby("item_sku_id_jm",sort=False).agg({'item_sku_id_jm':'count','sale_qtty':'sum','before_prefr_amount':'sum','after_prefr_amount':'sum','user_log_acct_jm':'nunique'}).rename(columns={'item_sku_id_jm':'order_count','user_log_acct_jm':'unique_buyer'}).reset_index()

# tag the sku id with impression, click, or order
sku["in_imp"] = sku["item_sku_id_jm"].isin(imp__sid["item_sku_id_jm"])
sku["in_clk"] = sku["item_sku_id_jm"].isin(clk__sid["item_sku_id_jm"])
sku["in_ord"] = sku["item_sku_id_jm"].isin(ord__sid["item_sku_id_jm"])
sku["in_imp_clk_ord"] = sku.apply(lambda x:x['in_imp']+x['in_clk']+x['in_ord'],axis=1)>0

# join the aggregate result together
sku = pd.merge(sku,imp__sid,how = "left",on = "item_sku_id_jm",sort=False)
sku = pd.merge(sku,clk__sid,how = "left",on = "item_sku_id_jm",sort=False)
sku = pd.merge(sku,ord__sid,how = "left",on = "item_sku_id_jm",sort=False)

# arrange column
sku = sku[list(dict.fromkeys(ord__sid.columns.tolist()+imp__sid.columns.tolist()+clk__sid.columns.tolist()+sku.columns.tolist()))]

# add columns for convenience
sku["brand_name_len"] = sku["brand_name_jm"].str.len()
sku["Include"] = np.nan
sku["if_client"] = np.nan
sku["sub_cate"] =np.nan
sku[sku["in_imp_clk_ord"] == True].to_csv(path_or_buf = "E:/Users/Zuo/Test/sku_with_imp_click_sales.csv", sep=',', encoding="utf-8",index = False)
sku[sku["in_imp_clk_ord"] == False].to_csv(path_or_buf = "E:/Users/Zuo/Test/sku_without_imp_click_sales.csv", sep=',', encoding="utf-8",index = False)

del(imp__sid,clk__sid,ord__sid,sku)

# re-read sku table
sku = pd.read_excel(sku_cleaned_xlsx_path,sheet_name=1,encoding="utf-8",na_values=["na","","-1.#IND","1.#QNAN","1.#IND","-1.#QNAN","#N/A N/A","#N/A","N/A","NA","#NA","NaN","-NaN","nan","-nan"],keep_default_na=False)
sku["wt"] = pd.to_numeric(sku["wt"])
sku["Include"] = pd.to_numeric(sku["Include"])

#ACTION 5: Clean order file (remove abnormal sales)
if_remove_ord_dup = True
if_remove_ord_free = True
clean_order_check(ord, sku, if_remove_ord_dup, if_remove_ord_free)

#NOTE:
#   1. Please use [02_Cleaned_Data/order_abnormal_qtty.csv] to decide an upper threshold for abnormal sales quantity
#   2. Please use [02_Cleaned_Data/order_sku_abnormal_price.csv] to decide a lower and an upper threshold for prcie/unit
#   3. Please use [02_Cleaned_Data/order_trans_abnormal.csv] to decide an upper threshold for daily individual transaction number
sku = pd.read_excel(sku_cleaned_xlsx_path,sheet_name=1,encoding="utf-8",na_values=["na","","-1.#IND","1.#QNAN","1.#IND","-1.#QNAN","#N/A N/A","#N/A","N/A","NA","#NA","NaN","-NaN","nan","-nan"],keep_default_na=False)
sku["wt"] = pd.to_numeric(sku["wt"])
sku["Include"] = pd.to_numeric(sku["Include"])
sku = sku[sku["Include"]==1]
ord = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/ord_in_scope.csv",encoding="UTF-8")
auto_select = False

trans_abnormal_upper_threshold = 6  
abnormal_price_upper_threshold = 1.35
abnormal_price_lower_threshold = 0.65
abnormal_qtty_upper_threshold = 6

clean_order(ord,sku,auto_select,abnormal_qtty_upper_threshold=6,
            abnormal_price_lower_threshold=0.65,
            abnormal_price_upper_threshold=1.35,
            trans_abnormal_upper_threshold=6)
del(ord)
#ACTION 6: Clean imp and click file
clk = pd.read_csv("E:/Users/Zuo/Test/nestle_click_20180910with_hit_key.csv",encoding="utf-8")
sku = pd.read_excel(sku_cleaned_xlsx_path,sheet_name=1,encoding="utf-8",na_values=["na","","-1.#IND","1.#QNAN","1.#IND","-1.#QNAN","#N/A N/A","#N/A","N/A","NA","#NA","NaN","-NaN","nan","-nan"],keep_default_na=False)
sku["Include"] = pd.to_numeric(sku["Include"])
sku = sku[(sku["Include"]==1)&(sku["if_client"]==1)]

# 删除项目范畴之外的曝光与点击记录
imp["dt"] = pd.to_datetime(imp["impress_time"]).dt.date
clk["dt"] = pd.to_datetime(clk["click_time"]).dt.date
imp = imp[imp["dt"] <= max(CampaignWindow)]
imp = imp[imp["dt"] >= min(CampaignWindow)]
clk = clk[clk["dt"] <= (max(CampaignWindow).date())]
clk = clk[clk["dt"] >= (min(CampaignWindow).date())]
imp = imp.drop(columns="dt")
clk = clk.drop(columns="dt)

imp = imp.rename(columns = {'terminal_type':'device_type','impress_time':'imp_time'} )
clk = clk.rename(columns = {'terminal_type':'device_type'})

clean_imp_click(imp,clk,sku)
del(imp,clk,sku)

imp = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/imp_cleaned.csv",sep=",")
clk = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/clk_cleaned.csv",sep=",")

imp["hit_key_ownbrand"][imp["hit_key_ownbrand"].isna()==True] = -1
imp["hit_key_ownbrand"][imp["hit_key_ownbrand"]==-1] = "Display_Ad"
imp["hit_key_ownbrand"][imp["hit_key_ownbrand"]==0] = "Not_Mentioned"
imp["hit_key_ownbrand"][imp["hit_key_ownbrand"]==1] = "Mentioned_Ownbrand"

imp["hit_key_competitor"][imp["hit_key_competitor"].isna()==True] = -1
imp["hit_key_competitor"][imp["hit_key_competitor"]==-1] = "Display_Ad"
imp["hit_key_competitor"][imp["hit_key_competitor"]==0] = "Not_Mentioned"
imp["hit_key_competitor"][imp["hit_key_competitor"]==1] = "Mentioned_Ownbrand"

imp["hit_key_category"][imp["hit_key_category"].isna()==True] = -1
imp["hit_key_category"][imp["hit_key_category"]==-1] = "Display_Ad"
imp["hit_key_category"][imp["hit_key_category"]==0] = "Not_Mentioned"
imp["hit_key_category"][imp["hit_key_category"]==1] = "Mentioned_Category"

imp["hit_key_function"][imp["hit_key_function"].isna()==True] = -1
imp["hit_key_function"][imp["hit_key_function"]==-1] = "Display_Ad"
imp["hit_key_function"][imp["hit_key_function"]==0] = "Not_Mentioned"
imp["hit_key_function"][imp["hit_key_function"]==1] = "Mentioned_Special_Formula"

clk["hit_key_ownbrand"][clk["hit_key_ownbrand"].isna()==True] = -1
clk["hit_key_ownbrand"][clk["hit_key_ownbrand"]==-1] = "Display_Ad"
clk["hit_key_ownbrand"][clk["hit_key_ownbrand"]==0] = "Not_Mentioned"
clk["hit_key_ownbrand"][clk["hit_key_ownbrand"]==1] = "Mentioned_Ownbrand"

clk["hit_key_competitor"][clk["hit_key_competitor"].isna()==True] = -1
clk["hit_key_competitor"][clk["hit_key_competitor"]==-1] = "Display_Ad"
clk["hit_key_competitor"][clk["hit_key_competitor"]==0] = "Not_Mentioned"
clk["hit_key_competitor"][clk["hit_key_competitor"]==1] = "Mentioned_Ownbrand"

clk["hit_key_category"][clk["hit_key_category"].isna()==True] = -1
clk["hit_key_category"][clk["hit_key_category"]==-1] = "Display_Ad"
clk["hit_key_category"][clk["hit_key_category"]==0] = "Not_Mentioned"
clk["hit_key_category"][clk["hit_key_category"]==1] = "Mentioned_Category"

clk["hit_key_function"][clk["hit_key_function"].isna()==True] = -1
clk["hit_key_function"][clk["hit_key_function"]==-1] = "Display_Ad"
clk["hit_key_function"][clk["hit_key_function"]==0] = "Not_Mentioned"
clk["hit_key_function"][clk["hit_key_function"]==1] = "Mentioned_Special_Formula"

# map extra brand info from SKU
# 因为Nestle的曝光包含MAGA_NAN之外的SKU，所以此处加以标记。
# 后期建模时仅仅选用和target一致的曝光。
imp = pd.merge(imp,sku[["item_sku_id_jm","Include","if_client","brand_family","sub_brand","target","import_final"]],how="inner",on="item_sku_id_jm",sort=False)
clk = pd.merge(clk,sku[["item_sku_id_jm","Include","if_client","brand_family","sub_brand","target","import_final"]],how="inner",on="item_sku_id_jm",sort=False)

# rename original cleaned imp and clk
os.rename('E:/Users/Zuo/Test/02_Cleaned_Data/imp_cleaned.csv','E:Users.Zuo/Test/02_Cleaned_Data/imp_cleaned_but_hit_key_unlabelled.csv")
os.rename('E:/Users/Zuo/Test/02_Cleaned_Data/clk_cleaned.csv','E:Users.Zuo/Test/02_Cleaned_Data/clk_cleaned_but_hit_key_unlabelled.csv")

# overwrite the cleaned imp and clk
imp.to_csv(path_or_buf="E:Users/Zuo/Test/02_Cleaned_Data/imp_cleaned.csv",sep=",")
clk.to_csv(path_or_buf="E:Users/Zuo/Test/02_Cleaned_Data/clk_cleaned.csv",sep=",")

#ACTION 7: Clean user file (CUSTOMIZED)
#usr <-
#    as.data.frame(fread('./01_Raw_Data/user_updated.csv', encoding = 'UTF-8'))
usr = pd.read_csv(usr_raw_path,sep="\t",encoding="utf-8",na_values=NA_Strings,keep_default_na=False)
# cleaning usr - relabel
usr["cpp_addr_province"][usr["cpp_addr_province"] == -1] ="Unknown"
usr["cpp_addr_city"][usr["cpp_addr_city"] == -1] ="Unknown"
usr["cpp_addr_sex"][usr["cpp_addr_sex"] == -1] ="Unknown"
usr["cpp_addr_age"][usr["cpp_addr_age"] == -1] ="Unknown"
usr["cgp_cust_purchpower"][usr["cgp_cust_purchpower"] == -1] ="Unknown"
usr["cgp_cust_purchpower"][usr["cgp_cust_purchpower"] == 1] ="Wealthy"
usr["cgp_cust_purchpower"][usr["cgp_cust_purchpower"] == 2] ="Premium White Collar"
usr["cgp_cust_purchpower"][usr["cgp_cust_purchpower"] == 3] ="Junior_White_Collar"
usr["cgp_cust_purchpower"][usr["cgp_cust_purchpower"] == 4] ="Blue_Collar"
usr["cgp_cust_purchpower"][usr["cgp_cust_purchpower"] == 5] ="Limited_Income"
usr["cfv_sens_promotion"][usr["cfv_sens_promotion"] == -1] ="Unknown"
usr["jd_repeat_buyer"] = "Unknown"
usr["jd_repeat_buyer"][usr["cfv_sens_promotion"].str.contains("L1")] = "近一年有复购用户"
usr["jd_repeat_buyer"][usr["cfv_sens_promotion"].str.contains("L2")] = "近一年无复购用户"
usr["jd_repeat_buyer"][usr["cfv_sens_promotion"].str.contains("L3")] = "一年前有复购用户"
usr["jd_repeat_buyer"][usr["cfv_sens_promotion"].str.contains("L4")] = "一年前无复购用户"
usr["jd_price_sensitive"] = "Unknown"
usr["jd_price_sensitive"][usr["cfv_sens_promotion"].str.contains("-1")] = "不敏感"
usr["jd_price_sensitive"][usr["cfv_sens_promotion"].str.contains("-2")] = "轻度敏感"
usr["jd_price_sensitive"][usr["cfv_sens_promotion"].str.contains("-3")] = "中度敏感"
usr["jd_price_sensitive"][usr["cfv_sens_promotion"].str.contains("-4")] = "高度敏感"
usr["jd_price_sensitive"][usr["cfv_sens_promotion"].str.contains("-5")] = "极度敏感"
usr["cgp_action_grpcate"] = re.sub("(,[ ]*!.*)$", "", usr["cgp_action_grpcate"])
usr["cgp_action_grpcate"][usr["cgp_action_grpcate"] == -1] ="Unknown"

usr["csf_medal_mombaby"][usr["csf_medal_mombaby"] == -1] ="Unkown"
usr["csf_medal_mombaby"][usr["csf_medal_mombaby"] == "V0"] ="Other"
usr["csf_medal_mombaby"][usr["csf_medal_mombaby"] == "V1"] ="(0,500)"
usr["csf_medal_mombaby"][usr["csf_medal_mombaby"] == "V2"] ="(500,2000)"
usr["csf_medal_mombaby"][usr["csf_medal_mombaby"] == "V3"] ="(2000,5000)"
usr["csf_medal_mombaby"][usr["csf_medal_mombaby"] == "V4"] ="(5000,99999)"

usr["csf_medal_beauty"][usr["csf_medal_beauty"] == -1] ="Unkown"
usr["csf_medal_beauty"][usr["csf_medal_beauty"] == "V0"] ="Other"
usr["csf_medal_beauty"][usr["csf_medal_beauty"] == "V1"] ="(0,500)"
usr["csf_medal_beauty"][usr["csf_medal_beauty"] == "V2"] ="(500,2000)"
usr["csf_medal_beauty"][usr["csf_medal_beauty"] == "V3"] ="(2000,5000)"
usr["csf_medal_beauty"][usr["csf_medal_beauty"] == "V4"] ="(5000,99999)"

usr["csf_medal_wine"][usr["csf_medal_wine"] == -1] ="Unkown"
usr["csf_medal_wine"][usr["csf_medal_wine"] == "V0"] ="Other"
usr["csf_medal_wine"][usr["csf_medal_wine"] == "V1"] ="(0,699)"
usr["csf_medal_wine"][usr["csf_medal_wine"] == "V2"] ="(699,1699)"
usr["csf_medal_wine"][usr["csf_medal_wine"] == "V3"] ="(1699,5699)"
usr["csf_medal_wine"][usr["csf_medal_wine"] == "V4"] ="(5699,1000000000)"

city_province = usr[["cpp_addr_province","cpp_addr_city"]].drop_duplicates()
city_province.to_csv(path_or_but="E:/Users/Zuo/Test/02_Cleaned_Data/city_province.csv",sep=",")
# 将之前的city_province_updated.csv改为通用的city tierbiao标签
city_nsln_n_jd_tag_path = "E:/JD_MTA/2018/2018-09-Nestle/City_Tier_Map_Modified_based_on_XuXu_-_180820_city_map_for_user.csv.xlsx"
city_nsln_n_jd_tag_for_usr = pd.read_excel(city_nsln_n_jd_tag_path,sheet_name='fromXuXu-forUser-Mod-Latest')

usr = pd.merge(usr,city_nsln_n_jd_tag_for_usr,how="inner",on=["cpp_addr_city","cpp_addr_province"],sort=False)

usr_map = usr[["user_log_acct_jm"]]
usr_map["user_id"] = usr_map.index +1
usr_map.to_csv(path_or_buf = "E:/Users/Zuo/Test/02_Cleaned_Data/user_id_mapping.csv",sep=",")
usr = pd.merge(usr,usr_map,how="inner",on="user_log_acct_jm",sort=False)
usr.to_csv(path_or_buf = "E:/Users/Zuo/Test/02_Cleaned_Data/user_cleaned.csv",sep=",")

# 生成sku map
sku = pd.read_excel(sku_cleaned_xlsx_path,sheet_name=1,encoding="utf-8",na_values=["na","","-1.#IND","1.#QNAN","1.#IND","-1.#QNAN","#N/A N/A","#N/A","N/A","NA","#NA","NaN","-NaN","nan","-nan"],keep_default_na=False)
sku["Include"] = pd.to_numeric(sku["Include"])
sku = sku[(sku["Include"]==1)&(sku["if_client"]==1)]
sku_mapping = sku[["item_sku_id_jm"]].drop_duplicates().sort_values("item_sku_id_jm")
sku_mapping["skuid"]=sku_mapping.index +1
sku_mapping.to_csv(path_or_buf = "E:/Users/Zuo/Test/02_Cleaned_Data/sku_id_mapping.csv",sep=",")

#ACTION 8: Calculate brand preference and update user file
#   1. Calculated by purchase frequency (1 yr before exposure period)
ord = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/order_cleaned.csv")
ord_brand_pref = pd.merge(ord,sku[["item_sku_id_jm","brand_family","target"],how="left",on="item_sku_id_jm",sort=False])
ord_brand_pref["brand_name"][ord_brand_pref["brand_family"]=="MAGA NAN"] = ord_brand_pref["brand_family"][ord_brand_pref["brand_family"]=="MAGA NAN"] 
## 因为Nestle的曝光包含MAGA_NAN之外的SKU，所以此处加以修改
brand_pref(ord  = ord_brand_pref, brand = ["MAGA_NAN"]))
del(ord_brand_pref)

#ACTION 9: Shopper Tag (new, repeat, lost) (OPTIONAL)
usr = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/user_cleaned.csv")
period = min(CampaignWindow)
usr = tagging_new(ord, usr, 'category', period)
usr = tagging_new(ord, usr, 'MOONY', period)
usr.to_csv(path_or_buf = "E:/Users/Zuo/Test/usr_with_tag.csv",sep=",")

#STEP 4 - MODEL INPUT
usr = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/user_cleaned.csv")
ord = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/order_cleaned.csv")
sku = pd.read_excel(sku_cleaned_xlsx_path,sheet_name=1,encoding="utf-8",na_values=["na","","-1.#IND","1.#QNAN","1.#IND","-1.#QNAN","#N/A N/A","#N/A","N/A","NA","#NA","NaN","-NaN","nan","-nan"],keep_default_na=False)
sku["Include"] = pd.to_numeric(sku["Include"])
sku = sku[sku["Include"]==1]
imp = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/imp_cleaned.csv")
clk = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/clk_cleaned.csv")

imp = imp[imp["target"]==1]
clk = clk[clk["target"]==1]

ord = pd.merge(ord, sku[["item_sku_id_jm","brand_family","sub_brand","target"]],how="left",on="item_sku_id_jm",sort=False)

#ACTION 10: Generate sku id mapping
user_mapping = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/user_id_mapping.csv")
sku_mapping = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/sku_id_mapping.csv")

#ACTION 11: generate non_media_with_seasonality.csv (UTF-8, no qoutes)
brand_pref = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/brand_preference.csv")
brand_pref["brand_pref_client"] = brand_pref["MAGA_NAN"]
usr = pd.merge(usr,brand_pref[["user_log_acct_jm","brand_pref_client"]],how = "left", on = "user_log_acct_jm",sort=False)
usr["brand_pref_client"][usr["brand_pref_client"].isna()=True] = 0
ord["if_client"] = pd.to_numeric(ord["if_client"])
ord["if_client"] = 0
ord["if_client"][ord["target"]==1] = 1

generate_non_media(ord,usr,sku,user_mapping,sku_mapping,start_date=min(CampaignWindow),end_date = max(CampaignWindow),channel='JD',level = NMWS_Granularity)
#ACTION 10: generate impression_digital.csv and impression_digital_map.csv
generate_media_mapping(imp)
#使用media_matrix.csv手动更新x,k,d,y,m 
matrix = pd.read_excel("E:/JD_MTA/2018/2018-09-Nestle/05_Model/v3_unlogged_shrink_external_intermediate_extremely/media_matrix_updated-shrink_external-intermediate-extremely.xlsx",sheet_name = "media_matrix_updated")
generate_media(
    imp,
    clk,
    matrix,
    user_mapping,
    #x = c('device_type', 'loc'),  # Default
    x = c("device_type"),          # TWEAKED
    #k = c('retrieval_type'),      # Default
    k = c("loc", "position_type"), # TWEAKED    #d = c('position_type'),       # Default
    #d = c("cost_type", "creative_format"),      # TWEAKED
    d = c("cost_type"),      # TWEAKED
    #y = c('keyword'),             # Default
    y = c("retrieval_type", "hit_key_ownbrand", "hit_key_competitor"), # TWEAKED
    #m = c('keyword')               # Default
    m = c("retrieval_type", "hit_key_category", "hit_key_function")    # TWEAKED
)

# Simplified impression_digital & split non_media_with_seasonality
impe = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/05_Model/impression_digital.csv",sep = ",")
nmws = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/05_Model/non_media_with_seasonality_day.csv",sep = ",")
impd_lite = impd[impd["id"].isin(nmws[["id"]].drop_duplicates())]
impd_lite.to_csv("E:/Users/Zuo/Test/05_Model/impression_digital_lite.csv")
nmws["date"] = pd.to_datetime(nmws["date"])
nmws["month"] = nmws["date"].dt.month
sortbase = nmws[["month"]].drop_duplicates().sort_values("month")
for i in sortbase["month"]:
    exec("nmws_month_"+ str(i) +"=nmws[nmws['month'] ==" +str(i)+"]")
    exec("nmws_month_"+str(i)+".to_csv(path_or_buf=('E:/Users/Zuo/Test/05_Model/non_media_with_seasonality_day_month_"+str(month_i)+".csv')"))
    exec("del(nmws_month_"+str(i)+")")
del(i)

#STEP 5 MODEL OUTPUT
# Path of Model Input
impression_digital_map_path = "E:/JD_MTA/2018/2018-09-Nestle/05_Model/v3_unlogged_shrink_external_intermediate_extremely/impression_digital_map.csv"
# - normal file path
impression_digital_path = "E:/JD_MTA/2018/2018-09-Nestle/05_Model/v3_unlogged_shrink_external_intermediate_extremely/impression_digital_lite_normal.csv"
non_media_with_seasonality_path = "E:/JD_MTA/2018/2018-09-Nestle/05_Model/v3_unlogged_shrink_external_intermediate_extremely/non_media_with_seasonality_day_normal.csv"
# Path of Model Output
transformed_adstock_path = "E:/JD_MTA/2018/2018-09-Nestle/05_Model/v3_unlogged_shrink_external_intermediate_extremely/model_output_normal/transformed_adstock_201809261340.csv"
bma_output_path = "E:/JD_MTA/2018/2018-09-Nestle/05_Model/v3_unlogged_shrink_external_intermediate_extremely/model_output_normal/bma_output_201809261340.csv"
bma_models_final_path = "E:/JD_MTA/2018/2018-09-Nestle/05_Model/v3_unlogged_shrink_external_intermediate_extremely/model_output_normal/bma_models_final_201809261340.csv"
level = NMWS_Granularity
target = True

model_input = pd.read_csv(transformed_adstock_path,sep=",",na_values=NA_Strings)
model_input.columns = model_input.columns.str.replace('transformed_adstock.','')
model_input = model_input.rename(columns={model_input.columns[0]:'id'})

# **************************************************************************** #
# JCloud平台的transformed_adstock可能会附加一行几乎全部为缺失值的行。这行数据的
# 'data_channel'列可能为'channel'
# 此处通过判断各个行的缺失值数量，加以去除缺失行。
# 默认：如果该行缺失值的数量与数据集的列数只差小等于3，就认为几乎全部为缺失
model_input["almost_all_na_row"] =  (model_input.isna().sum(axis=1))>= (model_input.shape[1] -3)
model_input = model_input[model_input["almost_all_na_row"] == False].drop(columns="almost_all_na_row")

model_input["id"] = model_input["id"].str.replace(",","")
model_input["id"] = pd.to_numeric(model_input["id"])
model_input["data_skuid"] = model_input["data_skuid"].str.replace(",","")
model_input["data_skuid"] = pd.to_numeric(model_input["data_skuid"])

# 转换变量类型(日期与digital曝光字段)
model_input["data_target"] = pd.to_numeric(model_input["data_target"],downcast='integer')
colnames = model_input.columns.tolist()
digital_vars = []
for i in range(len(colnames)):
    if "digital_" in colnames[i]:
        digital_vars.append(colnames[i])
if "data_purch_qty_tgt" in model_input.columns:
    digital_vars.insert(0,"data_purch_qty_tgt")
if "data_volume_tgt" in model_input.columns:
    digital_vars.insert(0,"data_volume_tgt")
if "data_net_extended_price_amt_tgt" in model_input.columns:
    digital_vars.insert(0,"data_net_extended_price_amt_tgt")
if "data_pct_feat" in model_input.columns:
    digital_vars.insert(0,"data_pct_feat")
if "data_pct_disp" in model_input.columns:
    digital_vars.insert(0,"data_pct_disp")
if "data_pct_fdisp" in model_input.columns:
    digital_vars.insert(0,"data_pct_fdisp")
if "data_brand_pref_trip" in model_input.columns:
    digital_vars.insert(0,"data_brand_pref_trip")
if "data_lnppi" in model_input.columns:
    digital_vars.insert(0,"data_lnppi")
if "data_lnbp" in model_input.columns:
    digital_vars.insert(0,"data_lnbp")
if "data_seasindex" in model_input.columns:
    digital_vars.insert(0,"data_seasindex")
if "data_sale_amt" in model_input.columns:
    digital_vars.insert(0,"data_sale_amt")
model_input[digital_vars] = model_input[digital_vars].apply(pd.to_numeric)
del(digital_vars)

data = []
for i in range(len(colnames)):
    if "data_" in colnames[i]:
        data.append(colnames[i])
model_input =model_input[["id","date"]] + matrix[data] +matrix[digital_vars]
if target == True:
    model_input = model_input[model_input[data_target]==1]
elif target == False:
    model_input = model_input[model_input[data_target]==0]
    
model_equation = pd.read_csv(bma_output_path)
model_equation.columns = model_equation.columns.str.replace('bma_output.','')
model_equation = model_equation.rename(columns={model_equation.columns[0]:'key'})
model_equation["wt"] = pd.to_numeric(model_equation["wt"])

bma_models_final = pd.read_csv(bma_models_final_path)
bma_models_final.columns = bma_models_final.columns.str.replace('bma_models_final.','')
bma_models_final = bma_models_final.rename(columns={bma_models_final.columns[0]:'key'})
bma_models_final = bma_models_final[["key","aic"]]
bma_models_final["aic"] = bma_models_final["aic"].astype(str).str.replace(",","")
bma_models_final["aic"] = pd.to_numeric(bma_models_final["aic"])

#Action 11: Fix Model Result Calculation
# 注意：该函数基于non_media_with_seasonality与impression_digital计算。
# 因此：(1) normal与outlier的模型后处理需要分别执行
#     (2) 同一模型的输入的fix结果相互通用，即target为TRUE或FALSE的fix结果一致，无需执行两次
fix(level)

#Action 12: generate score.csv
generate_score(model_input, model_equation, bma_models_final, level, target)