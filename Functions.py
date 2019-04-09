# -*- coding: utf-8 -*-
"""
Created on Wed Feb 13 14:35:27 2019

@author: Zuo
"""
#cross_check function
def cross_check(usr,
                sku,
                ord_usr,
                ord_sku,
                imp_usr,
                imp_sku,
                clk_usr,
                clk_sku):
    usr["usr"] = usr.index + 1
    sku["sku"] = sku.index + 1
    ord_usr["ord"] = ord_usr.index + 1
    ord_sku["ord"] = ord_sku.index + 1
    imp_usr["imp"] = imp_usr.index + 1
    imp_sku["imp"] = imp_sku.index + 1
    clk_usr["clk"] = clk_usr.index + 1
    clk_sku["clk"] = clk_sku.index + 1
    
    #order + sku
    check_sku = pd.merge(ord_sku,sku,how = "left", on = "item_sku_id_jm",sort=False)
    if len(check_sku[check_sku["sku"].isna() == True].index.tolist()) >0:
        missing_sku = check_sku[check_sku["sku"].isna()==True]
        print("Found " + str(len(check_sku[check_sku["item_sku_id_jm"].isna() == True].index.tolist())) + " SKUs in order file but not in sku file, please check.")
        print("[常规现象：通常而言，这部分SKU已下架。若数量不多，则建议删除这部分订单记录]")
    del(check_sku)
    
    #order + user
    check_usr = pd.merge(ord_usr,usr,how = "left", on = "user_log_acct_jm",sort=False)
    if len(check_usr[check_usr["usr"].isna() == True].index.tolist()) >0:
        missing_usr = check_usr[check_usr["usr"].isna()==True]
        print("Found " + str(len(check_usr[check_usr["usr"].isna()== True].index.tolist())) + " users in order file but not in user file, please check.")
        print("[明显错误：上述数字应该为0，至少不应该超过2%。在时间充裕的情况下，需要请京东补齐数据。若数量不多，则建议删除这部分订单记录]")
    del(check_usr)
    
    #order + impression
    check_usr = pd.merge(ord_usr,imp_usr,how = "outer", on = "user_log_acct_jm", sort=False)
    print("Found " + str(round(len(check_usr[check_usr["imp"].isna()==True].index.tolist())/len(check_usr[check_usr["ord"].isna()==False].index.tolist()),2)*100) + "% of users in order file are not in impression file.")
    print("[常规现象：上述数字表示未被曝光的用户数，即1-Reach，base为campaign期间购买、点击或购买的京东用户]")
    print("Found " + str(round(len(check_usr[check_usr["ord"].isna()==True].index.tolist())/len(check_usr[check_usr["imp"].isna()==False].index.tolist()),2)*100) + "% of users in impression file are not in order file.")
    print("[常规现象：上述数字表示Off-Target Rate，base为campaign期间购买、点击或购买的京东用户]")
    del(check_usr)
    
    #order + click
    check_usr = pd.merge(ord_usr,clk_usr,how = "outer", on ="user_log_acct_jm",sort=False)
    print("Found " + str(round(len(check_usr[check_usr["clk"].isna()==True].index.tolist())/len(check_usr[check_usr["ord"].isna()==False].index.tolist()),2)*100) + "% of users in order file are not in click file.")
    print("[常规现象：上述数字可大可小]")
    print("Found " + str(round(len(check_usr[check_usr["ord"].isna()==True].index.tolist())/len(check_usr[check_usr["clk"].isna()==False].index.tolist()),2)*100) + "% of users in click file are not in order file.")
    print("[常规现象：上述数字可大可小]")
    del(check_usr)
    
    #imp + sku
    check_sku = pd.merge(imp_sku,sku,how="left", on = "item_sku_id_jm", sort=False)
    if len(check_sku[check_sku["sku"].isna() == True].index.tolist())>0:
        missing_sku = check_sku[check_sku["sku"].isna()==True]
        print("Found " + str(len(check_sku[check_sku["sku"].isna() == True].index.tolist())) + " SKUs in impression file but not in sku file, please check.")
        print("[常规现象：通常而言，这部分SKU已下架。若数量不多，则建议删除这部分曝光记录]")
    del(check_sku)
    
    #imp + user
    check_usr = pd.merge(imp_usr,usr,how="left", on = "user_log_acct_jm", sort=False)
    if len(check_usr[check_usr["usr"].isna()==True].index.tolist())>0:
        missing_usr = check_usr[check_usr["usr"].isna()==True]
        print("Found " + str(len(check_usr[check_usr["usr"].isna() == True].index.tolist())) + " users in impression file but not in user file, please check.")
        print("[明显错误：尽量请京东补齐数据。尽在紧急情况下且缺失少于1%，则可以忽略]")
    del(check_usr)
    
    #imp + click
    check_usr = pd.merge(imp_usr,clk_usr,how="outer",on="user_log_acct_jm",sort=False)
    print("Found " + str(round(len(check_usr[check_usr["clk"].isna()==True].index.tolist())/len(check_usr[check_usr["imp"].isna()==False].index.tolist()),2)*100) + "% of users in impression file are not in click file.")
    print("[常规现象：上述表示曝光用户中不曾点击的比例，所以该比例通常比较大]")
    if any(check_usr["imp"].isna()==True):
        print("Found " + str(round(len(check_usr[check_usr["imp"].isna()==True].index.tolist())/len(check_usr[check_usr["clk"].isna()==False].index.tolist()),4)*100) + "% of users in click file are not in impression file.")
        print("[明显错误：上述数字表示点击用户中不曾被曝光的比例。该比例应该为0，绝对不应该超过2%，否则请京东校验]")
    else:
        print("0 % of users in click file are not in impression file.")
        print("[常规现象：通常而言，上述数字应该为0%]")
    del(check_usr)
        
    #click + sku
    check_sku = pd.merge(clk_sku,sku,how = "left",on = "item_sku_id_jm",sort=False)
    if len(check_sku[check_sku["sku"].isna() == True].index.tolist()) >0:
        missing_sku = check_sku[check_sku["sku"].isna()==True]
        print("Found " + str(len(check_sku[check_sku["sku"].isna() == True].index.tolist())) + " SKUs in click file but not in sku file, please check.")
        print("[常规现象：上述数字可大可小]")
    del(check_sku)
    
    #click + user
    check_usr = pd.merge(clk_usr,usr,how = "left",on = "user_log_acct_jm",sort=False)
    if len(check_sku[check_sku["usr"].isna() == True].index.tolist()) >0:
        missing_sku = check_sku[check_sku["usr"].isna()==True]
        print("Found " + str(len(check_sku[check_sku["usr"].isna() == True].index.tolist())) + " users in click file but not in user file, please check.")
        print("[明显错误：尽量请京东补齐数据。尽在紧急情况下且缺失少于1%，则可以忽略]")
    del(check_usr)
    return
#clean sku files
def clean_sku(sku,ord):
    sales_sku = ord[["item_sku_id_jm","sale_qtty","before_prefr_amount","after_prefr_amount"]]
    sales_sku = sales_sku.groupby("item_sku_id_jm",sort=False).agg({'item_sku_id_jm':'count','sale_qtty':'sum', 'before_prefr_amount': 'sum','after_prefr_amount':'sum'}).rename(columns={'item_sku_id_jm':'order_count'}).reset_index()
    sales_sku = pd.merge(sales_sku,sku,how = "outer",on="item_sku_id_jm",sort=False)
    sales_sku["include"] = np.nan
    sales_sku["if_client"] = np.nan
    sales_sku["sub_cate"] = np.nan
    sales_sku.to_csv(path_or_buf = "E:/Users/Zuo/Test/sku_w_sales.csv", sep=',', encoding="utf-8",index = False)
    return

#FUNCTION 3: Check Order file abnormalities
def clean_order_check(ord,sku,if_remove_ord_dup,if_remove_ord_free):
    if if_remove_ord_dup == True:
        ord = ord.drop_duplicates()
    #Remove uneccessary skus from order file
    sku["wt"] = pd.to_numeric(sku["wt"])
    sku_distinct = sku[["item_sku_id_jm","wt","if_client","sub_cate","brand_name","Include"]]
    ord = pd.merge(ord,sku_distinct,how = "inner",on = "item_sku_id_jm",sort = False)
    ord["vol"] = ord["sale_qtty"]*ord["wt"]
    backup = ord[["if_client","brand_name","after_prefr_amount","sale_qtty","user_log_acct_jm"]]
    backup["user_no"] = backup["user_log_acct_jm"]
    backup["vol"] = backup["sale_qtty"]*backup["wt"]
    backup = backup.groupby(["if_client","brand_name"],sort=False).agg({
                            'after_prefr_amount': lambda x: backup.loc[x.index, 'after_prefr_amount'][x >= 0].sum(),
                            'vol': lambda x: backup.loc[x.index, 'vol'][x >= 0].sum(),
                            'sale_qtty':lambda x: backup.loc[x.index,'sale_qtty'][x>=0].sum(),
                            'user_log_acct_jm': lambda x: backup.loc[x.index,'user_log_acct_jm'].count(),
                            'user_no': lambda x: backup.loc[x.index,'user_log_acct_jm'].nunique()
            }).rename(columns={'after_prefr_amount':'val','sale_qtty':'unit','user_log_acct_jm':'obs'}).reset_index()
    backup.to_csv(path_or_buf = "E:/Users/Zuo/Test/ord_start_result.csv")
    del(backup)
    
    #Remove Include=0 SKUs
    ord = ord[ord["Include"]==1]
    backup = ord[["if_client","brand_name","after_prefr_amount","sale_qtty","user_log_acct_jm"]]
    backup["user_no"] = backup["user_log_acct_jm"]
    backup["vol"] = backup["sale_qtty"]*backup["wt"]
    backup = backup.groupby(["if_client","brand_name"],sort=False).agg({
                            'after_prefr_amount': lambda x: backup.loc[x.index, 'after_prefr_amount'][x >= 0].sum(),
                            'vol': lambda x: backup.loc[x.index, 'vol'][x >= 0].sum(),
                            'sale_qtty':lambda x: backup.loc[x.index,'sale_qtty'][x>=0].sum(),
                            'user_log_acct_jm': lambda x: backup.loc[x.index,'user_log_acct_jm'].count(),
                            'user_no': lambda x: backup.loc[x.index,'user_log_acct_jm'].nunique()
            }).rename(columns={'after_prefr_amount':'val','sale_qtty':'unit','user_log_acct_jm':'obs'}).reset_index()
    backup.to_csv(path_or_buf = "E:/Users/Zuo/Test/ord_include_result.csv",sep=",")
    del(backup)
    
    #Remove free sampling sku obs (MIGHT NEED TO MODIFY LATER)
    if if_remove_ord_free == True:
        ord_n = ord[(ord["before_prefr_amount"]>0)&(ord["after_prefr_amount"]>0)]
        free = ord[(ord["before_prefr_amount"]<=0)|(ord["after_prefr_amount"]<=0)]
        free["reason"] = "Free"
        free.pd.to_csv(path_or_buf = "E:/Users/Zuo/Test/removed_from_order.csv",sep=",")
        backup = ord_n[["if_client","brand_name","after_prefr_amount","sale_qtty","user_log_acct_jm"]]
        backup["user_no"] = backup["user_log_acct_jm"]
        backup["vol"] = backup["sale_qtty"]*backup["wt"]
        backup = backup.groupby(["if_client","brand_name"],sort=False).agg({
                            'after_prefr_amount': lambda x: backup.loc[x.index, 'after_prefr_amount'][x >= 0].sum(),
                            'vol': lambda x: backup.loc[x.index, 'vol'][x >= 0].sum(),
                            'sale_qtty':lambda x: backup.loc[x.index,'sale_qtty'][x>=0].sum(),
                            'user_log_acct_jm': lambda x: backup.loc[x.index,'user_log_acct_jm'].count(),
                            'user_no': lambda x: backup.loc[x.index,'user_log_acct_jm'].nunique()
            }).rename(columns={'after_prefr_amount':'val','sale_qtty':'unit','user_log_acct_jm':'obs'}).reset_index()
        backup.to_csv(path_or_buf = "E:/Users/Zuo/Test/ord_no_free_result.csv",sep=",")
    del(ord)
    ord_n.to_csv(path_or_buf = "E:/Users/Zuo/Test/ord_in_scope.csv",sep=",")
    
    #Remove abnormal sales
    #STEP 1: check abnormal sales qtty
    order_sales = ord_n[["sale_qtty","after_prefr_amount","user_log_acct_jm"]]
    order_sales["user_no"] = order_sales["user_log_acct_jm"]
    order_sales["vol"] = order_sales["sale_qtty"]*order_sales["wt"]
    order_sales["sales_value"] = order_sales["after_prefr_amount"]
    order_sales = order_sales.groupby("sale_qtty",sort=False).agg({
                            'after_prefr_amount': lambda x: order_sales.loc[x.index, 'after_prefr_amount'][x >= 0].sum(),
                            'vol': lambda x: order_sales.loc[x.index, 'vol'][x >= 0].sum(),
                            'sale_qtty':lambda x: order_sales.loc[x.index,'sale_qtty'][x>=0].sum(),
                            'user_log_acct_jm': lambda x: order_sales.loc[x.index,'user_log_acct_jm'].count(),
                            'user_no': lambda x: order_sales.loc[x.index,'user_log_acct_jm'].nunique(),
                            'sales_value':lambda x: order_sales.loc[x.index,'sales_value'][x>=0].sum()
            }).rename(columns={'after_prefr_amount':'val','sale_qtty':'unit','user_log_acct_jm':'obs'}).reset_index()
    order_sales.to_csv(path_or_buf = "E/Users/Zuo/Test/order_abnormal_qtty.csv",sep=",")
    del(order_sales)
    
    #STEP 2: check sku abnormal price
    order_sku_median = ord_n[["item_sku_id_jm","after_prefr_amount","sale_qtty"]]
    order_sku_median["median_price"] = order_sku_median["after_prefr_amount"]/order_sku_median["sale_qtty"]
    order_sku_median = order_sku_median.groupby("item_sku_id_jm",sort=False).agg({"median_price": lambda x:order_sku_median.loc[x.index,"median_price"][x>=0].median()}).reset_index()
    
    order_sku = pd.merge(ord_n,order_sku_median,how="inner",on="item_sku_id_jm",sort=False)
    order_sku["price_threshold"] = round((order_sku["before_prefr_amount"]/order_sku["sale_qtty"]/order_sku["median_price"]),2)
    
    order_sku_ab = order_sku[["after_prefr_amount","sale_qtty","user_log_acct_jm","price_threshold"]]
    order_sku_ab["user_no"] = order_sku_ab["user_log_acct_jm"]
    order_sku_ab["sales_unit"] = order_sku_ab["sale_qtty"]
    order_sku_ab["vol"] = order_sku_ab["sale_qtty"]*order_sku_ab["wt"]
    order_sku_ab = order_sku_ab.groupby("price_threshold",sort=False).agg({
                            'after_prefr_amount': lambda x: order_sku_ab.loc[x.index, 'after_prefr_amount'][x >= 0].sum(),
                            'vol': lambda x: order_sku_ab.loc[x.index, 'vol'][x >= 0].sum(),
                            'sale_qtty':lambda x: order_sku_ab.loc[x.index,'sale_qtty'][x>=0].sum(),
                            'user_log_acct_jm': lambda x: len(x),
                            'user_no': lambda x: order_sku_ab.loc[x.index,'user_log_acct_jm'].nunique(),
                            'sales_unit':lambda x: order_sku_ab.loc[x.index,'sales_unit'][x>=0].sum()
            }).rename(columns={'after_prefr_amount':'val','sale_qtty':'unit','user_log_acct_jm':'obs'}).reset_index()
    order_sku_ab.to_csv(path_or_buf = "E:/Users/Zuo/Test/order_sku_abnormal_prices.csv",sep=",")
    order_sku.to_csv(path_or_buf = "E:/Users/Zuo/Test/temp_order_sku.csv",sep=",")
    
    #STEP 3: check abnormal transactions
    ord_n["dt"] = pd.to_datetime(ord_n["sale_ord_tm"],format ='y%-m%-d%')
    ord_trans = ord_n[["user_log_acct_jm","after_prefr_amount","sale_qtty","sale_ord_tm","dt"]]
    ord_trans["user_no"] = ord_trans["user_log_acct_jm"]
    ord_trans["vol"] = ord_trans["sale_qtty"]*ord_trans["wt"]
    ord_trans = ord_trans.groupby(["user_log_acct_jm","dt"],sort=False).agg({
                            'after_prefr_amount': lambda x: ord_trans.loc[x.index, 'after_prefr_amount'][x >= 0].sum(),
                            'vol': lambda x: ord_trans.loc[x.index, 'vol'][x >= 0].sum(),
                            'sale_qtty':lambda x: ord_trans.loc[x.index,'sale_qtty'][x>=0].sum(),
                            'user_log_acct_jm': lambda x: ord_trans.loc[x.index,'user_log_acct_jm'].count(),
                            'user_no': lambda x: ord_trans.loc[x.index,'user_log_acct_jm'].nunique(),
                            'sale_ord_tm':lambda x: ord_trans.loc[x.index,'sale_ord_tm'].nunique()
            }).rename(columns={'after_prefr_amount':'val','sale_qtty':'unit','user_log_acct_jm':'obs','sale_ord_tm':'no_trans_day'}).reset_index()
    
    ord_trans_check = ord_trans.groupby("no_trans_day",sort=False).agg({
            'val':lambda x: ord_trans.loc[x.index,'val'][x>=0].sum(),
            'vol': lambda x: ord_trans.loc[x.index, 'vol'][x >= 0].sum(),
            'unit':lambda x: ord_trans.loc[x.index,'unit'][x>=0].sum(),
            'obs': lambda x: ord_trans.loc[x.index,'obs'].count(),
            'user_no': lambda x: ord_trans.loc[x.index,'user_log_acct_jm'].nunique(),
            }).reset_index()
    ord_trans_check.to_csv(path_or_buf = "E/User/Zuo/Test/order_trans_abnormal.csv",sep=",")
    return
#FUNCTION 4: CLEAN ORDER FILE
def clean_order(ord,
                sku,
                auto_select,
                abnormal_qtty_upper_threshold,
                abnormal_price_lower_threshold,
                abnormal_price_upper_threshold,
                trans_abnormal_upper_threshold):
    abnormal = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/removed_from_order.csv",sep=",")
    #STEP 1: remove abnormal sales qtty
    if auto_select == True:
        result = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/order_abnormal_qtty.csv")
        result = result.sort_values("sale_qtty").reset_index(drop=True)
        result["percentage"] = 100*np.cumsum(result["obs"])/(result["obs"].sum())
        threshold = min(result[result["percentage"] > 99].index.tolist())
        abnormal_qtty_upper_threshold = threshold
        
    order_sales_ab = ord[ord["sale_qtty"]>abnormal_qtty_upper_threshold]
    ord_n = ord[ord["sale_qtty"]<=abnormal_qtty_upper_threshold]
    order_sales_ab = order_sales_ab.drop(columns="volume")
    order_sales_ab = order_sales_ab.drop(columns="date)
    order_sales_ab["reason"] = "ab_sales_qtty"
    abnormal = abnormal.append(order_sales_ab)
    ord_n["wt"] = pd.to_numeric(ord_n["wt"])
    
    backup = ord_n[["if_client","brand_name","after_prefr_amount","sale_qtty","user_log_acct_jm"]]
    backup["user_no"] = backup["user_log_acct_jm"]
    backup["vol"] = backup["sale_qtty"]*backup["wt"]
    backup = backup.groupby(["if_client","brand_name"],sort=False).agg({
                            'after_prefr_amount': lambda x: backup.loc[x.index, 'after_prefr_amount'][x >= 0].sum(),
                            'vol': lambda x: backup.loc[x.index, 'vol'][x >= 0].sum(),
                            'sale_qtty':lambda x: backup.loc[x.index,'sale_qtty'][x>=0].sum(),
                            'user_log_acct_jm': lambda x: backup.loc[x.index,'user_log_acct_jm'].count(),
                            'user_no': lambda x: backup.loc[x.index,'user_log_acct_jm'].nunique()
            }).rename(columns={'after_prefr_amount':'val','sale_qtty':'unit','user_log_acct_jm':'obs'}).reset_index()
    backup.to_csv(path_or_buf = "E:/Users/Zuo/Test/ord_after_additional_removal_result.csv")
    del(ord)
    
    #STEP 2: remove abnormal sku sales price
    if auto_select == True:
        result = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/order_sku_abnormal_price.csv")
        result = result.sort_values("price_threshold")
        result["percentage"] = 100*np.cumsum(result["sales_unit"])/(result["sales_unit"].sum())
        threshold = min(result[result["percentage"]>1].index.tolist())
        abnormal_price_lower_threshold = threshold
        threshold = min(result[result["percentage"]>99].index.tolist())
        abnormal_price_upper_threshold = threshold
    
    order_sku = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/temp_order_sku.csv",encoding='utf-8',sep=",")
    order_sku_ab = order_sku[(order_sku["price_threshold"]<abnormal_price_lower_threshold)|(order_sku["price_threshold"]>abnormal_price_upper_threshold)]
    order_sku_ab = order_sku_ab[["user_log_acct_jm","item_sku_id_jm","sale_ord_tm"]].drop_duplicates()
    order_sales_ab = pd.merge(ord_n,order_sku_ab,how='inner',on=['user_log_acct_jm','item_sku_id_jm','sale_ord_tm'],sort=False)
    ord_n = pd.merge(ord_n,order_sku_ab, how = "outer", on = ['user_log_acct_jm','item_sku_id_jm','sale_ord_tm'],sort=False, indicator = True)
    ord_n = ord_n[ord_n["_merge"]=="left_only"].drop(columns='_merge')
    order_sales_ab["reason"] = "ab_sku_price"
    abnormal["wt"] = pd.to_numeric(abnormal["wt"])
    abnormal = abnormal.append(order_sales_ab)
    backup = ord_n[["if_client","brand_name","after_prefr_amount","sale_qtty","user_log_acct_jm"]]
    backup["user_no"] = backup["user_log_acct_jm"]
    backup["vol"] = backup["sale_qtty"]*backup["wt"]
    backup = backup.groupby(["if_client","brand_name"],sort=False).agg({
                            'after_prefr_amount': lambda x: backup.loc[x.index, 'after_prefr_amount'][x >= 0].sum(),
                            'vol': lambda x: backup.loc[x.index, 'vol'][x >= 0].sum(),
                            'sale_qtty':lambda x: backup.loc[x.index,'sale_qtty'][x>=0].sum(),
                            'user_log_acct_jm': lambda x: backup.loc[x.index,'user_log_acct_jm'].count(),
                            'user_no': lambda x: backup.loc[x.index,'user_log_acct_jm'].nunique()
            }).rename(columns={'after_prefr_amount':'val','sale_qtty':'unit','user_log_acct_jm':'obs'}).reset_index()
    backup.to_csv(path_or_buf = "E:/Users/Zuo/Test/ord_after_additional_removal_result.csv")
    del(order_sku)
    
    #STEP 3: remove abnormal transactions
    if auto_select == True:
        result = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/02_Cleaned_Data/order_trans_abnormal.csv")
        result = result.sort_values("no_trans_day")
        result["percentage"] = 100 * np.cumsum(result["user_no"])/(result["user_no"].sum())
        if result["percentage"][result["no_trans_day"]==10] >=99:
            trans_abnormal_upper_threshold = 10
        else:
            threshold = min(result[result["percentage"]>99].index.tolist())
            trans_abnormal_upper_threshold = threshold
            
    ord_n["date"] = pd.to_datetime(ord_n["sale_ord_tm"],format ='y%-m%-d%')
    ord_trans = ord_n[["user_log_acct_jm","date","sale_ord_tm"]]
    ord_trans = ord_trans.groupby(["user_log_acct_jm","date"],sort=False).agg({
                            'sale_ord_tm': lambda x: x.nunique()
            }).rename(columns={'sale_ord_tm':'no_trans_day'}).reset_index()
    remove = ord_trans[ord_trans["no_trans_day"] > trans_abnormal_upper_threshold]
    order_sales_ab = pd.merge(ord_n,remove,how="inner", on = ["user_log_acct_jm","date"],sort=False)
    ord_n = pd.merge(ord_n,remove,how="outer",on=["user_log_acct_jm","date"],sort=False,indicator=True)
    ord_n = ord_n[ord_n["_merge"]=="left_only"].drop(columns='_merge')
    order_sales_ab = order_sales_ab.drop(columns = "volume")
    order_sales_ab = order_sales_ab.drop(columns = "date")
    order_sales_ab = order_sales_ab.drop(columns = "price")
    order_sales_ab = order_sales_ab.drop(columns = "no_trans_day")
    order_sales_ab["reason"] = "ab_trans_no"
    abnormal = abnormal.append(order_sales_ab)
    ord_n["wt"] = pd.to_numeric(ord_n["wt"])
    backup = ord_n[["if_client","brand_name","after_prefr_amount","sale_qtty","user_log_acct_jm"]]
    backup["user_no"] = backup["user_log_acct_jm"]
    backup["vol"] = backup["sale_qtty"]*backup["wt"]
    backup = backup.groupby(["if_client","brand_name"],sort=False).agg({
                            'after_prefr_amount': lambda x: backup.loc[x.index, 'after_prefr_amount'][x >= 0].sum(),
                            'vol': lambda x: backup.loc[x.index, 'vol'][x >= 0].sum(),
                            'sale_qtty':lambda x: backup.loc[x.index,'sale_qtty'][x>=0].sum(),
                            'user_log_acct_jm': lambda x: backup.loc[x.index,'user_log_acct_jm'].count(),
                            'user_no': lambda x: backup.loc[x.index,'user_no'].nunique()
            }).rename(columns={'after_prefr_amount':'val','sale_qtty':'unit','user_log_acct_jm':'obs'}).reset_index()
    ord_n["year_month"] = ord_n["date"].dt.to_period('M')
    ord_n["weekday"] = ord_n["date"].dt.weekday + 1
    ord_n["week_start"] = ord_n["date"] - pd.to_timedelta(ord_n["weekday"], unit='D') + datetime.timedelta(days=1)
    ord_n["week_end"] = ord_n["week_start"] + datetime.timedelta(days=6)
    ord_n["date"] = pd.to_datetime(ord_n["date"]).dt.date
    ord_n["weekday"][ord_n["weekday"]=="1"] = "Mon"
    ord_n["weekday"][ord_n["weekday"]=="2"] = "Tue"
    ord_n["weekday"][ord_n["weekday"]=="3"] = "Wed"
    ord_n["weekday"][ord_n["weekday"]=="4"] = "Thu"
    ord_n["weekday"][ord_n["weekday"]=="5"] = "Fri"
    ord_n["weekday"][ord_n["weekday"]=="6"] = "Sat"
    ord_n["weekday"][ord_n["weekday"]=="7"] = "Sun"
    abnormal.to_csv(path_or_buf="E/Users/Zuo/Test/removed_from_order.csv")
    ord_n.to_csv(path_or_buf="E/Users/Zuo/Test/order_cleaned.csv")
    sku.to_csv(path_or_buf="E/Users/Zuo/Test/sku_cleaned.csv)
    return

def clean_imp_click (imp,clk,sku):
    imp["device_type_updated"] = imp["device_type"]
    imp["device_type_updated"][imp["device_type_updated"]!="PC"] = "Mobile"
    
    clk["device_type_updated"] = clk["device_type"]
    clk["device_type_updated"][clk["device_type_updated"]!="PC"] = "Mobile"
    
    #relabeling loc
    
    #relabeling retrieval_type
    imp["retrieval_type"] = pd.to_numeric(imp["retrieval_type"])
    imp["retrieval_type"][imp["retrieval_type"]=="1"] = "Display"
    imp["retrieval_type"][imp["retrieval_type"]=="2"] = "Search"
    imp["retrieval_type"] = imp["retrieval_type"].to_string
    
    clk["retrieval_type"] = pd.to_numeric(clk["retrieval_type"])
    clk["retrieval_type"][clk["retrieval_type"]=="1"] = "Display"
    clk["retrieval_type"][clk["retrieval_type"]=="2"] = "Search"
    clk["retrieval_type"] = clk["retrieval_type"].to_string
    
    #Add Date
    imp["date"] = pd.to_datetime(imp["imp_time"],format = 'y%-m%-d%')
    clk["date"] = pd.to_datetime(clk["click_time"],format = 'y%-m%-d%')
    
    #remove unwanted sku exposure from imp and clk
    imp_remove = pd.merge(imp, sku[["item_sku_id_jm"]],how = "outer", on="item_sku_id_jm",sort=False,indicator=True)
    imp_remove = imp_remove[imp_remove["_merge"] == "left_only"].drop(columns="_merge")
    imp = pd.merge(imp, sku[["item_sku_id_jm"]],how="inner",on="item_sku_id_jm",sort=False)
    if len(imp_remove) >0:
        imp_remove["reason"] = "no_use_sku"
        print("Removed " + str(len(imp_remove)) + " obs from Impression because it is brought by unwanted SKUs.")
    
    clk_remove = pd.merge(clk,sku[["item_sku_id_jm"]],how="outer",on="item_sku_id_jm",sort=False,indicator=True)
    clk_remove = clk_remove[clk_remove["_merge"] == "left_only"].drop(columns="_merge")
    clk = pd.merge(clk,sku[["item_sku_id_jm"]],how="inner",on="item_sku_id_jm",sort=False)
    if len(clk_remove) >0:
        clk_remove["reason"] = "no_use_sku"
        print("Removed " + str(len(clk_remove))+" obs from Click because it is brought by unwanted SKUs.")
    
    # 新增代码，用于统一Impression和Click表中Display广告对应Keyword。          #
    # 待后续Keyword的提供方式确定后，增加该部分代码 
    
     #remove not in imp position from clk
     imp_distinct = imp[["user_log_acct_jm","device_type","loc","retrieval_type","imp_time","creative_format","cost_type","ad_plan_id","hit_key_ownbrand","hit_key_competitor","hit_key_category","hit_key_function","position_type","date"]].drop_duplicates()
     clk_dup = pd.merge(clk,imp_distinct,how="left",on=["user_log_acct_jm","device_type","loc","retrieval_type","creative_format","cost_type","ad_plan_id","hit_key_ownbrand","hit_key_competitor","hit_key_category","hit_key_function","position_type","date"],sort=False)
     clk_notmatch = clk_dup[clk_dup["imp_time"].isna()==True]
     clk_notmatch = clk_notmatch.drop(columns = "imp_time")
     clk_notmatch = clk_notmatch.drop_duplicates()
     clk_notmatch["reason"] = "no match in imp"
     print("Found " + str(round(len(clk_notmatch)/len(clk),4)*100)+" % of clicks not match with imp; they have been removed from clk.")
     
     clk_remove = clk_remove.append(clk_notmatch)
     clk = pd.merge(clk,clk_notmatch,how="outer",sort=False,indicator="True")
     clk = clk[clk["_merge"]=="left_only"].drop(columns="_merge")
     del(clk_notmatch,ckj_dup)
     
     imp.to_csv(path_or_buf="E:/Users/Zuo/Test/imp_cleaned.csv.",sep=",")
     clk.to_csv(path_or_buf="E:/Users/Zuo/Test/clk_cleaned.csv.",sep=",")
     imp_remove.to_csv(path_or_buf="E:/Users/Zuo/Test/removed_from_imp.csv",sep=",")
     clk_remove.to_csv(path_or_buf="E:/Users/Zuo/Test/removed_from_clk.csv",sep=",")

def brand_pref(ord,brand):
    first_12 = ord[["year_month"]].drop_duplicates().sort_values("year_month").head(12)
	ord = pd.merge(ord,first_12,how="inner",on="year_month",sort=False)
	brand_no = len(brand)
	trans = ord[["sale_ord_tm","user_log_acct_jm"]]
	trans = trans.groupby("user_log_acct_jm",sort=False).agg({'sale_ord_tm':lambda x: x.nunique()}).rename(columns={'sale_ord_tm':'trans_no'}).reset_index()
	#统计order表中每个用户的全部购买频次
    i =1
	for i <= brand_no:
	    target = brand[i]
		ord_part = ord[ord["brand_name"]==target]
		trans_target = ord_part.groupby("user_log_acct_jm",sort=False).agg({'sale_ord_tm':lambda x: lambda x: x.nunique()}).rename(columns={'sale_ord_tm':target}).reset_index()
		#统计order表中每个用户对特定品牌的订单数
		#用该品牌名称作为购买频次的标签
        trans = pd.merge(trans,trans_target,how="left",on="user_log_acct_jm",sort=False)
		trans[target][trans[target].isna()==True] = 0
		trans[target] = trans[target]/trans["trans_no"]
    trans.to_csv(path_or_buf = "E:/Users/Zuo/Test/brand_preference.csv",sep=",")
	return

def tagging_new(ord,usr,brand,period):
    if brand == "category":
	   ord["target"] = 1
	elif brand == "client":
	   ord = ord[ord["Include"] == 1]
	   ord["target"] = 1
    else:
	   ord = ord[ord["brand_name"] = brand]
	   ord["target"] = 1
	ord["date"] = pd.to_datetime(ord["date"])
	first = ord[ord["date"] < period]
	second = ord[ord["date"] >= period]
	
	first = first.groupby(["target","user_log_acct_jm"],sort=False).agg({'after_prefr_amount':lambda x: first.loc[x.index,'after_prefr_amount'][x>=0].sum()}).rename(columns = {'after_prefr_amount':'first_target'}).reset_index()
	second = second.groupby(["target","user_log_acct_jm"],sort=False).agg({'after_prefr_amount':lambda x: second.loc[x.index,'after_prefr_amount'][x>=0].sum()}).rename(columns = {'after_prefr_amount':'second_target'}).reset_index()
	result = pd.merge(first,second, how = "outer", on = "user_log_acct_jm",sort=False)
	del(first,second)
	result["type"] = np.nan
	result["type"][(result["first_target"].isna()==False)&(result["second_target"].isna()==False)] = "repeat_user"
	result["type"][(result["first_target"].isna()==True)&(result["second_target"].isna()==False)] = "new_user"
	result["type"][result["second_target"].isna()==True] = "lost_user"
    second["type"][second["second_target"].isna()==True] = "lost_user"
	
	if brand = "category":
	   result = result.rename(columns = {"type":"category_type"})
	   usr = pd.merge(usr,result[["user_log_acct_jm","category_type"]],how = "left", on = "user_log_acct_jm",sort=False)
	elif brand = "client":
	   result = result.rename(columns = {"type":"client_type"})
	   usr = pd.merge(usr,result[["user_log_acct_jm","client_type"]],how = "left", on = "user_log_acct_jm",sort=False)
	else:
	   result = result.rename(columns={"type":"_type"})
	   usr = pd.merge(usr,result[["user_log_acct_jm","_type"]],how = "left", on = "user_log_acct_jm",sort=False)
	
	return(usr)
    
def generate_non_media(ord,
                       usr,
                       sku,
                       user_mapping,
                       sku_mapping,
                       start_date,
                       end_date,
                       channel,
                       level):
    ord["date"] = pd.to_datetime(ord["date"])
    ord = ord[(ord["date"]>=start_date)&(ord["date"]<=end_date)]
    
    #Select useful columns from order file
    ord = pd.merge(ord, user_mapping, how = "inner", on ="user_log_acct_jm",sort=False)
    ord["wt"] = pd.to_numeric(ord["wt"])
    non_media = ord[["user_log_acct_jm",
                     "item_sku_id_jm",
                     "sale_ord_tm",
                     "sale_qtty",
                     "before_prefr_amount",
                     "after_prefr_amount',
                     "wt",
                     "if_client",
                     "subcate",
                     "date",
                     "user_id"]]
    if level == "second":
        non_media_day = non_media
        non_media_day["volume"] = non_media_day["sale_qtty"]*non_media_day["wt"]
        non_media_day = non_media_day.groupby(["user_log_acct_jm",
                                           "user_id",
                                           "item_sku_id_jm",
                                           "date",
                                           "sale_ord_tm",
                                           "if_client",
                                           "sub_cate"],sort=False).agg({
                                           'sale_qtty':'sum','before_prefr_amount':'sum','after_prefr_amount':'sum','volume':'sum'}).reset_index()
    elif level == "day":
        non_media_day = non_media
        non_media_day["volume"] = non_media_day["sale_qtty"]*non_media_day["wt"]
        non_media_day = non_media_day.groupby(["user_log_acct_jm",
                                           "user_id",
                                           "item_sku_id_jm",
                                           "date",
                                           "if_client",
                                           "sub_cate"],sort=False).agg({
                                           'sale_qtty':'sum','before_prefr_amount':'sum','after_prefr_amount':'sum','volume':'sum'}).reset_index()
    
    #Calculate week end (SUNDAY)
    non_media_day["weekend"] = np.nan
    non_media_day["weekend"] = pd.to_datetime(non_media_day["weekend"])
    non_media_day["weekday"] = non_media_day["date"].dt.weekday
    non_media_day["weekend"][non_media_day["weekday"]==6] = non_media_day["date"][non_media_day["weekday"] == 6]
    non_media_day["weekend"][non_media_day["weekday"]==0] = non_media_day["date"][non_media_day["weekday"] == 0] + datetime.timedelta(days = 6)
    non_media_day["weekend"][non_media_day["weekday"]==1] = non_media_day["date"][non_media_day["weekday"] == 1] + datetime.timedelta(days = 5)
    non_media_day["weekend"][non_media_day["weekday"]==2] = non_media_day["date"][non_media_day["weekday"] == 2] + datetime.timedelta(days = 4)
    non_media_day["weekend"][non_media_day["weekday"]==3] = non_media_day["date"][non_media_day["weekday"] == 3] + datetime.timedelta(days = 3)
    non_media_day["weekend"][non_media_day["weekday"]==4] = non_media_day["date"][non_media_day["weekday"] == 4] + datetime.timedelta(days = 2)
    non_media_day["weekend"][non_media_day["weekday"]==5] = non_media_day["date"][non_media_day["weekday"] == 5] + datetime.timedelta(days = 1)
    non_media_day = non_media_day.drop(columns="weekday")
    
    non_media_day["ahhldid_nbr"] = non_media_day["user_id"]
    non_media_day["pod_id"] = 1
    non_media_day["pseudo"] = 1
    non_media_day["store_dma"] = 1
    non_media_day["hh_dma"] = 1
    non_media_day["channel"] = channel
    non_media_day = non_media_day.rename(columns={'weekend':'week'})
    non_media_day["purch_qty_tgt"] = non_media_day["sale_qtty"]
    non_media_day["volume_tgt"] = non_media_day["volume"]
    non_media_day["net_extended_price_amt_tgt"] = non_media_day["after_prefr_amount"]/non_media_day["volume"]
    non_media_day = non_media_day.rename(columns = {'if_client':'target'})
    non_media_day["pct_feat"] = 0
    non_media_day["pct_disp"] = 0
    non_media_day["pct_fdisp"] = 0
    
    non_media_day = pd.merge(non_media_day, usr[["user_log_acct_jm","brand_pref_client"]],how="inner",on="user_log_acct_jm",sort=False)
    non_media_day = non_media_day.rename(columns={'brand_pref_client':'brand_pref_trip'})
    non_media_day["seasindex"] = 0
    # COMPUTE LNPPI - CATEGORY WISE PROMOTION INDEX - ONE COMMON BETA REGAARDLESS OF BRANDS
    non_media_day["lnppi"] = np.log1p(1-(non_media_day["after_prefr_amount"]/non_media_day["before_prefr_amount"]))
    non_media_day["lnppi"][non_media_day["target"]==0] = non_media_day["lnppi"][non_media_day["target"]==0] * (-1)
    non_media_day["lnbp"] = 0
    non_media_day = pd.merge(non_media_day, sku_mapping, how = "inner", on = "item_sku_id_jm", sort=False)
    non_media_day["sale_amt"] = non_media_day["after_prefr_amount"]
    non_media_day = non_media_day.rename(columns={'user_id':'id'})
    
    if level == "second":
        non_media_final = non_media_day[["ahhldid_nbr",
                                         "pod_id",
                                         "pseudo",
                                         "store_dma",
                                         "hh_dma",
                                         "channel",
                                         "week",
                                         "date",
                                         "purch_qty_tgt",
                                         "volume_tgt",
                                         "net_extended_price_amt_tgt",
                                         "target",
                                         "id",
                                         "pct_feat",
                                         "pct_disp",
                                         "pct_fdisp",
                                         "brand_pref_trip",
                                         "lnppi",
                                         "lnbp",
                                         "seasindex",
                                         "skuid",
                                         "sale_ord_tm",
                                         "sale_amt"]]
    elif level == "day":
        non_media_final = non_media_day[["ahhldid_nbr",
                                         "pod_id",
                                         "pseudo",
                                         "store_dma",
                                         "hh_dma",
                                         "channel",
                                         "week",
                                         "date",
                                         "purch_qty_tgt",
                                         "volume_tgt",
                                         "net_extended_price_amt_tgt",
                                         "target",
                                         "id",
                                         "pct_feat",
                                         "pct_disp",
                                         "pct_fdisp",
                                         "brand_pref_trip",
                                         "lnppi",
                                         "lnbp",
                                         "seasindex",
                                         "skuid",
                                         "sale_amt"]]
    non_media_final = non_media_final.sort_values("id")
    
    print('max_purchase_date: ' + str(max(non_media_final["date"])))
    print('min_purchase_date: ' + str(min(non_media_final["date"])))
    print('num_model_weeks: ' + str((non_media_final["week"].nunique())))
    print('num_purchase_weeks: ' + str((non_media_final["week"].nunique())))
    
    if level == "second":
        non_media_final.to_csv("E:/Users/Zuo/Test/non_media_with_seasonality_second.csv")
    elif level == "day":
        non_media_final.to_csv("E:/Users/Zuo/Test/non_media_with_seasonality_day.csv")
    return

def generate_keyword(imp,clk):
    keyword_imp = imp[["keyword","user_log_acct_jm"]]
    keyword_imp["imp_user_count"] = keyword_imp["user_log_acct_jm"]
    keyword_imp = keyword_imp.groupby("keyword",sort=False).agg({'user_log_acct_jm':'count','imp_user_count':'nunique'}).rename(columns={'user_log_acct_jm':'imp_count'}).reset_index()
    keyword_clk = clk.groupby("keyword",sort=False).agg({'user_log_acct_jm':'count'}).rename(columns={'user_log_acct_jm':'click_count'}).reset_index()
    keyword = pd.merge(keyword_imp,keyword_clk,how="outer",on="keyword",sort=False)
    keyword.to_csv(path_or_buf ="E:/Users/Zuo/Test/05_Model/keyword_check.csv")

def generate_media_mapping(imp):
    imp = imp.rename(str.lower,axis='coloumns')
    matrix = imp.groupby(["device_type","loc","retrieval_type","creative_format","cost_type","position_type","hit_key_own_brand","hit_key_competitor","hit_key_category","hit_key_function","imp_time"],sort=False).agg({'imp_time':'count'}).rename(columns={'imp_time':'imp'}).reset_index()
    matrix["x"] = np.nan
    matrix["k"] = np.nan
    matrix["d"] = np.nan
    matrix["y"] = np.nan
    matrix["m"] = np.nan
    matrix = matrix.sort_values(["device_type","loc","position_type","retrieval_type","creative_format","cost_type","hit_key_ownbrand","hit_key_competitor","hit_key_category","hit_key_function"])
    matrix.to_csv(path_or_buf ="E:/Users/Zuo/Test/05_Model/media_matrix.csv" )

def generate_media(imp,clk,matrix,user_mapping,x,k,d,y,m):
    dim_type = ['x','k','d','y','m']
    combo1 = []
    combo = []
    for i in range(5):
        exec("combo1.append(len("+dim_type[i]+"))") #每个dimension中含有几个元素
    for i in range(len(combo1)):
        if combo1[i] > 1: 
            combo.append(i) #含有多个元素的dimension的位置
    if len(combo) >0:
        for i in range(len(combo)):
            exec("col=" + dim_type[combo[i]]) #提取含有多个元素的dimension
            exec((dim_type[combo[i]]+" = 'combo_'+str(i+1)")) #标记含有多个元素的dimension,与后续拆分重新连接的元素进行对应
            exec("matrix['combo_"+str(i+1)+"']=''")
            exec("clk['combo_"+str(i+1)+"']=''")
            for j in range(len(col)):
                exec("matrix['combo_"+str(i+1)+"']= matrix['combo_"+str(i+1)+"']+'-'+matrix['"+col[j]+"']")
                #将同一dimension的元素拆分后用-连接
                #exec("imp['combo_"+str(i+1)+"']= imp['combo_"+str(i+1)+"']+'-'+imp['"+col[j]+"']")
                exec("clk['combo_"+str(i+1)+"']= clk['combo_"+str(i+1)+"']+'-'+clk['"+col[j]+"']")
        for i in range(5):
           exec("dim_type[i] =''.join(" + dim_type[i]+")")#将dimension实际元素放入dim_type中
           
        for i in matrix.columns:
            if (i not in dim_type) & (i not in ['x','k','d','y','m']):
                matrix = matrix.drop(i,axis=1)
        #从matrix中去除dimension之外的数据
        
        #Generate dimensions
    dim_x = matrix[['x']].drop_duplicates().reset_index(drop=True)
    dim_x = dim_x.rename(columns={'x':'label'})
    dim_x["category"] = "x"
    dim_x["dim_key"] = dim_x.index + 1
        
    dim_k = matrix[['k']].drop_duplicates().reset_index(drop=True)
    dim_k = dim_k.rename(columns={'k':'label'})
    dim_k["category"] = "k"
    dim_k["dim_key"] = dim_k.index + 1
        
    dim_d = matrix[['d']].drop_duplicates().reset_index(drop=True)
    dim_d = dim_d.rename(columns={'d':'label'})
    dim_d["category"] = "d"
    dim_d["dim_key"] = dim_d.index + 1
        
    dim_y = matrix[['y']].drop_duplicates().reset_index(drop=True)
    dim_y = dim_y.rename(columns={'y':'label'})
    dim_y["category"] = "y"
    dim_y["dim_key"] = dim_y.index + 1
        
    dim_m = matrix[['m']].drop_duplicates().reset_index(drop=True)
    dim_m = dim_m.rename(columns={'m':'label'})
    dim_m["category"] = "m"
    dim_m["dim_key"] = dim_m.index + 1
        
    dim = dim_x.append([dim_k,dim_d,dim_y,dim_m])
    dim["nchar"] = dim["dim_key"].astype(str).str.len()
        
    dim["dim_key_updated"] = np.nan
    if len(dim[dim["nchar"]==3].index.tolist()) >0:
        dim["dim_key_updated"][dim["nchar"]==3] = dim["category"][dim["nchar"]==3] + dim["dim_key"][dim["nchar"]==3].astype(str)
    if len(dim[dim["nchar"]==2].index.tolist()) >0:
        dim["dim_key_updated"][dim["nchar"]==2] = dim["category"][dim["nchar"]==2] + '0' + dim["dim_key"][dim["nchar"]==2].astype(str)
    if len(dim[dim["nchar"]==1].index.tolist()) >0:
        dim["dim_key_updated"][dim["nchar"]==1] = dim["category"][dim["nchar"]==1] + '00' + dim["dim_key"][dim["nchar"]==1].astype(str)
            
    dim["dim_key_updated"] = dim["dim_key_updated"].str.upper()
    matrix = pd.merge(matrix,dim[dim["category"]=="x"],how="inner",left_on="x",right_on = 'label',sort=False)
    matrix["x"] = matrix["dim_key_updated"]
    matrix = matrix.drop(columns="dim_key_updated").drop(columns="label")
        
    matrix = pd.merge(matrix,dim[dim["category"]=="k"],how="inner",left_on="k",right_on="label",sort=False)
    matrix["k"] = matrix["dim_key_updated"]
    matrix = matrix.drop(columns="dim_key_updated").drop(columns="label")
        
    matrix = pd.merge(matrix,dim[dim["category"]=="d"],how="inner",left_on="d",right_on="label",sort=False)
    matrix["d"] = matrix["dim_key_updated"]
    matrix = matrix.drop(columns="dim_key_updated").drop(columns="label")
        
    matrix = pd.merge(matrix,dim[dim["category"]=="y"],how="inner",left_on="y",right_on="label",sort=False)
    matrix["y"] = matrix["dim_key_updated"]
    matrix = matrix.drop(columns="dim_key_updated").drop(columns="label")
        
    matrix = pd.merge(matrix,dim[dim["category"]=="m"],how="inner",left_on="m",right_on="label",sort=False)
    matrix["m"] = matrix["dim_key_updated"]
    matrix = matrix.drop(columns="dim_key_updated").drop(columns="label")
  
    for i in matrix.columns:
        if (i not in dim_type) & (i not in ['x','k','d','y','m']):
            matrix = matrix.drop(i,axis=1)
    matrix = matrix.drop_duplicates()
    imp = pd.merge(imp,matrix,how="left",on= dim_type,sort=False)
    clk = pd.merge(clk,matrix,how="left",on= dim_type,sort=False)
    #按照5个dimension区分media信息，并添加编号
    if len(imp['x'].isna().sum()) > 0:
        print("Imp and matrix is not 100% matching. Please check.")
    if len(clk['x'].isna().sum()) > 0:
        print("Click and matrix is not 100% matching. Please check.")
            
    imp.to_csv(path_or_buf = "E:/Users/Zuo/Test/05_Model/imp_with_matrix.cxv")
    clk.to_csv(path_or_buf = "E:/Users/Zuo/Test/05_Model/clk_with_matrix.cxv")
    output = imp[["user_log_acct_jm","date","x","k","d","y","m","imp_time"]].groupby(
                ["user_log_acct_jm","date","x","k","d","y","m"],sort=False).agg({'imp_time':'count'}).rename(columns={'imp_time':'exp_cnt'}).reset_index()
    del(imp)
    output["log1p_exp"] = np.log1p(output["exp_cnt"])
        
    #output dimension table
    dim["dimension"] = ""
    dim["dimension"][dim["category"] == "x"] = "level_1"
    dim["dimension"][dim["category"] == "k"] = "level_2"
    dim["dimension"][dim["category"] == "d"] = "level_3"
    dim["dimension"][dim["category"] == "y"] = "level_4"
    dim["dimension"][dim["category"] == "m"] = "level_5"
    dim = dim.rename(columns={'category':'prefix','dim_key_updated':'id'})
        
    dim = dim[["dimension","prefix","label","id"]]
        
    dim.to_csv(path_or_buf = "E:/Users/Zuo/Test/05_Model/impression_digital_map.csv")
    output = pd.merge(output,user_mapping,how = "inner",on="user_log_acct_jm",sort=False)
    output = output.rename(columns={'log1p_exp':'exposure','user_id':'id'})
    output = output.sort_values(['id','date','x','k','d','y','m'])
    output.to_csv(path_or_buf = "E:/Users/Zuo/Test/05_Model/impression_digital.csv")
    return

def fix(level):
    non_media = pd.read_csv(non_media_with_seasonality_path)
    non_media.columns = "data_" + non_media.columns
    if level == "day":
        non_media = non_media[["data_id","data_date","data_target","data_skuid","data_sale_amt"]]
    elif level == "second":
        non_media = non_media[["data_id","data_date","data_target","data_skuid","data_sale_ord_tm","data_sale_amt"]]
    non_media = non_media.rename(columns={non_media.columns[0]:'id',non_media.columns[1]:'date'})
    
    imp = pd.read_csv(impression_digital_path)
    imp["touchpoint"] = imp['x'] + "_" +imp['k']+ "_" +imp['y']+ "_" +imp['d']+ "_" +imp['m']
    imp = imp[["id","touchpoint"]].drop_duplicates()
    imp["mark"] = 1
    touchpoint = imp[["touchpoint"]].drop_duplicates() # impd中有的x-k-d-y-m组合
    imp = pd.merge(imp,non_media[["id"]].drop_duplicates(),how="inner",on="id",sort=False) # 仅保留有购买者的imp
    touchpoint_2 = imp[["touchpoint"]].drop_duplicates() # 购买者对应的impd中的x-k-d-y-m组合
    touchpoint = pd.merge(touchpoint,touchpoint_2,how="outer",on="touchpoint",sort=False, indicator=True)
    touchpoint = touchpoint[touchpoint["_merge"]=="left_only"].drop(columns="_merge")  # touchpoint更新为没有购买的user中所独有的x-k-d-y-m组合
    imp_d = pd.crosstab(index=imp["id"],columns=imp["touchpoint"],values = imp["mark"],aggfunc=len).reset_index()
    del(imp)
    
    non_media = pd.merge(non_media,imp_d,how="left",on="id",sort=False)
    del(imp_d)

    for i in range(non_media.shape[1]):# 将各x-k-d-y-m组合的缺失值修改为0，便于之后相乘
        non_media.iloc[non_media[non_media.iloc[:,i].isna()==True].index.tolist(),i] = 0
    non_media.to_csv(path_or_buf = "E:/Users/Zuo/Test/05_Model/non_media_with_seasonality_blank.csv")

def generate_score(model_input,      # prepared transformed_adstock [storing non_media input and media inputs of diverse dimension]
                   model_equation,   # prepared bma_output [storing beta-hat]
                   bma_models_final, # prepared bma_models_final [storing AIC]
                   level,            # modelling granularity {"day", "second"}
                   target):          # target records of processing {"TRUE" -> target = 1, "FALSE" ->  target = 0}
    #Extract dimensions -->> dimension为X???_K???_D???_Y???_M???的组合
    dimension = model_equation["key"][model_equation["key"].str.contains("X0")]
    #Multiply original exposure by aic
    model_input_final_mid = model_input
    del(model_input)
    model_input_final_mid = pd.concat([model_input_final_mid,pd.DataFrame(columns=('S_' + dimension))],sort=False) # 生成S_X???_K???_D???_Y???_M???用于存储X???_K???_D???_Y???_M???的组合之后的曝光量。注意该曝光量可能与实际曝光量不符，需要后期修正。
    model_input_final_mid_2 = model_input_final_mid.copy() # 创建model_input_final_mid_2的目的在于：model_input_final_mid_2使用其digital_x/k/d/y/m的原始值与aic相乘计算加权平均，确保model_input_final_mid中digital_x/k/d/y/m不被修改
    i=0
    for i in range(bma_models_final.shape[0]):
        match = np.argwhere(model_input_final_mid_2.columns.str.contains(bma_models_final.key[i])==True)
        for j in match: # 对digital_x???/k???/d???/y???/m???加权。权重为对应X/K/D/Y/M的AIC
            model_input_final_mid_2.iloc[:,j] = model_input_final_mid_2.iloc[:,j] * bma_models_final.aic[i]
    colnames = model_input_final_mid_2.columns.tolist()
    for i in range(len(colnames)):
        if "digital_" in colnames[i]:
            colnames[i] = colnames[i].upper()
    model_input_final_mid_2.columns = colnames
    
    col_start = model_input_final_mid.shape[1] - len(dimension)
    remove(i,match)
    #Sum up different dimensions (weigthed by AIC), if one of the media exposure is 0, set the sum to 0
     for i in range(len(np.argwhere(model_input_final_mid_2.columns.str.contains('S_')==True))):
         colnum = np.argwhere(model_input_final_mid_2.columns.str.contains('S_')==True)
         split = model_input_final_mid_2.columns[colnum[i]].str.split('_').tolist()
         split = list(itertools.chain.from_iterable(split))
         split.pop(0)# 获取S_X???_K???_D???_Y???_M???触点对应的X???、K???、D???、Y???，与M???
         # 根据split获取对应X???、K???、D???、Y???，与M???在model_input_final_mid_2中的列序号
         match_col = []
         colnames = model_input_final_mid_2.columns.tolist()
         for j in range(len(colnames)):
             for k in ("DIGITAL_" + pd.Series(split)).tolist():
                 if k in colnames[j]:
                     match_col.append(j)

         model_input_final_mid_2.iloc[:, (col_start + i - 1)] = model_input_final_mid_2.iloc[:,match_col].sum(axis=1)/ bma_models_final.aic.sum() # 完成对于X???_K???_D???_Y???_M???的加权。
         
         model_input_final_mid_2 = 0
         rownum = model_input_final_mid_2[(model_input_final_mid_2.iloc[:,match_col]==0).sum(axis=1) >0].index.tolist()
         model_input_final_mid_2.iloc[rownum,(col_start+i-1)] = 0# 一定程度上修正组合X???、K???、D???、Y???、M???时所造成的实际并未发生的曝光量。因为组合过程中并没有考虑该用户是否在某特定X???_K???_D???_Y???_M???上曝光。
        # 在X???_K???_D???_Y???_M???列大于0并不能表明该用户在该触点上曝光。
        # 上述两句的目的在于X???、K???、D???、Y???、M???中，如果任何一个维度水平上为0，则认为该触点上的实际曝光并不存在。
        # （例如：X001上不存在曝光，那么所有涉及X001的X001_K???_D???_Y???_M???组合都不应该存在曝光）
        # 该操作可以修正一部分曝光，但是无法修复所有的曝光。例如如下情形:
        # 某用户在X001_K002与X002_K001上均被曝光，则transformed_adstock上X001与K002均大于0。
        # 基于上述代码的计算得到的X001_K001、X001_K002、X002_K001、X002_K002这些触点皆被曝光，但实际上X001_K001与X002_K002并没有曝光。
        # 且上述代码不会修正这类情形。因此Ada引入'Correct exposure'过程。但是该过程修复的时候没有考虑订单与曝光的先后顺序，所以存在缺陷。
     
    # 至此初步基于non_media_with_seasonality中X???、K???、D???、Y???，与M???的值，使用各x/k/d/y/m的AIC作为权重，加权计算出S_X???_K???_D???_Y???_M???的模型输入值。
    
    #correct exposure
    model_input_final_mid.iloc[:,col_start:(model_input_final_mid.shape[1])] = (
            model_input_final_mid_2.iloc[:,col_start:(model_input_final_mid.shape[2])]) # 将在model_input_final_mid_2上计算完毕的S_X???_K???_D???_Y???_M???赋予model_input_final_mid
    del (model_input_final_mid,i,match_col,split)
    model_input_final_mid["id"] = pd.to_numeric(model_input_final_mid["id"])
    model_input_final_mid["date"] = pd.to_datetime(model_input_final_mid["date"])
    if level == "day" :
        model_input_final_mid = model_input_final_mid.sort_values(["id","date","data_skuid"])
    elif level = "second":
        model_input_final_mid = model_input_final_mid.sort_values(["id","date","data_sale_ord_tm","data_skuid"])
    model_input_final_mid.columns = model_input_final_mid.columns.str.replace("S_","")
    
    non_media = pd.read_csv("E:/JD_MTA/2018/2018-09-Nestle/05_Model/non_media_with_seasonality_blank.csv")
    if target == True:
        non_media = non_media[non_media["data_target"]==1]
    elif target == False:
        non_media = non_media[non_media["data_target"]==0]
    if level == 'day':
        non_media = non_media.sort_values('id','date','data_skuid')
    elif level == 'second':
        non_media = non_media.sort_values('id','date','data_sale_ord_tm','data_skuid')
    
    if level == 'day':
        for j in range(5,non_media.shape[1]):
            col = non_media.columns[j]
            match = []
            for k in range(len(model_input_final_mid.columns)):
                if col in model_input_final_mid.colums[k]:
                    match.append(k)
            if len(match) != 0 :
                model_input_final_mid.iloc[:,match] = model_input_final_mid.iloc[:,match] * non_media.iloc[:,j]
    elif level == 'second':
        for j in range(6,non_media.shape[1]):
            col = non_media.columns[j]
            match = []
            for k in range(len(model_input_final_mid.columns)):
                if col in model_input_final_mid.colums[k]:
                    match.append(k)
            if len(match) != 0 :
                model_input_final_mid.iloc[:,match] = model_input_final_mid.iloc[:,match] * non_media.iloc[:,j]
    weights = model_equation[['key','weight']]
    weights.loc[weights['key'].str.contains('data_'),'key'] = weights.loc[weights['key'].str.contains('data_'),'key'].str.lower()
    model_input_final_mid_2 = model_input_final_mid.copy()
    for i in range(0,(weights.shape[0]-2)):
        match = []
        for k in range(len(model_input_final_mid_2.columns)):
            if weights['key'][i] in model_input_final_mid_2.columns[k]:
                match.append(k)
        model_input_final_mid_2.iloc[:,match] = model_input_final_mid_2.iloc[:,match] * weights['weight'][i]
        
    we