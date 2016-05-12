# Author: Austen Wang
# Date: 2016-04-22

# 输入数据源（原始数据）有23种logtype，每个logtype有相应的字段。
# logtype分为三大类，分别是 userpay, userinfo 和userlog，其中userlog有21种

# 此处定义的函数代码仅涉及数据加工/转换规则，不涉及数据的筛选，也不涉及错误排查。

library(plyr)
library(dplyr)
library(pipeR)
library(magrittr)
library(assertthat)
library(stringr)
library(foreach)
library(data.table)

# 付费增强表计算

## 版本1. 从历史动作数据中计算出付费增强表
## 涉及到的数据块包括：
## today：今天的日期；
## df.pay: 所有的付费日志；
## df.login: 所有用户的所有登录记录（用于判断活跃天数）。
## df.user_info: 所有用户的所有注册记录（用于计算距离首次付费的天数）。

# Section1.1 载入数据
today <- as.Date("2015-12-04")
load(file = "/data/SampleDataHero/RData/df.user_pay.RData")
df.user_pay %>>% asas.data.table() -> dt.user_pay

# 付费动作按roleid，时间来排序
dt.user_pay <- arrange(dt.user_pay, roleid, timestamp)

ls.roleid.paid <- dt.user_pay$roleid %>>% unique()
ls.accid.paid <- dt.user_pay$accid %>>% unique()

# 载入user_info表，并过滤掉没有付费的角色
load("/data/SampleDataHero/RData/df.user_info.RData")

df.user_info %>>%
  select(userid, accid, rolename, craetetime) %>>%
  as.data.table() -> dt.user_info

names(dt.user_info) <- 
  c("roleid", "accid", "role_name_user_info", "createtime")

# 载入登录动作表 df.login, 过滤掉没有付费的角色；
load("/data/SampleDataHero/RData/Processing/df.account_login.RData")
df.account_login %>>%
  select(accid, timestamp) %>>%
  filter(accid %in% ls.accid.paid) %>>%
  arrange(accid, timestamp) %>>%
  as.data.table() -> dt.account_login


# left_join(dt.user_pay, dt.user_info, by = "roleid") -> dt.user_pay.enhanced.roleid
# left_join(dt.user_pay, dt.user_info, by = "accid") -> dt.user_pay.enhanced.accid

left_join(dt.user_pay, dt.user_info, by = "roleid") -> dt.user_pay.enhanced

# 计数这是第几次付费，还找不到好的“函数式编程”办法，只好采用面向过程的思路
## 面向过程的方法是从头往后数，如果和上一个roleid一样，就在上一个基础上+1，否则设置为1
ls.roleid <- dt.user_pay$roleid
n <- length(ls.roleid)
ls.nth <- rep(1,n)

for(i in c(2:n)) {
  if(ls.roleid[i] == ls.roleid[i-1])
    ls.nth[i] = ls.nth[i-1] + 1
  else
    ls.nth[i] = 1
}

nth_pay <- ls.nth
cbind(dt.user_pay.enhanced, nth_pay) -> dt.user_pay.enhanced

delta.date <- function(ts_start, ts_end) {
  date_start <- as.Date(ts_start)
  date_end <- as.Date(ts_end)
  (date_end - date_start) %>>% 
    as.integer() %>>%
    add(1)
}

dt.user_pay.enhanced <- dplyr::mutate(
  dt.user_pay.enhanced, 
  nth_days_register = delta.date(createtime, timestamp))



# path.rawlog <- "/data/SampleDataHero/RData/user_log"
# ls.filepath.acclogin <- list.files(path.rawlog, pattern = 'account_login', 
#                                    recursive = TRUE, full.names = TRUE)
# ls.filename.acclogin <- list.files(path.rawlog, pattern = 'account_login', recursive = TRUE)
# sapply(ls.filename.acclogin, function(x) {
#   x %>>%
#     sub(pattern = "\\.RData", replacement = "") %>>%
#     extract2(1)
# }) -> ls.str.df.names 
# 
# ldply(ls.filepath.acclogin, function(x) {
#   x %>>%
#     strsplit("\\/") %>>%
#     extract2(1) %>>% 
#     extract(6) %>>%
#     sub(pattern = "\\.RData", replacement = "") %>>%
#     extract2(1) -> str.df.name
#   load(x)
#   get(str.df.name)
#   }) -> df.account_login
# 
# # save(df.account_login, file = "/data/SampleDataHero/RData/Processing/df.account_login.RData")
# 
# ## 载入注册日志
# ls.filepath.create_role <- list.files(path.rawlog, pattern = 'create_role',
#                                            recursive = TRUE, full.names = TRUE)
# ldply(ls.filepath.create_role, function(x) {
#   x %>>%
#     strsplit("\\/") %>>%
#     extract2(1) %>>% 
#     extract(6) %>>%
#     sub(pattern = "\\.RData", replacement = "") %>>%
#     extract2(1) -> str.df.name
#   load(x)
#   get(str.df.name)
# }) -> df.create_role
# 
# save(df.create_role, file = "/data/SampleDataHero/RData/Processing/df.create_role.RData")
# 
# 

# 
# nth_pay <- ls.nth
# df.user_pay.enhanced <- cbind(df.user_pay, nth_pay)
# 
# df.date.role.register <- select(df.create_role, roleid, timestamp)
# names(df.date.role.register) <- c("roleid", "register_ts")
# row.names(df.date.role.register) <- df.date.role.register$roleid
# ls.roleid <- df.user_pay$roleid
# 
# merge(df.user_pay.enhanced, df.date.role.register, 
#       by = "roleid", all.x = TRUE) -> df.user_pay.enhanced
# 
# # 直接这么操作不行，报错说 cannot allocate vector of size 148.8Gb
# # sapply(ls.roleid, function(role) {
# #   df.date.role.register[role, "register_ts"]
# # }) -> ls.first_pay_time


