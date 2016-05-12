# Author: Austen Wang
# Date: 2016-04-19

# 读取原始数据，转换为R语言的.RData格式，为下次使用做准备
# Source: /data/SampleDataHero 下的数据范例
# Target: 输出清洗后的数据，最终完成 付费增强表、画像表和KPI指标。

library(plyr)
library(dplyr)
library(pipeR)
library(magrittr)
library(assertthat)
library(stringr)

setwd("~/ldp/hero/bin/r-scripts")

# 载入metadata，获取日志格式的定义
load("../../conf/metadata/metadata.RData")

# 数据出了问题，重新加工一下
meta.logtype <- read_excel("../../conf/metadata/crisis.action.metadata.rdb.xlsx", sheet = "logtype")
meta.logtype$log_type %>% tolower() -> meta.logtype$log_type
meta.logtype[21,5] <- 'timestamp,logtype,roleid,accid,roomid,roomtype,battle_diamond_used'
# 从11-28日起，该日志 rolebattleprev 中途退出战斗，从7列变成9列
meta.logtype[21,5] <- 'timestamp,logtype,roleid,accid,roomid,roomtype,battle_diamond_used,roundid,roundrestrictionid'
meta.logtype[5,5] <- 'timestamp,logtype,roleid,accid,roleprice,roleid_got,roleid_got_reason'
meta.logtype[20,5] <- 'timestamp,logtype,roleid,accid,roomid,battletype,roundid,killnumber,bigkillnumber,deathnumber,battleduration,battle_diamond_used,battlecamp,battleresult'
# 从12-01日起，该日志 rolebattle完成战斗，开始有15列，多了最后一列
meta.logtype[20,5] <- 'timestamp,logtype,roleid,accid,roomid,battletype,roundid,killnumber,bigkillnumber,deathnumber,battleduration,battle_diamond_used,battlecamp,battleresult,roundrestrictionid'

data.frame(meta.logtype$id, 
           meta.logtype$log_type, 
           meta.logtype$log_type_cn, 
           meta.logtype$fields_list) -> df.tmp
meta.logtype <- df.tmp
remove(df.tmp)
colnames(meta.logtype) <- c("id", "log_type", "log_type_cn", "fields_list")
row.names(meta.logtype) <- meta.logtype$log_type
meta.logtype$log_type %>% as.character() -> meta.logtype$log_type
meta.logtype$log_type_cn %>% as.character() -> meta.logtype$log_type_cn
meta.logtype$fields_list %>% as.character() -> meta.logtype$fields_list



# 读取user_info 数据
str_logtype <- "user_info"

path.data <- "/data/SampleDataHero"
path.user_info <- "/data/SampleDataHero/user_info/wh_105_user_info_20151204_server_5_active.txt"

df.user_info <- read.csv(path.user_info, header = F, sep = '\t')

meta.logtype[str_logtype, "fields_list"] %>% 
  strsplit(',') %>% 
  extract2(1) -> names(df.user_info)
# save(df.user_info, file = "/data/SampleDataHero/RData/df.user_info.RData")

ls.accid <- df.user_info$accid
ls.userid <- df.user_info$userid
ls.roleid <- ls.userid
# save(ls.accid, file="/data/SampleDataHero/RData/ls_accid.RData")
# save(ls.userid, file="/data/SampleDataHero/RData/ls_userid.RData")

# 读取user_pay 数据
str.logtype <- "user_pay"
path.user_pay <- "/data/SampleDataHero/user_pay"

# 读取user_pay文件名列表
ls.filepath.user_pay <- list.files(path.user_pay, recursive = TRUE, full.names = TRUE)


# 读取所有96天的付费日志，共计323万条
ldply(ls.filepath.user_pay, function(filepath) {
  read.csv(filepath, header = FALSE, sep = '\t')
}) -> df.user_pay.all

meta.logtype[str_logtype, "fields_list"] %>% 
  strsplit(',') %>% 
  extract2(1) -> names(df.user_pay.all)


df.user_pay.all %>% 
  subset(serverid == '5') %>%
  subset(accid %in% ls.accid) -> df.user_pay
# save(df.user_pay, file = "/data/SampleDataHero/RData/df.user_pay.RData")

# intersect(df.user_pay$roleid, ls.userid) -> tmp1
# intersect(df.user_pay$accid, ls.accid) -> tmp2

# 读取所有user_log数据，需要处理21种不同格式的数据
# 96天的数据，最终需要读到96*21个df里面，形如 df.account_login.2015-08-29

path.user_log <- "/data/SampleDataHero/user_log/ripe"
path.user_log.output <- "/data/SampleDataHero/RData/user_log"

if(!file.exists(path.user_log.output)) {
  dir.create(path.user_log.output)
}

ls.filepath.user_log <- list.files(path.user_log, recursive = TRUE, full.names = TRUE)

df.logfile <- data.frame(ls.filepath.user_log)
names(df.logfile) <- c("filepath")
df.logfile$filepath %>>% as.character() -> df.logfile$filepath

df.logfile$filesize <- sapply(df.logfile$filepath, file.size)

df.logfile$filetype <- sapply(df.logfile$filepath, function(x) {
  x %>>%
    strsplit("log\\/5\\/|\\.txt") %>>%
    extract2(1) %>>%
    extract(2)
})

df.logfile$ymd <- sapply(df.logfile$filepath, function(x) {
  x %>>%
    strsplit("wh_105\\/|\\/user_log") %>>%
    extract2(1) %>>%
    extract(3) %>%
    gsub("\\/", "-", .)
})

df.logfile$df.name <- paste("df", df.logfile$filetype, df.logfile$ymd, sep = ".")
df.logfile$file.name.output <- paste0(df.logfile$df.name, ".RData")
df.logfile$file.path.output <- paste0(path.user_log.output, "/", df.logfile$file.name.output )

df.logfile %>>%
  dplyr::filter(filesize > 0) -> df.logfile.queued

number <- nrow(df.logfile.queued)
# number = 15
for(n in c(1:33)) {
  line <- df.logfile.queued[n, ]
  
  df.name <- line$df.name %>% as.character()
  paste("No.", n, line$filepath) %>>% print()
  
  if(line$filetype == "battlestart") {
    paste("The battlestart log", df.name, "has neither accid or roleid") %>% print()
    next;
  }
  df <- read.csv(line$filepath, header = FALSE, sep = '\t')
  
  paste("No.", n, "date =", line$ymd, "type =", line$filetype, "total", nrow(df), "lines.") %>% print()
        
  meta.logtype[line$filetype, "fields_list"] %>>% 
    as.character() %>>%
    strsplit(',') %>>% 
    extract2(1) -> names
  
  names -> names(df)
  
  if("accid" %in% names(df)) {
    dplyr::filter(df, accid %in% ls.accid) -> df.accid
    assign(df.name, df.accid)    
  }

  if("roleid" %in% names(df)) {
    dplyr::filter(df, roleid %in% ls.roleid) -> df.roleid
    assign(df.name, df.roleid)    
  }
  
  if("accid" %in% names(df) && "roleid" %in% names(df))
    assert_that(nrow(df.accid) == nrow(df.roleid))
  
  ## 开始保存数据
  paste("Save file", df.name, "which", nrow(get(df.name)), " lines.") %>% print
  
  save(list = c(df.name), file = line$file.path.output)
  remove(list = c(df.name))
  remove(df)
  
}

  

