# Author: Austen Wang
# Date: 2016-04-25

# 读取清洗后的RData数据，存为带表头的.h.csv文件
# Source: /data/SampleDataHero/RData
# Target: /data/SampleDataHero/h.csv。

library(plyr)
library(dplyr)
library(pipeR)
library(magrittr)
library(assertthat)
library(stringr)

path.rdata <- "/data/SampleDataHero/RData"
path.h.csv <- "/data/SampleDataHero/h.csv"

path.rdata.user_log <- "/data/SampleDataHero/RData/user_log"
path.h.csv.user_log <- "/data/SampleDataHero/h.csv/user_log"

write.csv(df.user_info, file = paste(path.h.csv, "user_info.h.csv", sep = "/"))
write.csv(df.user_pay, file = paste(path.h.csv, "user_pay.h.csv", sep = "/"))

write.csv(ls.accid, file = paste(path.h.csv, "accid.h.csv", sep = "/"))
write.csv(ls.roleid, file = paste(path.h.csv, "roleid.h.csv", sep = "/"))


# 开始处理user_log
list.files <- list.files(path.rdata.user_log)
list.path <- list.files(path.rdata.user_log, full.names = TRUE)

ls.df.names <- sapply(list.files, function(x) {
  x %>>%
    strsplit("\\.RData") %>>%
    extract2(1) %>>%
    extract(1)
})

for(n in c(1:length(list.path))) {
  path <- list.path[n]
  df.name <- ls.df.names[n]
  
  load(path)
  write.csv(get(df.name), file = paste0(path.h.csv.user_log, "/", df.name, ".h.csv"))
}
