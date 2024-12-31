# ===
# 轉換Appsflyer PUSH API 的原始json數據
project_name <- 'Appsflyer_DB_Data_Processor'
# ===

# ===== 套件及參數載入 =====
# 載入套件
library(dplyr)          # data manipulation
library(jsonlite)
library(lubridate)
library(mailR)
library(magrittr)       # pipeline
library(parallel)       # 平行化運算
library(plyr)
library(RMySQL)         # mySQL
library(yaml)           # reading config.yml

# 每次最大更新筆數
parser_n <- 30000

# 平行化使用核心數
max_core <- ifelse(floor(detectCores() / 3) == 0, 1, floor(detectCores() / 3))
min_core <- 1
default_core <- 2
use_core <- ifelse(default_core <= max_core & default_core > min_core, use_core, 1)

# 設定檔載入
all_config <- yaml.load_file('C:/git/Appsflyer_PUSH_API_to_DB/all_config.yml')
af_config <- yaml.load_file('C:/git/Appsflyer_PUSH_API_to_DB/af_config.yml')

# 資料庫連線
connect_DB <- dbConnect(RMySQL::MySQL(),
                        host     = all_config$db$host,
                        dbname   = all_config$db$name,
                        dbport   = all_config$db$port,
                        username = all_config$db$user,
                        password = all_config$db$pass)
# 設定MySQL連線編碼
dbSendQuery(connect_DB,'SET NAMES utf8')


# ===== 轉換函式設定 =====
# R NA to SQL NULL函數(SQL value)
NAtoNULL <- function(R_data){
  outputs <- lapply(1:length(R_data), function(col_value){ 
    if (is.na(R_data[col_value])){
      trans_value <- "NULL"
    } else {
      trans_value <- paste0("'", R_data[col_value],"'")
    }
  }) %>%
    call("paste0", ., collapse = ", ") %>% 
    eval %>%
    paste0("(", .,")")
  return(outputs)
}

# 拆解json_data函數，event_value提取
getJsontoDf <- function(af_id_json_data){
  
  # af_id_json_data <- apps_flyer_data[1,]
  
  output <- cbind(as.data.frame(af_id_json_data["id"], stringsAsFactors = FALSE),
                  af_id_json_data["json_data"] %>%
                    str_replace_all(., pattern = "\n", replacement = "") %>%
                    str_replace_all(., pattern = "'", replacement = " ") %>%
                    paste0('[', ., ']') %>% 
                    fromJSON)
  
  if (!output[1, "event_name"] %in% c("install","re-attribution")){
    if (!is.na(output[1, "event_name"])){
      # 處理儲值資料
      if (output[1, "event_name"] == "af_purchase"){
        output %<>% cbind(.,
                          output[1, "event_value"] %>%
                            paste0('[', ., ']') %>%
                            fromJSON)
      }

      # 處理登入資料
      if (output[1, "event_name"] == "af_login"){
        if (output[1, "event_value"] != ""){
          output %<>% cbind(.,
                            output[1, "event_value"] %>%
                              paste0('[', ., ']') %>%
                              fromJSON)
        } else {
          output[1, "serverId"] <- NA
        }
      }
      
      # 處理自訂義事件
      if (!(output[1, "event_name"] %in% c("af_purchase", "af_login"))){
        if (output[1, "event_value"] != ""){
          output %<>% cbind(.,
                            output[1, "event_value"] %>%
                              paste0('[', ., ']') %>%
                              fromJSON)
        } else {
          output[1, "event_value"] <- NA
        }
      }
    }
  }
  return(output)    
}

# 確認是否為json_data
check_json <- function(json) {
  output <- json["json_data"] %>% validate()
}


# ===== 更新範圍確認 =====
# 確認資料庫各表目前最新ID
new_id_SQL <- "SELECT MIN(id) AS id FROM (
                 SELECT MAX(appsflyer_id) AS id FROM appsflyer_install
                 UNION 
                 SELECT MAX(appsflyer_id) AS id FROM appsflyer_login
                 UNION 
                 SELECT MAX(appsflyer_id) AS id FROM appsflyer_purchase
                 UNION 
                 SELECT MAX(appsflyer_id) AS id FROM appsflyer_ad_revenue
               ) AS js 
               UNION 
               SELECT MAX(id) AS id FROM apps_flyer;"
new_id_get <- dbGetQuery(connect_DB, new_id_SQL)
new_id <- data.frame(id = 1:2)
# 最大最小id一樣時，union只會有一值導致後面程式出錯，這邊統一給一樣的id
new_id$id[1] <- ifelse(is.na(new_id_get$id[1]), 1, new_id_get$id[1])
new_id$id[2] <- ifelse(is.na(new_id_get$id[2]), new_id_get$id[1],new_id_get$id[2])
print(paste("執行開始appsflyer id:", new_id$id[1], ", 結束appsflyer id:", new_id$id[2]))
# 執行id
run_id <- c(new_id$id[1]:new_id$id[2]) %>% unique %>% sort


# ===== 更新範圍確認 =====
json_error <- data.frame()
error_1.0 <- data.frame()

# i <- 1
if (length(run_id) > 0){
  
  for (i in seq(1, length(run_id), by = parser_n)){
    start_ind <- i
    end_ind <- i - 1 + parser_n
    if (end_ind > length(run_id)){ 
      end_ind <- length(run_id)
    }
    
    # apps_flyer_SQL
    # 新增2.0版本判斷(event_type)，2.0版本event_type被拔除 (如果造成效能不佳可把條件刪除) (拔除後如果有1.0資料進來拆json那會有error)
    apps_flyer_SQL <- sprintf("SELECT id, json_data FROM apps_flyer 
                              WHERE id BETWEEN %d AND %d and json_data not like '%%event_type%%' and event_name != 'reinstall';", 
                              run_id[start_ind], run_id[end_ind])
    apps_flyer_data <- dbGetQuery(connect_DB, apps_flyer_SQL)
    
    # 只篩選是json格式的資料，紀錄非json的資料
    apps_flyer_data$check_json <- apply(apps_flyer_data,1,check_json)
    apps_flyer_json_error <- apps_flyer_data %>% filter(check_json == FALSE) %>% select(-c(check_json))
    apps_flyer_data %<>% filter(check_json == TRUE)
    json_error <- rbind(json_error, apps_flyer_json_error)
    
    # 紀錄沒被拆解的資料(1.0版本, 非json格式)
    apps_flyer_1.0_sql<- sprintf("SELECT id, json_data FROM apps_flyer 
                              WHERE id BETWEEN %d AND %d and json_data like '%%event_type%%' and event_name != 'reinstall';", 
                              run_id[start_ind], run_id[end_ind])
    apps_flyer_1.0 <- dbGetQuery(connect_DB, apps_flyer_1.0_sql)
    error_1.0 <- rbind(error_1.0, apps_flyer_1.0)
    
    # 有資料才拆解json
    if (nrow(apps_flyer_data) > 0){
      
      # ===== 1. 平行化拆解json =====
      # use_core <- ifelse(nrow(apps_flyer_data) >= use_core, user, nrow(apps_flyer_data))
      cl <- makeCluster(use_core)
      clusterEvalQ(cl = cl, {library(jsonlite); library(stringr); library(plyr); library(dplyr); library(magrittr)})
      clusterExport(cl, varlist = list("getJsontoDf", "apps_flyer_data"), envir = environment())
      appsflyer_data <- parApply(cl, apps_flyer_data, 1, getJsontoDf) %>%
        rbind.fill %>% 
        plyr::rename(c("appsflyer_id" = "appsflyer_device_id"
                       ,'af_id_json_data["id"]' = "appsflyer_id"
                       , "event_time" = "time"
                       , "af_prt" = "agency"
                       , "af_attribution_lookback" = "af_click_lookback"
                       , "retargeting_conversion_type" = "re_targeting_conversion_type"
                       , "af_cost_currency" = "currency"
                       , "attributed_touch_time_selected_timezone" = "click_time_selected_timezone"
                       , "attributed_touch_time" = "click_time"
                       , "device_download_time" = "download_time"
                       , "original_url" = "click_url"
                       , "af_cost_value" = "cost_per_install"
                       ))
      # 停止
      stopCluster(cl)
      print(sprintf('拆解完json_data，共%s筆資料', nrow(appsflyer_data)))
      
      # 檢查錯誤是在哪一列資料
      # for (p in 1:nrow(apps_flyer_data)) {
      #   print(p)
      #   print(apps_flyer_data[p,])
      #   output <- getJsontoDf(apps_flyer_data[p,])
      # }
      
      # 2.0版本刪除的欄位加回來
      appsflyer_data$fb_campaign_name <- NA
      appsflyer_data$fb_campaign_id <- NA
      appsflyer_data$fb_adset_name <- NA
      appsflyer_data$fb_adset_id <- NA
      appsflyer_data$fb_adgroup_name <- NA
      appsflyer_data$fb_adgroup_id <- NA
      appsflyer_data$device_brand <- NA
      # 如果沒有device_model才建立空欄位
      if ("device_model" %in% colnames(appsflyer_data))
      { } else {
        appsflyer_data$device_model <- NA
      }
      # 如果沒有device_type才建立空欄位
      if ("device_type" %in% colnames(appsflyer_data))
      { } else {
        appsflyer_data$device_type <- NA
      }
      # 如果沒有af_channel才建立空欄位
      if ("af_channel" %in% colnames(appsflyer_data))
      { } else {
        appsflyer_data$af_channel <- NA
      }
      
      # 將media_source, af_channel, af_c_id, campaign & agency 強制轉成小寫
      appsflyer_data$media_source <- appsflyer_data$media_source %>% tolower
      appsflyer_data$af_channel <- appsflyer_data$af_channel %>% tolower
      appsflyer_data$af_c_id <- appsflyer_data$af_c_id %>% tolower
      appsflyer_data$campaign <- appsflyer_data$campaign %>% tolower
      if (sum(names(appsflyer_data) == "agency") == 1){
        appsflyer_data$agency <- appsflyer_data$agency %>% tolower
      } else {
        appsflyer_data$agency <- NA
      }
      
      # 將wifi, is_primary_attribution  (TRUE,FALSE) 轉換成 (1,0)
      appsflyer_data$wifi %<>% as.logical()
      appsflyer_data$wifi <- ifelse(appsflyer_data$wifi == TRUE, 1, 0)
      appsflyer_data$is_primary_attribution %<>% as.logical()
      appsflyer_data$is_primary_attribution <- ifelse(is.na(appsflyer_data$is_primary_attribution),0, ifelse(appsflyer_data$is_primary_attribution==TRUE,1,0))
      
      # 將media_source為restricted的資料，campaign改為restricted，media_source改為facebook ads
      appsflyer_data$campaign[which(appsflyer_data$media_source=='restricted')] <- 'restricted'
      appsflyer_data$af_c_id[which(appsflyer_data$media_source=='restricted')] <- 'restricted'
      appsflyer_data$media_source[which(appsflyer_data$media_source=='restricted')] <- 'facebook ads'
      
      # 將facebook資訊從af_c_id等補到fb_campaign_id等
      appsflyer_data$fb_campaign_name[which(appsflyer_data$media_source=='facebook ads')] <- appsflyer_data$campaign[which(appsflyer_data$media_source=='facebook ads')]
      appsflyer_data$fb_campaign_id[which(appsflyer_data$media_source=='facebook ads')] <- appsflyer_data$af_c_id[which(appsflyer_data$media_source=='facebook ads')]
      appsflyer_data$fb_adset_name[which(appsflyer_data$media_source=='facebook ads')] <- appsflyer_data$af_adset[which(appsflyer_data$media_source=='facebook ads')]
      appsflyer_data$fb_adset_id[which(appsflyer_data$media_source=='facebook ads')] <- appsflyer_data$af_adset_id[which(appsflyer_data$media_source=='facebook ads')]
      appsflyer_data$fb_adgroup_name[which(appsflyer_data$media_source=='facebook ads')] <- appsflyer_data$af_ad[which(appsflyer_data$media_source=='facebook ads')]
      appsflyer_data$fb_adgroup_id[which(appsflyer_data$media_source=='facebook ads')] <- appsflyer_data$af_ad_id[which(appsflyer_data$media_source=='facebook ads')]
      
      # 裝置資訊分成 device_type(iOS), device_model(Android), device_brand(Android廠牌)
      # 裝置資訊有些是放在device_type、devie_model, PUSH API 2022/2/2後都放在device_model
      # Android
      appsflyer_data$device_brand[which(appsflyer_data$platform=='android'&!is.na(appsflyer_data$device_type))] <- gsub("::.*$","",appsflyer_data$device_type[which(appsflyer_data$platform=='android'&!is.na(appsflyer_data$device_type))])
      appsflyer_data$device_model[which(appsflyer_data$platform=='android'&!is.na(appsflyer_data$device_type))] <- gsub("^.*::","",appsflyer_data$device_type[which(appsflyer_data$platform=='android'&!is.na(appsflyer_data$device_type))])
      appsflyer_data$device_brand[which(appsflyer_data$platform=='android'&is.na(appsflyer_data$device_type))] <- gsub("::.*$","",appsflyer_data$device_model[which(appsflyer_data$platform=='android'&is.na(appsflyer_data$device_type))])
      appsflyer_data$device_model[which(appsflyer_data$platform=='android'&is.na(appsflyer_data$device_type))] <- gsub("^.*::","",appsflyer_data$device_model[which(appsflyer_data$platform=='android'&is.na(appsflyer_data$device_type))])
      appsflyer_data$device_type[which(appsflyer_data$platform=='android')] <- NA
      # IOS
      appsflyer_data$device_type[which(appsflyer_data$platform=='ios'& !is.na(appsflyer_data$device_model))] <- appsflyer_data$device_model[which(appsflyer_data$platform=='ios' & !is.na(appsflyer_data$device_model))]
      appsflyer_data$device_model[which(appsflyer_data$platform=='ios')] <- NA
      
      # JSON轉dataframe的函數會把NA字串轉成R的NA，目前還沒找到比較好的方案，先把最常出現的region做特別處理
      appsflyer_data$region[which(is.na(appsflyer_data$region))] <- "NA"
      
      # 將欄位空白值變成R的NA，如果只有一列會被轉置，需要轉回來
      if (nrow(appsflyer_data) == 1) {
        appsflyer_data %<>% apply(2, function(x){
          x[which(x == "")] <- NA
          return(x)
        }) %>% t()%>% as.data.frame(stringsAsFactors = FALSE)
      } else {
        appsflyer_data %<>% apply(2, function(x){
          x[which(x == "")] <- NA
          return(x)
        }) %>% as.data.frame(stringsAsFactors = FALSE)
      }

      # 補足8小時時差
      appsflyer_data$time %<>% ymd_hms
      hour(appsflyer_data$time) <- hour(appsflyer_data$time) + 8
      appsflyer_data$time <- as.character(appsflyer_data$time)
      
      # install purchase login mobile_custom 筆數
      install_n <- appsflyer_data %>% filter(event_name == "install") %>% nrow
      purchase_n <- appsflyer_data %>% filter(event_name == "af_purchase") %>% nrow
      login_n <- appsflyer_data %>% filter(event_name == "af_login") %>% nrow
      ad_revenue_n <- appsflyer_data %>% filter(event_name == "RewardedAd_Send_reward") %>% nrow #如果廣告營收事件有新增其他廣告公司這邊要新增
      print(sprintf("新的安裝:%s筆, 新的儲值:%s筆, 新的登入:%s筆, 新的廣告收益:%s筆", install_n, purchase_n, login_n, ad_revenue_n))
      
      # 補足8小時時差
      # 自然流量沒有click_time，只有單一筆會沒有click_time欄位導致錯誤
      if (c("click_time") %in% names(appsflyer_data)) {
        appsflyer_data$click_time %<>% ymd_hms
        hour(appsflyer_data$click_time) <- hour(appsflyer_data$click_time) + 8
        appsflyer_data$click_time <- as.character(appsflyer_data$click_time)
      } else {
        appsflyer_data$click_time <- NA
      }
      
      appsflyer_data$download_time %<>% ymd_hms
      hour(appsflyer_data$download_time) <- hour(appsflyer_data$download_time) + 8
      appsflyer_data$download_time <- as.character(appsflyer_data$download_time)
      
      appsflyer_data$install_time %<>% ymd_hms
      hour(appsflyer_data$install_time) <- hour(appsflyer_data$install_time) + 8
      appsflyer_data$install_time <- as.character(appsflyer_data$install_time)
      
      # ===== 2. appsflyer 安裝數據匯入DB =====
      if (install_n > 0){
        # 組合SQL語法
        install_fill_column <- data.frame(matrix(rep(NA, length(af_config$install_column_name)), nrow = 1, ncol = length(af_config$install_column_name)), 
                                          stringsAsFactors = FALSE) %>% slice(0)
        
        names(install_fill_column) <- af_config$install_column_name
        
        install_values <- install_fill_column %>% 
          rbind.fill(., appsflyer_data %>% 
                       filter(event_name == "install")) %>%
          `[`(af_config$install_column_name) %>%
          apply(., 1, NAtoNULL) %>%
          paste0(., collapse = ",")
        
        # appsflyer_install 寫入DB
        install_SQL <- sprintf("INSERT appsflyer_install (%s) VALUES ", 
                               paste0(af_config$install_column_name, collapse = ", "))
        
        # ON DUPLICATE KEY UPDATE 組字串
        DUPLICATE_KEY_UPDATE_SQL <- names(install_fill_column) %>% paste0(" = VALUES(",.,")") %>% 
          paste0(names(install_fill_column),.) %>%
          paste0(collapse = " , ") %>% 
          paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
        
        # 蓋掉除了PK以外的資料
        insert_install_SQL <- paste0(install_SQL, install_values, DUPLICATE_KEY_UPDATE_SQL)
        dbSendQuery(connect_DB, insert_install_SQL)
        print(paste0(exe_datetime, " apps_flyer_install ", run_id[start_ind], " ", run_id[end_ind], " ", Sys.time()))
      }
      
      # ===== 3. appsflyer 儲值數據匯入DB =====
      if (purchase_n > 0){
        
        # 建立purchase欄位
        purchase_fill_column <- data.frame(matrix(rep(NA, length(af_config$purchase_column_name)), nrow = 1, ncol = length(af_config$purchase_column_name)), 
                                           stringsAsFactors = FALSE) %>% slice(0)
        names(purchase_fill_column) <- af_config$purchase_column_name
        
        # ===== 3-1. appsflyer_purchase =====
        # 組合SQL語法
        purchase_values <- purchase_fill_column %>% 
          rbind.fill(., appsflyer_data %>%
                       filter(event_name == "af_purchase" & is.na(re_targeting_conversion_type))) %>%
          `[`(af_config$purchase_column_name) %>%
          apply(., 1, NAtoNULL) %>%
          paste0(., collapse = ",")
        
        if (purchase_values != '') {
          # appsflyer_purchase 寫入DB
          purchase_SQL <- sprintf("INSERT appsflyer_purchase (%s) VALUES ", 
                                 paste0(af_config$purchase_column_name, collapse = ", "))
          
          # ON DUPLICATE KEY UPDATE 組字串
          DUPLICATE_KEY_UPDATE_SQL <- names(purchase_fill_column) %>% paste0(" = VALUES(",.,")") %>% 
            paste0(names(purchase_fill_column),.) %>%
            paste0(collapse = " , ") %>% 
            paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
          
          # 蓋掉除了PK以外的資料
          insert_purchase_SQL <- paste0(purchase_SQL, purchase_values, DUPLICATE_KEY_UPDATE_SQL)
          dbSendQuery(connect_DB, insert_purchase_SQL)
          print(paste0(exe_datetime, " apps_flyer_purchase ", run_id[start_ind], " ", run_id[end_ind], " ", Sys.time()))
        }
       }
      
      # ===== 4. appsflyer 登入數據匯入DB =====
      if (login_n > 0){
        # 建立login欄位
        login_fill_column <- data.frame(matrix(rep(NA, length(af_config$login_column_name)), nrow = 1, ncol = length(af_config$login_column_name)), 
                                        stringsAsFactors = FALSE) %>% slice(0)
        names(login_fill_column) <- af_config$login_column_name
        
        # ===== 4-1. appsflyer_login =====
        # 組合SQL語法
        login_values <- login_fill_column %>% 
          rbind.fill(., appsflyer_data %>% 
                       filter(event_name == "af_login" & is.na(re_targeting_conversion_type))) %>%
          `[`(af_config$login_column_name) %>%
          apply(., 1, NAtoNULL) %>%
          paste0(., collapse = ",")
        
        if (login_values != '') {
          # appsflyer_login 寫入DB
          login_SQL <- sprintf("INSERT appsflyer_login (%s) VALUES ", 
                                 paste0(af_config$login_column_name, collapse = ", "))
          
          # ON DUPLICATE KEY UPDATE 組字串
          DUPLICATE_KEY_UPDATE_SQL <- names(login_fill_column) %>% paste0(" = VALUES(",.,")") %>% 
            paste0(names(login_fill_column),.) %>%
            paste0(collapse = " , ") %>% 
            paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
          
          # 蓋掉除了PK以外的資料
          insert_login_SQL <- paste0(login_SQL, login_values, DUPLICATE_KEY_UPDATE_SQL)
          dbSendQuery(connect_DB, insert_login_SQL)
          print(paste0(exe_datetime, " apps_flyer_login ", run_id[start_ind], " ", run_id[end_ind], " ", Sys.time()))
        }
      }
      
      # ===== 5. appsflyer 廣告收益數據匯入DB =====
      if (ad_revenue_n > 0){
        # 建立ad_revenue欄位
        ad_revenue_fill_column <- data.frame(matrix(rep(NA, length(af_config$ad_revenue_column_name)), nrow = 1, ncol = length(af_config$ad_revenue_column_name)), 
                                       stringsAsFactors = FALSE) %>% slice(0)
        names(ad_revenue_fill_column) <- af_config$ad_revenue_column_name
        
        # ===== 5-1. appsflyer_ad_revenue =====
        # 組合SQL語法
        ad_revenue_values <- ad_revenue_fill_column %>% 
          rbind.fill(., appsflyer_data %>% 
                       filter(event_name == "RewardedAd_Send_reward" & is.na(re_targeting_conversion_type))) %>%
          `[`(af_config$ad_revenue_column_name) %>%
          apply(., 1, NAtoNULL) %>%
          paste0(., collapse = ",")
        
        if (ad_revenue_values != '') {
          # appsflyer_ad_revenue 寫入DB
          ad_revenue_SQL <- sprintf("INSERT appsflyer_ad_revenue (%s) VALUES ", 
                              paste0(af_config$ad_revenue_column_name, collapse = ", "))
          
          # ON DUPLICATE KEY UPDATE 組字串
          DUPLICATE_KEY_UPDATE_SQL <- names(ad_revenue_fill_column) %>% paste0(" = VALUES(",.,")") %>% 
            paste0(names(ad_revenue_fill_column),.) %>%
            paste0(collapse = " , ") %>% 
            paste0(" ON DUPLICATE KEY UPDATE ",.,";") 
          
          # 蓋掉除了PK以外的資料
          insert_ad_revenue_SQL <- paste0(ad_revenue_SQL, ad_revenue_values, DUPLICATE_KEY_UPDATE_SQL)
          dbSendQuery(connect_DB, insert_ad_revenue_SQL)
          print(paste0(exe_datetime, " appsflyer_ad_revenue ", run_id[start_ind], " ", run_id[end_ind], " ", Sys.time()))
        }
      }
      

    }
    print(paste0(exe_datetime," apps_flyer finish ", run_id[start_ind], " ", run_id[end_ind], " ", Sys.time()))
  }
  
  
  # ===== 錯誤通知 =====
  # 錯誤通知，JSON格式異常
  if (nrow(json_error) > 0){
    json_error_data <- json_error %>% apply(., 1, paste0, collapse = "~~") %>% paste0(., collapse = "\n\n")
    json_error_data_name <- names(json_error) %>% paste0(., collapse = "~") %>% paste0(., "\n\n")
    
    # 寄信通知
    send.mail(from = all_config$mail$from,
              to = all_config$mail$to,
              subject = sprintf("apps_flyer 轉換異常，跳過轉換數據%s筆 - %s", nrow(json_error), as.character(Sys.time())),
              body =   sprintf("json格式錯誤共%s筆 \n%s%s", nrow(json_error), json_error_data_name, json_error_data),
              encoding = "utf-8",
              smtp = list(host.name = "aspmx.l.google.com", port = 25),
              authenticate = FALSE,
              send = TRUE)
    print(sprintf('%s 異常資料%s筆，已寄信通知', exe_datetime, nrow(json_error)))
  }
  
  # 錯誤通知，收到1.0舊版資料、有問題每天上班前9點通知一次
  if (nrow(error_1.0) > 0 & hour(Sys.time()) == 9){
    error_1.0_data <- error_1.0 %>% apply(., 1, paste0, collapse = "~~") %>% paste0(., collapse = "\n\n")
    error_1.0_data_name <- names(error_1.0) %>% paste0(., collapse = "~") %>% paste0(., "\n\n")
    
    # 寄信通知
    send.mail(from = all_config$mail$from,
              to = all_config$mail$to,
              subject = sprintf("apps_flyer 轉換異常，跳過轉換數據%s筆 - %s", nrow(error_1.0), as.character(Sys.time())),
              body =   sprintf("版本錯誤(1.0版本)共%s筆: \n%s%s", nrow(error_1.0), error_1.0_data_name, error_1.0_data),
              encoding = "utf-8",
              smtp = list(host.name = "aspmx.l.google.com", port = 25),
              authenticate = FALSE,
              send = TRUE)
    print(sprintf('%s 異常資料%s筆，已寄信通知', exe_datetime, nrow(error_1.0)))
  }
}
