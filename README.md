# Appsflyer_DB_Data_Processor
### 專案簡介 | Project Overview
將 Appsflyer PUSH API 即時寫入資料庫的用戶行為資料進行處理，轉換為結構化的各類表格，如安裝、登入、儲值、廣告事件等。  
程式採用平行化處理技術，可高效處理大量數據，提升 JSON 解析與異常資料偵測的效率。

Processes data written to the database from Appsflyer PUSH API.  
Converts raw data into structured tables, such as installations, logins, payments, and ad events.  
Utilizes parallel processing for efficient handling of large datasets, including JSON parsing and abnormal data detection.

---

### 主要目的 | Main Purpose
將 Appsflyer 原始數據轉換為各種 event_name 對應的表格，並進行欄位篩選與數據處理。

Convert Appsflyer raw data into corresponding tables for each event_name, with field filtering and data processing.

---

### 各檔案描述 | File Descriptions
- **Appsflyer_DB_Data_Processor.py**  
  1. 確認資料庫中最新的已更新流水號。  
     Verify the latest updated serial number in the database.  
  2. 拆解 API 資訊 JSON 檔案，確認版本並進行偵錯。  
     Parse the API JSON file to confirm the version and debug issues.  
  3. 啟用平行化處理，將 JSON 轉換為 DataFrame。  
     Enable parallel processing to convert JSON into a DataFrame.  
  4. 執行資料處理並進行欄位篩選。  
     Perform data processing and field selection.  
  5. 將處理後的資料寫入資料庫。  
     Write the processed data into the database.  


- **af_config.yml**  
  Appsflyer 欄位相關的配置。  
  Configuration file for Appsflyer field settings.
- **all_config**
  包含資料庫設定與電子郵件通知設定。  
  Contains database configurations and email notification settings.
