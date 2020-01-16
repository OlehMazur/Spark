// Databricks notebook source
// MAGIC %run /Users/o_mazur@carlsberg.ua/dcd_etl_functions

// COMMAND ----------

//util variables

// COMMAND ----------

val job = "dcd_notebook_workflow_hfa_All"
val notebook = "df_bal"
val notebook_start_time  = LocalDateTime.now
val fc_acc_week  = "fc_acc_week.csv" 
val fc_acc_month = "fc_acc_month.csv"
val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/" + job
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp/" +job

// COMMAND ----------

//Init log DB  

// COMMAND ----------

// MAGIC %sql use etl_info

// COMMAND ----------

save_to_log_first_step(job,notebook)

// COMMAND ----------

//log for parameters 

// COMMAND ----------

save_to_log_parameters_value (job,notebook, "type_of_ETL", type_of_ETL)
save_to_log_parameters_value (job,notebook, "type_of_data_extract", type_of_data_extract)

// COMMAND ----------

//log for source files

// COMMAND ----------

val source_file_name = "CPG_rename.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("CPG_rename")

// COMMAND ----------

//Direct_Forecast.csv

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "Fc_Hist.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Fc_Hist")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

// COMMAND ----------

val sqldf = spark.sql("""
select
version,
type,
subchannel,
address,
if (isnull(r.CPG_new), cl.client, r.CPG_old )  client,
sku,
geo,
week_date,
vol_dal,
week_num,
promo,
sku_code
from Fc_Hist cl left join  (select distinct * from  CPG_rename ) r on cl.client = r.CPG_new
""")                      
sqldf.createOrReplaceTempView("Fc_Hist")

// COMMAND ----------

//FC_KPI

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name = "FC_KPI.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("FC_KPI")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

//some tables are not updating on a regular base
if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && !is_regularly_updated_table  ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
}

// COMMAND ----------

//Calendar

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "Calendar.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

// COMMAND ----------

//MD_SKU

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "MD_SKU.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}


// COMMAND ----------

//PlantID Info

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "PlantID.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("plant_id_info")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}



// COMMAND ----------

//val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "Sell_in_RU_p2_with_formats_All.csv"
val file_location = writePath  +"/" + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Sell_in_RU_p2_with_formats")

// save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

// //number of rows in the file > 1
// if (df.count == 0) {
//   save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
//   save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
//   dbutils.notebook.exit("Failure")  
//   //throw new Exception("the source file is empty")
// }

// //if the source file is up todate

// if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
//   save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
//   save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
//   dbutils.notebook.exit("Skipped")  
//   //throw new Exception("the source file is not up to date")
// }


// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "Sell_in_All.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("orders_wpromo")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

// COMMAND ----------

val sqldf = spark.sql ("""
select 
if (isnull(r.CPG_new), ow.Client, r.CPG_old ) Client,
OrderDate,
AdressID,
Date,
SKUID,
Order_Volume,
Actual_Volume,
Return_Volume,
Order_Value,
Actual_Value,
Return_Value,
infoDeletedAnalysis,
PromoID,
Promo_BL,
Promo_Ded_ID
from orders_wpromo ow left join  (select distinct * from  CPG_rename ) r on ow.Client = r.CPG_new """)
sqldf.createOrReplaceTempView("orders_wpromo")


// COMMAND ----------

//val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "MD_SKU_RU.csv"
val file_location = writePath  +"/" + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU_RU")

// save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

// //number of rows in the file > 1
// if (df.count == 0) {
//   save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
//   save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
//   dbutils.notebook.exit("Failure")  
//   //throw new Exception("the source file is empty")
// }

// //if the source file is up todate

// if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
//   save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
//   save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
//   dbutils.notebook.exit("Skipped")  
//   //throw new Exception("the source file is not up to date")
// }

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name = "seas_sku.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("seas_sku")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}

//some tables are not updating on a regular base
if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && !is_regularly_updated_table  ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
}

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true 
val source_file_name = "MD_SKU.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}


// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true 
val source_file_name = "Calendar.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}


// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true 
val source_file_name = "MD_Clients.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_Clients")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}


// COMMAND ----------

val sqldf = spark.sql("""
select
AdressCode,
ClientCode,
if (isnull(r.CPG_new), cl.Client, r.CPG_old )  Client,
REP,
OP,
Region,
Format,
Type_TT,
ActiveAdress,
Chanal,
SGP,
New1,
New2,
RKAGroupName,
SGP_1,
RKAName
from MD_Clients cl left join  (select distinct * from  CPG_rename ) r on cl.Client = r.CPG_new
""")                      
sqldf.createOrReplaceTempView("MD_Clients")

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true 
val source_file_name = "PlantID.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("plant_id_info")

save_to_log_file_processing(job,notebook,start_time, df, source_file_name )

//number of rows in the file > 1
if (df.count == 0) {
  save_to_log_if_source_is_empty(job,notebook,start_time, source_file_name)
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
  //throw new Exception("the source file is empty")
}

//if the source file is up todate

if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0  && is_regularly_updated_table ) {
  save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
  save_to_log_last_step(job,notebook,"Skipped",notebook_start_time)
  dbutils.notebook.exit("Skipped")  
  //throw new Exception("the source file is not up to date")
}


// COMMAND ----------

//Active SKU

// COMMAND ----------

val sqldf = spark.sql("""

--	Все SKU для которых существует история продаж не менее чем в 12-ти последних месяцах (подряд, на уровне SKU по всей стране)

select SKU_Lead_ID, sum(diff) NumOfSkippedMonth
from 
(
select 
  tab.SKU_Lead_ID 
  ,
  if (
  isnull(months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))) , 1, 
  months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))
  )-1 diff

from (
  select   
  s.SKU_Lead_ID ,calendar.MonthID , sum (o.Actual_Volume) as Actual_Volume , LAG(calendar.MonthID, 1,0) OVER (partition by s.SKU_Lead_ID ORDER BY calendar.MonthID) MonthID_prev
  from orders_wpromo o left join md_sku s on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = cast(s.SKU_ID as int)
  left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
  where 
  calendar.MonthID >= year(date_add(current_date(),-365))*100 + month(date_add(current_date(),-365)) 
  group by   s.SKU_Lead_ID , calendar.MonthID
  having (Actual_Volume > 0)     
  ) tab  
) tab
group by SKU_Lead_ID
having (NumOfSkippedMonth = 0)

union 

--Кроме этого активными SKU будут считаться все сезонные Subbrand. Их список в файле seas_sku.csv

select distinct md_sku_ru.lead_sku SKU_Lead_ID,  0 as NumOfSkippedMonth
from md_sku_ru 
inner join seas_sku 
on  md_sku_ru.subbrand_name = seas_sku.Subbrand
""")
sqldf.createOrReplaceTempView("Active_SKU_Only")



// COMMAND ----------

//Active Plant

// COMMAND ----------


val sqldf = spark.sql("""

--	Все Plant, которые присутствуют не меньше чем на протяжении последних 3 месяцев без перерыва

select Plant_ID, sum(diff) NumOfSkippedMonth
from 
(
select 
  tab.Plant_ID 
  ,
  if (
  isnull(months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))) , 1, 
  months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))
  )-1 diff

from (
    select   
  plant_id_info.Plant_ID ,calendar.MonthID , sum (o.Actual_Volume) as Actual_Volume , LAG(calendar.MonthID, 1,0) OVER (partition by plant_id_info.Plant_ID ORDER BY calendar.MonthID) MonthID_prev
  from orders_wpromo o 
  left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
  left join md_clients cl on o.AdressID =cl.AdressCode 
  left join plant_id_info on cl.ActiveAdress = plant_id_info.ActiveAdress
  where 
  calendar.MonthID >= year(date_add(current_date(),-90))*100 + month(date_add(current_date(),-90)) 
  group by   plant_id_info.Plant_ID , calendar.MonthID
  having (Actual_Volume > 0)   
  ) tab  
) tab
group by Plant_ID
having (NumOfSkippedMonth = 0)

""")
sqldf.createOrReplaceTempView("Active_Plant_Only")



// COMMAND ----------

//Active CPG

// COMMAND ----------


val sqldf = spark.sql("""

-- Все CPG, которые присутствуют не меньше чем на протяжении последних 3 месяцев без перерыва

select Client, sum(diff) NumOfSkippedMonth
from 
(
select 
  tab.Client 
  ,
  if (
  isnull(months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))) , 1, 
  months_between( to_date(concat(left(MonthID,4), '.', right(MonthID,2), '.', '01'), 'yyyy.MM.dd'),to_date(concat(left(MonthID_prev,4), '.', right(MonthID_prev,2), '.', '01'), 'yyyy.MM.dd'))
  )-1 diff

from (
  select   
  o.Client ,calendar.MonthID , sum (o.Actual_Volume) as Actual_Volume , LAG(calendar.MonthID, 1,0) OVER (partition by o.Client ORDER BY calendar.MonthID) MonthID_prev
  from orders_wpromo o 
  left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
  where 
  calendar.MonthID >= year(date_add(current_date(),-90))*100 + month(date_add(current_date(),-90)) 
  group by   o.Client , calendar.MonthID
  having (Actual_Volume > 0)   
  ) tab  
) tab
group by Client
having (NumOfSkippedMonth = 0)

""")
sqldf.createOrReplaceTempView("Active_CPG_Only")



// COMMAND ----------

//step 1

// COMMAND ----------

val sqldf = spark.sql("""
select fc.version, year(fc.version)*10000 + month(fc.version)*100 + day(fc.version) as version_key , fc.client, fc.address, 
fc.sku_code, int(if(left(fc.sku_code,1) = "=", replace(fc.sku_code,left(fc.sku_code,1), ''), fc.sku_code )) sku_code_int,
fc.week_date, year(fc.week_date)*10000 + month(fc.week_date)*100 + day(fc.week_date) as week_date_key,  date_format(fc.week_date, 'dd.MM.yy') week_date_format,
fc.week_num,  kpi.Horizon, sum (fc.vol_dal)/10 vol_hl 
from Fc_Hist fc left join FC_KPI kpi on 
year(fc.version)*10000 + month(fc.version)*100 + day(fc.version) = int(right(kpi.Version,4))* 10000 + int(right(replace(kpi.Version, right(kpi.Version,5), ''),2)) *100 + int(left(kpi.Version,2)) and
year(fc.week_date)*10000 + month(fc.week_date)*100 + day(fc.week_date) = int(right(kpi.Week_Forecast,4))* 10000 + int(right(replace(kpi.Week_Forecast, right(kpi.Week_Forecast,5), ''),2)) *100 + int(left(kpi.Week_Forecast,2)) 
where (kpi.Horizon is not null) or (kpi.Horizon <> "\\N")
group by fc.version, fc.client, fc.address, fc.sku_code, fc.week_date, fc.week_num , kpi.Horizon
""")
sqldf.createOrReplaceTempView("FC_input")

// COMMAND ----------

//step 2

// COMMAND ----------

val sqldf = spark.sql("""
select * 
from (
select version_key, client, address,  sku_code_int, week_date_key,week_date_format, week_num, Horizon, vol_hl
from FC_input
)
pivot 
(
   sum(vol_hl)
   for Horizon in ('w1', 'w4', 'm3')
 )
 """) 
sqldf.createOrReplaceTempView("FC_input_pivot")

// COMMAND ----------

//step 3

// COMMAND ----------

val sqldf = spark.sql("""
select p.*, int(if(left(s.SKU_Lead_ID ,1) = "=", replace(s.SKU_Lead_ID ,left(s.SKU_Lead_ID ,1), ''), s.SKU_Lead_ID  )) lead_sku_code_int , c.WeekId, c.MonthId , plant.Plant_ID
from FC_input_pivot p 
left join MD_SKU s on p.sku_code_int = int(if(left(s.SKU_ID,1) = "=", replace(s.SKU_ID,left(s.SKU_ID,1), ''), s.SKU_ID ))
left join (select distinct Week, WeekId,MonthId from Calendar) c on p.week_date_format = c.Week
left join plant_id_info plant on trim(BOTH '		' from p.address) = trim(BOTH '		' from plant.ActiveAdress) --trim unicode symbols from  PlantID.csv
 """)
sqldf.createOrReplaceTempView("FC_result")

// COMMAND ----------

//step 4

// COMMAND ----------

val sqldf = spark.sql(""" 
select WeekId calendar_yearweek , MonthId calendar_yearmonth, lead_sku_code_int lead_sku , Plant_ID plant , client customer_planning_group, sum(w1) `sales_forecast_volume_w-1`, sum(w4) `sales_forecast_volume_w-4`
from FC_result 
group by WeekId, MonthId, lead_sku_code_int, Plant_ID, client
""")
sqldf.createOrReplaceTempView("FC_result_W1_W4")

// COMMAND ----------

//step 5

// COMMAND ----------

val sqldf = spark.sql(""" 
select MonthId calendar_yearmonth,  WeekId calendar_yearweek , lead_sku_code_int lead_sku , Plant_ID plant , client customer_planning_group, sum(m3) `forecast_volume_m-3`
from FC_result 
group by MonthId, WeekId, lead_sku_code_int, Plant_ID, client
""" )
sqldf.createOrReplaceTempView("FC_result_M3")

// COMMAND ----------

//fc_direct_weekly

// COMMAND ----------

val query_full_week = s"""
select *
from (
select  fc.* , sell.total_shipments_volume

from FC_result_W1_W4 fc 

left join 
(
select calendar_yearweek, lead_sku, plant, customer_planning_group, sum (total_shipments_volume_hl) total_shipments_volume
from Sell_in_RU_p2_with_formats 
group by calendar_yearweek, lead_sku, plant, customer_planning_group
) sell
on 
fc.calendar_yearweek = sell.calendar_yearweek and fc.lead_sku = sell.lead_sku and fc.plant = sell.plant and fc.customer_planning_group = sell.customer_planning_group

/*
where
(int(fc.lead_sku) in (select distinct int(SKU_Lead_ID) from Active_SKU_Only)) and
(fc.plant in (select distinct Plant_ID from Active_Plant_Only where Plant_ID is not null )) and 
(fc.customer_planning_group in (select distinct Client from Active_CPG_Only where Client is not null))
*/

) tab
""" 

// COMMAND ----------

val sqldf_full_week = spark.sql(query_full_week)

// COMMAND ----------

//Export to CAP

// COMMAND ----------

val query_incremental_week = s"""
select *
from (
select fc.calendar_yearmonth partition_name,  fc.* , sell.total_shipments_volume

from FC_result_W1_W4 fc 

left join 
(
select calendar_yearweek, lead_sku, plant, customer_planning_group, sum (total_shipments_volume_hl) total_shipments_volume
from Sell_in_RU_p2_with_formats 
group by calendar_yearweek, lead_sku, plant, customer_planning_group
) sell
on 
fc.calendar_yearweek = sell.calendar_yearweek and fc.lead_sku = sell.lead_sku and fc.plant = sell.plant and fc.customer_planning_group = sell.customer_planning_group

/*
where
(int(fc.lead_sku) in (select distinct int(SKU_Lead_ID) from Active_SKU_Only)) and
(fc.plant in (select distinct Plant_ID from Active_Plant_Only where Plant_ID is not null )) and 
(fc.customer_planning_group in (select distinct Client from Active_CPG_Only where Client is not null))
*/

) tab
""" +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth >= year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 
 """ 
 else ""}

// COMMAND ----------

val sqldf_incremental_week = spark.sql(query_incremental_week)

// COMMAND ----------

//fc_direct_monthly

// COMMAND ----------

val query_full_month = s"""
select * 
from( 
select  fc.*, sell.actual_sales_volume 

from FC_result_M3 fc 

left join 
(
select calendar_yearmonth,calendar_yearweek, lead_sku, plant, customer_planning_group, sum (total_shipments_volume_hl) actual_sales_volume
from Sell_in_RU_p2_with_formats 
group by calendar_yearmonth,calendar_yearweek, lead_sku, plant, customer_planning_group
) sell 
on 
fc.calendar_yearmonth = sell.calendar_yearmonth and fc.calendar_yearweek = sell.calendar_yearweek and fc.lead_sku = sell.lead_sku and fc.plant = sell.plant and fc.customer_planning_group = sell.customer_planning_group

/*
where
  (int(fc.lead_sku) in (select distinct int(SKU_Lead_ID) from Active_SKU_Only)) and
  (fc.plant in (select distinct Plant_ID from Active_Plant_Only where Plant_ID is not null )) and 
  (fc.customer_planning_group in (select distinct Client from Active_CPG_Only where Client is not null))
*/  

) tab 

""" 

// COMMAND ----------

val sqldf_full_month = spark.sql(query_full_month)

// COMMAND ----------

val query_incremental_month  = s"""
select * 
from( 
select fc.calendar_yearmonth partition_name,  fc.*, sell.actual_sales_volume 

from FC_result_M3 fc 

left join 
(
select calendar_yearmonth,calendar_yearweek, lead_sku, plant, customer_planning_group, sum (total_shipments_volume_hl) actual_sales_volume
from Sell_in_RU_p2_with_formats 
group by calendar_yearmonth,calendar_yearweek, lead_sku, plant, customer_planning_group
) sell 
on 
fc.calendar_yearmonth = sell.calendar_yearmonth and fc.calendar_yearweek = sell.calendar_yearweek and fc.lead_sku = sell.lead_sku and fc.plant = sell.plant and fc.customer_planning_group = sell.customer_planning_group

/*
where
  (int(fc.lead_sku) in (select distinct int(SKU_Lead_ID) from Active_SKU_Only)) and
  (fc.plant in (select distinct Plant_ID from Active_Plant_Only where Plant_ID is not null )) and 
  (fc.customer_planning_group in (select distinct Client from Active_CPG_Only where Client is not null))
*/  

) tab 

""" +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth >= year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 """ 
 else ""}

// COMMAND ----------

val sqldf_incremental_month = spark.sql(query_incremental_month)

// COMMAND ----------

//Final testing before result export

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fc_acc_week, sqldf_full_week, readPath)

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fc_acc_month, sqldf_full_month, readPath)

// COMMAND ----------

val sqldf_result_fw = load_preliminary_result(fc_acc_week)

// COMMAND ----------

val sqldf_result_fm = load_preliminary_result(fc_acc_month)

// COMMAND ----------

save_to_log_start_result_testing(job,notebook)

// COMMAND ----------

if (Test_Number_of_Rows(job,notebook, sqldf_result_fw, fc_acc_week) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Number_of_Rows(job,notebook, sqldf_result_fm, fc_acc_month) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Is_incomplete_period(job,notebook, sqldf_result_fw, fc_acc_week) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Is_incomplete_period(job,notebook, sqldf_result_fm, fc_acc_month) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result_fw, "lead_sku", fc_acc_week ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  //dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result_fm, "lead_sku", fc_acc_month ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  //dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result_fw, "customer_planning_group", fc_acc_week ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result_fm, "customer_planning_group", fc_acc_month ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

Test_Values_In_Correct_Range(job, notebook, sqldf_result_fw, "weekly", "total_shipments_volume", 67349, 238703, fc_acc_week)

// COMMAND ----------

Test_Values_In_Correct_Range(job, notebook, sqldf_result_fw, "weekly", "`sales_forecast_volume_w-1`", 114153, 293463, fc_acc_week)

// COMMAND ----------

Test_Values_In_Correct_Range(job, notebook, sqldf_result_fw, "weekly", "`sales_forecast_volume_w-4`", 111735, 276854, fc_acc_week)

// COMMAND ----------

Test_Values_In_Correct_Range(job, notebook, sqldf_result_fm, "monthly", "actual_sales_volume", 276257, 1036805, fc_acc_month)

// COMMAND ----------

Test_Values_In_Correct_Range(job, notebook, sqldf_result_fm, "monthly", "`forecast_volume_m-3`", 387047, 1107956, fc_acc_month)

// COMMAND ----------

// end Final testing before result export

// COMMAND ----------

val Result = 
if (type_of_ETL == 0) { 
  if (exportToBlobStorage_Baltika (job,notebook,fc_acc_week, sqldf_full_week, readPath ) == "Success" &&
      exportToBlobStorage_Baltika (job,notebook,fc_acc_month, sqldf_full_month, readPath ) == "Success" &&
      exportToBlobStorage_CAP (job,notebook,0, sqldf_incremental_week, "FCACCWEEKDIRECT_" , readPath, readPath_GBS ) == "Success" &&
      exportToBlobStorage_CAP (job,notebook,0, sqldf_incremental_month, "FCACCMONTHDIRECT_" , readPath, readPath_GBS ) == "Success"
     )  "Success" else "Failure"  }
else if (type_of_ETL == 1) { 
  if (exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental_week, "FCACCWEEKDIRECT_" , readPath, readPath_GBS ) == "Success" &&
      exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental_month, "FCACCMONTHDIRECT_" , readPath, readPath_GBS ) == "Success"
     ) "Success" else  "Failure"  }
else if (type_of_ETL == 2) { 
  if (exportToBlobStorage_Baltika (job,notebook,fc_acc_week, sqldf_full_week, readPath ) == "Success" &&
      exportToBlobStorage_Baltika (job,notebook,fc_acc_month, sqldf_full_month , readPath ) == "Success" &&
      exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental_week, "FCACCWEEKDIRECT_" , readPath, readPath_GBS ) == "Success" &&
      exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental_month, "FCACCMONTHDIRECT_" , readPath, readPath_GBS ) == "Success"
    
     ) "Success" else "Failure" }
else "Unexpected parameter"


//save to log
save_to_log_last_step(job,notebook,Result,notebook_start_time)

dbutils.notebook.exit(Result)