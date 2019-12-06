// Databricks notebook source
//Type of ETL: 0 (only Baltika ) 1 (only CAP) 2 (Both)

// COMMAND ----------

val type_of_ETL: Int = 2

// COMMAND ----------

//Type of data extraction: 0 (full) , 1 (incremental )

// COMMAND ----------

val type_of_data_extract: Int = 0

// COMMAND ----------

// The range of month in case of incremental loading (only if type_of_data_extract = 1 !!! )

// COMMAND ----------

val num_of_days_before_current_date: Int = 30 //number of day before current date
//val num_of_days_after_current_date: Int = 30  //number of day after current date

// COMMAND ----------

//Configuration (Baltika)
val storage_account_name = "staeeprodbigdataml2c"
val storage_account_access_key = "EHYumrwso4XLSUHpvLptI33z7mumiZwZOErjrlP8FiW51Bb6NS2PaWJsqW9hsMttbZizgQjUexFZfZDBQJebYw=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

// COMMAND ----------

//Configuration (CAP)
spark.conf.set(
  "fs.azure.sas.dcd.prdcbwesa01.blob.core.windows.net",
  "https://prdcbwesa01.blob.core.windows.net/dcd?st=2019-09-13T15%3A01%3A24Z&se=2020-03-14T14%3A01%3A00Z&sp=rwdl&sv=2018-03-28&sr=c&sig=aErgDFXTRr3Lj519B4ZtjDHTp%2F3xsXchFqVuS2IAnGc%3D")

// COMMAND ----------

//constants (Baltika)

// COMMAND ----------

val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
val writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result" // //etl_fbkp
val writePath_tmp = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp" 
val writePath_СAP = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/export_to_CAP"
val fname = "Sell_in_Promo_RU_p2_with_formats_All.csv"
//val fname = "test2_.csv"
val fname_tmp = "Sell_in_Promo_RU_p2_with_formats_tmp_All.csv"
val file_location_path = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/"

// COMMAND ----------

//constants (CAP)

// COMMAND ----------

val writePath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU" 
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp" 

// COMMAND ----------

//Calendar

// COMMAND ----------

val file_location = file_location_path + "Calendar.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Calendar")

// COMMAND ----------

//MD_SKU

// COMMAND ----------

val file_location = file_location_path + "MD_SKU.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU")

// COMMAND ----------

//MD_Clients

// COMMAND ----------

val file_location = file_location_path + "MD_Clients.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_Clients")

// COMMAND ----------

//Sell-in

// COMMAND ----------

val file_location = file_location_path+ "Sell_in_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Orders_Main")

// COMMAND ----------

//Divisions

// COMMAND ----------

val file_location = file_location_path + "Division.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Divisions")

// COMMAND ----------

//PlantID

// COMMAND ----------

val file_location = file_location_path + "PlantID.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("plant_id_info")

// COMMAND ----------

//Actionid_wheader

// COMMAND ----------

val file_location =file_location_path + "Action_MT_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Action")

// COMMAND ----------

//RP_patch

// COMMAND ----------

val file_location = file_location_path + "RP_patch.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("RP_patch")

// COMMAND ----------

//CPG_Formats

// COMMAND ----------

val file_location = file_location_path+ "CPG_Formats.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("CPG_Formats")

// COMMAND ----------

//MD_SKU_RU

// COMMAND ----------

val file_location = writePath + "/"+ "MD_SKU_RU.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU_RU")

// COMMAND ----------

//seas_sku

// COMMAND ----------

val file_location = file_location_path + "/"+ "seas_sku.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("seas_sku")

// COMMAND ----------

//PromoDSD_All_History.csv

// COMMAND ----------

val file_location = file_location_path + "/"+ "PromoDSD_All_History.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("PromoDSD_All_History")

// COMMAND ----------

//SGP_B_A_ActiveAddress.csv

// COMMAND ----------

val file_location = file_location_path + "/"+ "SGP_B_A_ActiveAddress.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("SGP_B_A_ActiveAddress")

// COMMAND ----------

//Sell_in_RU_p2_with_formats_All.csv

// COMMAND ----------

val file_location = writePath  +"/" +  "Sell_in_RU_p2_with_formats_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Sell_in_RU_p2_with_formats")

// COMMAND ----------

//uplift_info_history

// COMMAND ----------

val sqldf = spark.sql("""

select 
calendar_yearweek ,  customer_planning_group, format,  lead_sku,  active_address, activation_code, sum(uplift_hl) uplift_hl

from (

select 
cl.WeekId calendar_yearweek , if(isnull(client_info.RKAName) or client_info.RKAName = "\\N" , pdah.Client_name, client_info.RKAName) customer_planning_group, 

/*
if (
if(isnull(client_info.RKAName) or client_info.RKAName = "\\N" , pdah.Client_name, client_info.RKAName) = 'X5 Retail Group' or 
if(isnull(client_info.RKAName) or client_info.RKAName = "\\N" , pdah.Client_name, client_info.RKAName) = 'Магнит (Тандер)' ,  
  if(client_info.New2 = 'СМ/ГМ', 'Супермаркет', if(isnull(client_info.New2) or client_info.New2 = "\\N", 'Минимаркет', client_info.New2 )) , 
  if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
  ) format,
 */ 
 
 
  if (
if(isnull(client_info.RKAName) or client_info.RKAName = "\\N" , pdah.Client_name, client_info.RKAName) = 'X5 Retail Group' or 
if(isnull(client_info.RKAName) or client_info.RKAName = "\\N" , pdah.Client_name, client_info.RKAName) = 'Магнит (Тандер)' ,  
  --if( pdah.Format_TT = '' or isnull(pdah.Format_TT) or pdah.Format_TT = "\\N" or pdah.Format_TT= '(пусто)', 'Минимаркет', pdah.Format_TT ) , 
  if( (pdah.Format_TT <> 'Гипермаркет'  and pdah.Format_TT <> 'Супермаркет') or pdah.Format_TT is null , 'Минимаркет', pdah.Format_TT), 
  if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
  ) format,
 
  
  MD_SKU.SKU_Lead_ID lead_sku, 
  act_info.ActiveAdress active_address,
  pdah.ActivationID activation_code,
  
  round(
  sum(
  if(isnull(pdah.Promo_week) or pdah.Promo_week = "\\N", 0, pdah.Promo_week)  + 
  if(isnull(pdah.BL_week) or pdah.BL_week = "\\N", 0 , pdah.BL_week) 
  ) /10
  ,5) uplift_hl
  
from  PromoDSD_All_History pdah 
left join Calendar cl on pdah.week_forecast = cl.DayName
left join (select  Client /*, New2*/, RKAName from MD_Clients group by Client, RKAName /*, New2*/) client_info on pdah.Client_name = client_info.Client
left join CPG_Formats cpgf on if(isnull(client_info.RKAName) , pdah.Client_name, client_info.RKAName) = cpgf.CPG
left join (select distinct SKU_ID, SKU_Lead_ID from MD_SKU) MD_SKU  on int( if(left(pdah.SKUID, 1) = "=", replace(pdah.SKUID, left(pdah.SKUID, 1), ''), pdah.SKUID))  = MD_SKU.SKU_ID

left join 
(
  select ActiveAdress, SGP_Before
  from (
  select ba.ActiveAdress, ba.SGP_Before,  row_number() over ( partition by  ba.SGP_Before order by  sum (s.total_shipments_volume_hl) desc) row_num
  from SGP_B_A_ActiveAddress ba left join Sell_in_RU_p2_with_formats s on s.plant_name = ba.ActiveAdress
  group by  ba.SGP_Before, ba.ActiveAdress
  ) tab 
  where 
  tab.row_num = 1
) act_info
on  pdah.SGP =  act_info.SGP_Before 

group by cl.WeekId, pdah.Client_name, client_info.RKAName, /*client_info.New2 ,*/ pdah.Format_TT ,  cpgf.format, MD_SKU.SKU_Lead_ID, act_info.ActiveAdress, pdah.ActivationID
) tab

group by calendar_yearweek ,  customer_planning_group, format,  lead_sku,  active_address, activation_code
""")
sqldf.createOrReplaceTempView("uplift_info")

// COMMAND ----------

//Sell_in_Promo_RU_p2_with_formats_tmp (with the same numbers of Activation codes and Formats)

// COMMAND ----------

var Result = "Failure"   
val sqldf = spark.sql(
  """
select 
calendar_yearmonth, calendar_yearweek ,  lead_sku, active_sku,  active_address, plant,  customer_planning_group, division, mechanism,  format, concat(customer_planning_group, '_', format) as cpg_format, activation_code, /*start_orders, end_orders,*/ length_promo, shelf_discount, discount,  forecast_volume, sum(promo_volume) promo_volume

from (
select 
tab.Year_Month calendar_yearmonth, tab.Year_Week calendar_yearweek , replace (replace (tab.Lead_SKUID ,51070074, 510700 ) ,38070074, 380700 ) lead_sku, 
tab.ActiveAdress active_address, tab.plant, tab.Client customer_planning_group, tab. division
, if(act_info.mechanik_new is not null and act_info.mechanik_new <> "\\N" and act_info.mechanik_new <> '', if(lower(act_info.mechanik_new) = "ценовое промо+паллеты+каталог", "Ценовое промо+Паллеты+Каталог", act_info.mechanik_new ), 
     if(tab.Activation_Code = 900 , "Client Promo",
       if(act_info.mechanik is null or act_info.mechanik = "DIOT_MT_Лок.промо_конечный потребитель" or act_info.mechanik = "кастомер промо" or act_info.mechanik = "Каст.промо_контракт" or 
         act_info.mechanik = "тактические доп.активности" , "Другое", 
         if(lower(act_info.mechanik) = "ценовое промо+паллеты+каталог"  ,  "Ценовое промо+Паллеты+Каталог", 
           if(act_info.mechanik = "ценовое промо+каталог_не использовать для 2017 года", "Ценовое промо+Каталог", 
             if(act_info.mechanik ="ценовое промо_не использовать для 2017 года", "Ценовое промо", 
               if(act_info.mechanik = "In Out", "Ценовое промо",
                 if (act_info.mechanik = "контрактованные паллеты_не использовать для 2017 года", "Паллеты",
                   if(act_info.mechanik = "Ценовое Промо Indirect для смешанного типа доставки", "Ценовое промо" ,
                     if(act_info.mechanik = "ценовое промо+паллеты_не использовать для 2017 года","Ценовое промо+Паллеты",
                       if(act_info.mechanik = "контрактованные паллеты+цен промо_не использовать для 2017 года", "Ценовое промо+Паллеты",
                         if(act_info.mechanik = "контрактованные паллеты+цен промо+каталог_не использовать для 2017 года" ,"Ценовое промо+Паллеты+Каталог",act_info.mechanik            
             )))))))))))) mechanism             
--, if(tab.Activation_Code=900, "Минимаркет", if (isnull(act_info.format), "не определено", act_info.format))  format 
--,if(cl.New2 = 'СМ/ГМ', 'Супермаркет', cl.New2) format
,
if (
tab.Client = 'X5 Retail Group' or tab.Client = 'Магнит (Тандер)' ,  
  if(cl.New2 = 'СМ/ГМ', 'Супермаркет', if(isnull(cl.New2) or cl.New2 = "\\N", 'Минимаркет', cl.New2 )) , 
  if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
  ) format

,tab.Activation_Code activation_code 
,if( isnull(acts.SKU_Lead_ID), 0, 1) active_sku
,if (tab.Activation_Code=900, tab.Year_Week , act_info.weekid_st) start_orders
,if (tab.Activation_Code=900, tab.Year_Week , act_info.weekid_end) end_orders
,if (isnull(act_info.length_promo), 0, act_info.length_promo) length_promo
,if (isnull(act_info.shelf_discount) or act_info.shelf_discount > 1 , 0, act_info.shelf_discount) shelf_discount
,if (isnull(act_info.discount), 0, act_info.discount) discount
-- if (isnull(act_info.forecast_volume/100), 0, act_info.forecast_volume/100)   forecast_volume
,if (isnull(fr_v.forecast_volume/100), 0, fr_v.forecast_volume/100)   forecast_volume
,if (isnull(tab.Promo_Volume/10), 0, tab.Promo_Volume/10) promo_volume



from (
select 
source.Year_Month, source.Year_Week, source.Lead_SKUID, source.ActiveAdress, source.Client, source.Activation_Code, plant_id_info.Plant_ID plant, Divisions.Division division , source.AdressID,
sum(source.Actual_Volume) Actual_Volume, sum(Promo_Volume) Promo_Volume

from (
select 
date_info.MonthId  Year_Month,
date_info.cl_Week Year_Week ,
date_info.Lead_SKUID_int Lead_SKUID,
ActiveAddress_ID_info.ActiveAddress_ID,
date_info.ActiveAdress,
date_info.Client, 
if (isnull(orders.Actual_Volume), 0,orders.Actual_Volume) as Actual_Volume , 
if (isnull(orders.Promo_Volume), 0, orders.Promo_Volume) as Promo_Volume,
--key_promo_info.Activation_Code,
city_info.City,
orders.Promo_Ded_ID Activation_Code,
date_info.AdressID
/*[start block] year_month, year_week with unique combination of lead SKU, ActiveAddress, Client, Brand */
from 
(
    select distinct 
    date_info.MonthId, 
    date_info.WeekId cl_Week,   
    date_info.ActiveAdress,  
    date_info.Client, 
    date_info.Lead_SKUID_int, 
  
    date_info.Promo_Ded_ID,
    date_info.AdressID
    from (
            select  
            DateKey, 
            MonthId , 
            WeekId, 
            keys_info.ActiveAdress,  
            keys_info.Client, 
            keys_info.Lead_SKUID_int,        
          
            keys_info.Promo_Ded_ID,
            keys_info.AdressID,
            cast (concat(left(DateKey, 4), '-', 
            left(replace(DateKey, left(DateKey, 4), ''), 2),'-', left(replace(DateKey, left(DateKey, 6), ''), 2)) as date)date
            from Calendar     
            cross join 
            (
            select  
            cl.ActiveAdress,  
            o.Client , 
            ls.Lead_SKU_ID as Lead_SKUID_int, 
           
            o.Promo_Ded_ID, 
            o.AdressID
            from orders_main o 
            left join (select distinct cast (SKU_ID as int) SKU_ID, cast(SKU_Lead_ID as int) Lead_SKU_ID from md_sku) ls 
            on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = ls.SKU_ID 
            left join md_clients cl 
            on o.AdressID =cl.AdressCode 
            left join (select cast (SKU_ID as int) SKU_ID_Int from  md_sku) md_sku 
            on  ls.Lead_SKU_ID = md_sku.SKU_ID_Int   
            group by ls.Lead_SKU_ID,  cl.ActiveAdress,  o.Client , o.Promo_Ded_ID, o.AdressID
            ) keys_info    
          ) date_info
          cross join 
          (select max(Date) max_date from orders_main) date_limit 
          where  
          cast (concat( left(DateKey, 4),  '-',  left(replace(DateKey, left(DateKey, 4), ''), 2) , '-', 
          left(replace(DateKey, left(DateKey, 6), ''), 2)) as date) <= date_limit.max_date
  
) date_info
/*[end block] year_month, year_week with unique combination of lead SKU, ActiveAddress, Client, Brand */  

left join 
/*[start block] orders with actual volume and promo volume */ 
(
select  orders.MonthId, orders.WeekId, orders.Client, orders.ActiveAdress, orders.Lead_SKU_ID , orders.Lead_SKU_ID_Int, sum(orders.Actual_Volume) Actual_Volume , sum( orders.Promo_Volume) Promo_Volume , orders.Promo_Ded_ID, orders.AdressID
from (

select tab_main.MonthId,  tab_main.WeekId, tab_main.Client, tab_main.ActiveAdress,tab_main.AdressID, tab_main.SKU_Lead_ID Lead_SKU_ID ,
cast(tab_main.SKU_Lead_ID as int) Lead_SKU_ID_Int,
if(isnull(sum (tab_main.Actual_Volume)) , 0, sum (tab_main.Actual_Volume)) as Actual_Volume, 
if(isnull(sum (tab_main.Actual_Volume_With_Promo)) , 0, sum (tab_main.Actual_Volume_With_Promo)) as Promo_Volume
,tab_main.Promo_Ded_ID
from (

  select tab1.*, tab2.Actual_Volume_With_Promo
  from 
  (

    select tab.MonthId, tab.WeekId, tab.Client, tab.ActiveAdress,tab.AdressID, tab.SKU_Lead_ID, tab.Promo_Marker, tab.Promo_Ded_ID,
    sum (tab.Actual_Volume) as Actual_Volume
    from 
    (
      select tab.*
      from (
      
      select calendar.MonthId, calendar.WeekId,  o.Client, cl.ActiveAdress,o.AdressID, s.SKU_Lead_ID, 
      if(cast(o.Promo_Ded_ID as int) is null, 0,1 ) as Promo_Marker,  sum (o.Actual_Volume) as Actual_Volume ,
      o.Promo_Ded_ID
      from orders_main o left join md_clients cl on o.AdressID =cl.AdressCode 
      left join md_sku s on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = cast(s.SKU_ID as int)
      --left join lead_sku ls on s.SKUID = ls.SKU_ID 
      left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
      group by  calendar.MonthId, calendar.WeekId,  o.Client, s.SKU_Lead_ID, o.Promo_Ded_ID, cl.ActiveAdress, o.AdressID
      ) tab
      
      )tab 
      group by  tab.MonthId, tab.WeekId, tab.Client, tab.AdressID, tab.ActiveAdress, tab.SKU_Lead_ID, tab.Promo_Marker, tab.Promo_Ded_ID
   ) tab1

  left join 
  
(
  select tab.*
  from 
  (

    select tab.MonthId,  tab.WeekId,  tab.Client, tab.ActiveAdress, tab.AdressID, tab.SKU_Lead_ID, tab.Promo_Marker, tab.Promo_Ded_ID,
    sum (tab.Actual_Volume) Actual_Volume_With_Promo
    from 
    (
      select tab.*
      from (

      select calendar.MonthId, calendar.WeekId,  o.Client, cl.ActiveAdress,o.AdressID, s.SKU_Lead_ID, 
      if(cast(o.Promo_Ded_ID as int) is null, 0,1 ) as Promo_Marker,  sum (o.Actual_Volume) as Actual_Volume ,
      o.Promo_Ded_ID
      from orders_main o left join md_clients cl on o.AdressID =cl.AdressCode 
      left join md_sku s on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = cast(s.SKU_ID as int)
      --left join lead_sku ls on s.SKUID = ls.SKU_ID
      left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
      group by calendar.MonthId, calendar.WeekId, o.Client, s.SKU_Lead_ID, o.Promo_Ded_ID,cl.ActiveAdress, o.AdressID
      ) tab
      
        )tab 
      group by  tab.MonthId, tab.WeekId,  tab.Client, tab.ActiveAdress, tab.AdressID, tab.SKU_Lead_ID, tab.Promo_Marker, tab.Promo_Ded_ID
   ) tab
   where 
   tab.Promo_Marker = 1
  ) 
  tab2
  on 
   tab1.MonthId = tab2.MonthId and tab1.WeekId = tab2.WeekId  and tab1.Client = tab2.Client and tab1.ActiveAdress = 
  tab2.ActiveAdress and tab1.AdressID = tab2.AdressID and tab1.SKU_Lead_ID =tab2.SKU_Lead_ID and tab1.Promo_Marker = tab2.Promo_Marker
  and tab1.Promo_Ded_ID = tab2.Promo_Ded_ID
 
    ) tab_main 

    group by  tab_main.MonthId, tab_main.WeekId,  tab_main.Client, tab_main.ActiveAdress, tab_main.AdressID, tab_main.SKU_Lead_ID, tab_main.Promo_Ded_ID  

  ) orders
  group by orders.MonthId, orders.WeekId,  orders.Client, orders.ActiveAdress, orders.Lead_SKU_ID , orders.Lead_SKU_ID_Int, orders.Promo_Ded_ID, orders.AdressID
  
) orders 
on 
date_info.MonthId = orders.MonthId and
date_info.cl_Week = orders.WeekId and
date_info.Lead_SKUID_int = orders.Lead_SKU_ID_int and 
date_info.ActiveAdress = orders.ActiveAdress and
date_info.Client = orders.Client and
date_info.Promo_Ded_ID = orders.Promo_Ded_ID and
date_info.AdressID =  orders.AdressID
/*[end block] orders with actual volume and promo volume */ 


left join 

/*[start block] city info */ 
(
select tab.ActiveAdress, REP City
from (
  select cl.ActiveAdress , cl.REP ,Actual_Volume, row_number() over ( partition by   cl.ActiveAdress  order by if(isnull(Actual_Volume),0, Actual_Volume) desc ) as key_city
  from md_clients cl left join orders_main o on o.AdressID =cl.AdressCode
  ) tab
  where tab.key_city = 1 
) city_info
on date_info.ActiveAdress = city_info.ActiveAdress
/*[end block] city info */ 

/*[start block] ActiveAddress_ID */ 
left join 
(
select ActiveAdress, row_number() over ( order by ActiveAdress  ) as ActiveAddress_ID
from  ( select distinct ActiveAdress from md_clients) clients
) ActiveAddress_ID_info
on date_info.ActiveAdress = ActiveAddress_ID_info.ActiveAdress
/*[end block] ActiveAddress_ID */ 

/*[start block] without SKU's with Actual_Volume = 0 for the entire period*/ 
where   if(isnull(date_info.Lead_SKUID_int), 0, date_info.Lead_SKUID_int) not in 
(select 
if(isnull(cast(s.SKU_Lead_ID as int)) , 0,  cast(s.SKU_Lead_ID as int) )   Lead_SKU_ID_Int
from orders_main o  
left join md_sku s on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = cast(s.SKU_ID as int)
--left join lead_sku ls on s.SKUID = ls.SKU_ID
group by s.SKU_Lead_ID
having (sum (o.Actual_Volume) = 0)
)
/*[end block] without SKU's with Actual_Volume = 0 for the entire period*/ 

) source

left join plant_id_info on source.ActiveAdress = plant_id_info.ActiveAdress
left join Divisions on Divisions.WH = source.ActiveAdress

where 
source.Promo_Volume > 0 and int(if(isnull(source.Activation_Code) , 0, source.Activation_Code)) > 0 
group by 
source.Year_Month, source.Year_Week, source.Lead_SKUID, source.ActiveAdress, source.Client, source.Activation_Code, plant_id_info.Plant_ID, Divisions.Division, source.AdressID 
) tab

left join 

(
  select tab.*
  from (
  select
  ac.ActivationID activation_id, ac.mechanik, ac.in_voice, ac.format, ac.end_action , ac.begin_action , ac.DeliveryType delivery_type
  , ac.mechanik_new, ac.start_orders, ac.end_orders , st.weekid weekid_st, ed.weekid weekid_end,
  datediff(end_action , begin_action)+1 length_promo ,
  max(if (ac.shelf_discount = "\\N" or ac.shelf_discount is null, 0, ac.shelf_discount )) shelf_discount , 
  max(if (ac.discount = "\\N" or ac.discount is null, 0, ac.discount )) discount,
  sum(Forecast_Volume) forecast_volume, --!!!
  row_number() over ( partition by   ac.ActivationID   order by if(isnull(sum(Forecast_Volume)),0, sum(Forecast_Volume)) desc ) act_rank

  from  Action ac 
  left join calendar st on  year(ac.start_orders)*10000 +month(ac.start_orders)*100 + day(ac.start_orders)  = st.DateKey
  left join calendar ed on  year(ac.end_orders)*10000 +month(ac.end_orders)*100 + day(ac.end_orders)  = ed.DateKey
  where 
  year(ac.start_orders) >=2015 --!!!
  and ac.DeliveryType = 'DSD'
  group by 
  ac.ActivationID, ac.Mechanik, ac.In_voice, ac.Format, ac.end_action , ac.begin_action ,ac.Mechanik_new, ac.start_orders, ac.end_orders , st.weekid, ed.weekid , ac.DeliveryType
  ) tab
  where tab.act_rank = 1 
  
) act_info 
on tab.Activation_Code = act_info.activation_id

left join 
(
select ActivationID activation_id, sum(Forecast_Volume) forecast_volume
from Action  
group by ActivationID
) fr_v
on tab.Activation_Code = fr_v.activation_id

left join md_clients cl on tab.AdressID =cl.AdressCode 

left join CPG_Formats cpgf on  tab.Client = cpgf.CPG

left join 
(
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
  from orders_main o left join md_sku s on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = cast(s.SKU_ID as int)
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
     
)acts
on tab.Lead_SKUID = acts.SKU_Lead_ID 

where 
tab.Year_Week >= 201601  and tab.ActiveAdress <> 'Нет активного адреса' 

) main

group by 
calendar_yearmonth, calendar_yearweek ,  lead_sku, active_sku,  active_address, plant,  customer_planning_group, division, mechanism, format, activation_code, /*start_orders, end_orders,*/ 
length_promo, shelf_discount, discount, forecast_volume
  """
  )

sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , writePath_tmp+"/"+fname_tmp)   
dbutils.fs.rm(readPath , recurse = true) 
if (row_count > 0) Result = "Success" else println("The file " +writePath_tmp+"/"+fname + " is empty !" )
println("temp promo table has been created with status: " + Result )
//sqldf.createOrReplaceTempView("result_source_table")


// COMMAND ----------

//Sell_in_Promo_RU_p2_with_formats_tmp

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/Sell_in_Promo_RU_p2_with_formats_tmp_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("result_source_table")

// COMMAND ----------

//promo_activities_of_Competitors_info

// COMMAND ----------

val sqldf = spark.sql("""
select plant, lead_sku, calendar_yearweek, count(distinct activation_code ) promo_activities_of_competitors
from result_source_table
group by plant, lead_sku, calendar_yearweek
""")
sqldf.createOrReplaceTempView("promo_activities_of_Competitors_info")

// COMMAND ----------

//distance_between_promotions_info

// COMMAND ----------

val sqldf = spark.sql("""

  select 
  s.activation_code, s.plant, s.lead_sku, s.customer_planning_group, a.start_orders, a.end_orders , 
  LAG(a.end_orders, 1, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders) end_prev,
  
 if (
   datediff(a.start_orders, LAG(a.end_orders, 1, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)) > 0 , 
   datediff(a.start_orders, LAG(a.end_orders, 1, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)) , 
     if (
       datediff(a.start_orders, LAG(a.end_orders, 2, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)) > 0 ,
       datediff(a.start_orders, LAG(a.end_orders, 2, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)) , 
       if (
         datediff(a.start_orders, LAG(a.end_orders, 3, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)) > 0,
         datediff(a.start_orders, LAG(a.end_orders, 3, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)),        
         if (
           datediff(a.start_orders, LAG(a.end_orders, 4, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)) > 0,
           datediff(a.start_orders, LAG(a.end_orders, 4, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)),
           if (
            datediff(a.start_orders, LAG(a.end_orders, 5, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)) > 0,
            datediff(a.start_orders, LAG(a.end_orders, 5, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)), 
             if (
              datediff(a.start_orders, LAG(a.end_orders, 6, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)) > 0,
              datediff(a.start_orders, LAG(a.end_orders, 6, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)), 
                if (
                  datediff(a.start_orders, LAG(a.end_orders, 7, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)) > 0,
                  datediff(a.start_orders, LAG(a.end_orders, 7, null) OVER (partition by s.plant, s.lead_sku, s.customer_planning_group  ORDER BY a.start_orders)), 
                  null
            ))))))) distance_between_promotions
    

  from result_source_table s 
  left join Action a on s.activation_code = a.ActivationID
  where s.activation_code is not null
  group by   s.activation_code, s.plant, s.lead_sku, s.customer_planning_group, a.start_orders, a.end_orders

""")
sqldf.createOrReplaceTempView("distance_between_promotions_info")

// COMMAND ----------

//  val sqldf = spark.sql("""
//  select  calendar.WeekId calendar_yearweek ,  s.SKU_Lead_ID lead_sku,  plant_id_info.Plant_ID plant,  count(o.Promo_Ded_ID) promo_activities_of_competitors
//  from orders_main o 
//  left join md_sku s on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = cast(s.SKU_ID as int)
//  left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
//  left join md_clients cl on o.AdressID =cl.AdressCode 
//  left join plant_id_info on cl.ActiveAdress = plant_id_info.ActiveAdress
//  where 
//  calendar.WeekId >= 201601  and cl.ActiveAdress <> 'Нет активного адреса' 
//  group by  calendar.WeekId, s.SKU_Lead_ID, plant_id_info.Plant_ID
//  """)
// sqldf.createOrReplaceTempView("promo_activities_of_Competitors_info")        

   

// COMMAND ----------

//hfa HFA_RU_tmp_All_promo_uplift.csv or ETL/Result/HFA_RU_All.csv

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result/HFA_RU_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("hfa")

// COMMAND ----------

//future_uplift

// COMMAND ----------

val fu = spark.sql("""
select distinct 
hfa.calendar_yearmonth, hfa.calendar_yearweek, hfa.lead_sku, 1 as active_sku, plant_id_info.ActiveAdress active_address, hfa.plant, hfa.customer_planning_group, hfa.division, hfa.mechanism, hfa.format, hfa.cpg_format,
hfa.activation_code, hfa.length_promo, hfa.shelf_discount, hfa.discount, fr_v.forecast_volume, null as promo_volume, hfa.METRO_super_Wednesday, hfa.X5_MTP, hfa.X5_MTP_Loc, hfa.X5_OTP, hfa.X5_OTP_Loc, 
hfa.promo_activities_of_competitors, hfa.distance_between_promotions, hfa.uplift_hl
from hfa 
left join plant_id_info on hfa.plant = plant_id_info.Plant_ID
left join 
(
select ActivationID activation_id, sum(Forecast_Volume)/100 forecast_volume
from Action  
group by ActivationID
) fr_v
on hfa.activation_code = fr_v.activation_id

where hfa.activation_code is not null
"""
 )
fu.createOrReplaceTempView("future_uplift")

// COMMAND ----------

val hu = spark.sql("""

  select distinct
  
  s.*, 
  
  if (isnull(METRO_super_Wednesday_info.METRO_super_Wednesday) , 0,  METRO_super_Wednesday_info.METRO_super_Wednesday ) METRO_super_Wednesday,
  
  if (s.customer_planning_group = 'X5 Retail Group' and rp.MTP is not null and isnull(rp.Lok), 1, 0) X5_MTP,
  if (s.customer_planning_group = 'X5 Retail Group' and rp.MTP is not null and rp.Lok is not null , 1, 0) X5_MTP_Loc,
  if (s.customer_planning_group = 'X5 Retail Group' and rp.OTP is not null and isnull(rp.Lok), 1, 0) X5_OTP,
  if (s.customer_planning_group = 'X5 Retail Group' and rp.OTP is not null and rp.Lok is not null, 1, 0) X5_OTP_Loc,
  
  if (pac.promo_activities_of_competitors is null, 0, pac.promo_activities_of_competitors) promo_activities_of_competitors,
  if (dbp.distance_between_promotions is null, 0, dbp.distance_between_promotions) distance_between_promotions,
  
  upi.uplift_hl
  
  from result_source_table s 
  
  left join 
  (
  
  select tab.activation_id , 1 as METRO_super_Wednesday
  from (
  select
  ac.Client, ac.ActivationID activation_id, ac.mechanik, ac.in_voice, ac.format, ac.end_action , ac.begin_action , ac.DeliveryType delivery_type
  , ac.mechanik_new, ac.start_orders, ac.end_orders , st.weekid weekid_st, ed.weekid weekid_end,
  datediff(end_action , begin_action)+1 length_promo ,
  max(if (ac.shelf_discount = "\\N" or ac.shelf_discount is null, 0, ac.shelf_discount )) shelf_discount , 
  max(if (ac.discount = "\\N" or ac.discount is null, 0, ac.discount )) discount,
  sum(Forecast_Volume) forecast_volume, cl.DayOfWeek_Num,
  row_number() over ( partition by   ac.ActivationID   order by if(isnull(sum(Forecast_Volume)),0, sum(Forecast_Volume)) desc ) act_rank

  from  Action ac 
  left join calendar st on  year(ac.start_orders)*10000 +month(ac.start_orders)*100 + day(ac.start_orders)  = st.DateKey
  left join calendar ed on  year(ac.end_orders)*10000 +month(ac.end_orders)*100 + day(ac.end_orders)  = ed.DateKey
  left join calendar cl on  year(ac.end_action)*10000 +month(ac.end_action)*100 + day(ac.end_action) = cl.DateKey
  where 
  year(ac.start_orders) >=2015 --!!!
  and ac.DeliveryType = 'DSD'
  group by 
  ac.ActivationID, ac.Mechanik, ac.In_voice, ac.Format, ac.end_action , ac.begin_action ,ac.Mechanik_new, ac.start_orders, ac.end_orders , st.weekid, ed.weekid , ac.DeliveryType, cl.DayOfWeek_Num, ac.Client
  ) tab
  where tab.act_rank = 1  and tab.length_promo = 1 and tab.DayOfWeek_Num = 3 and tab.client = 'METRO'
  
  )METRO_super_Wednesday_info 
  on 
  s.activation_code = METRO_super_Wednesday_info.activation_id    
  
  left join 
  (
  select distinct Activ_id,  Comment ,
  if ( position("Лок", Comment ) > 0 ,substring (Comment, position("Лок", Comment ),  +3), null ) Lok ,   
  if ( position("ОТП", Comment ) > 0 ,substring (Comment, position("ОТП", Comment ),  +3), null ) OTP , 
  if ( position("МТП", Comment ) > 0 ,substring (Comment, position("МТП", Comment ),  +3), null ) MTP 
  from rp_patch 
  ) 
  rp on s.activation_code = rp.Activ_id
  
  left join 
  
  promo_activities_of_Competitors_info pac 
  on 
  s.plant = pac.plant and 
  s.lead_sku = pac.lead_sku and
  s.calendar_yearweek = pac.calendar_yearweek 
  
  left join 
  
   (
    select 
    activation_code, plant, lead_sku, customer_planning_group, distance_between_promotions
    from (
    select * , row_number() over (partition by activation_code, plant, lead_sku, customer_planning_group order by distance_between_promotions   ) row_num
    from distance_between_promotions_info 
    ) tab
    where row_num = 1
    ) dbp 
  
  on 
  s.activation_code = dbp.activation_code and
  s.plant = dbp.plant and 
  s.lead_sku = dbp.lead_sku and
  --s.calendar_yearweek = dbp.calendar_yearweek and
  s.customer_planning_group = dbp.customer_planning_group
  
  left join uplift_info upi 
  
  on 
  s.activation_code = upi.activation_code and
  s.calendar_yearweek = upi.calendar_yearweek and
  s.customer_planning_group = upi.customer_planning_group and
  s.lead_sku = upi.lead_sku and 
  s.format = upi.format and 
  s.active_address = upi.active_address
  

""")
hu.createOrReplaceTempView("promo_main_info")

// COMMAND ----------

val hu = spark.sql(""" 
select distinct tab1.*, 1 as check
from (
select calendar_yearweek, customer_planning_group, format, lead_sku, active_address, activation_code
from future_uplift 
group by calendar_yearweek, customer_planning_group, format, lead_sku, active_address, activation_code
) tab1

inner join 
(
select calendar_yearweek, customer_planning_group, format, lead_sku, active_address, activation_code
from promo_main_info 
group by calendar_yearweek, customer_planning_group, format, lead_sku, active_address, activation_code

) tab2
on 
tab1.calendar_yearweek = tab2.calendar_yearweek and 
tab1.customer_planning_group = tab2.customer_planning_group and 
tab1.format = tab2.format and 
tab1.lead_sku = tab2.lead_sku and 
tab1.active_address = tab2.active_address and 
tab1.activation_code = tab2.activation_code 
""" )
hu.createOrReplaceTempView("future_uplift_filter")


// COMMAND ----------

val query_full = """
  select * from promo_main_info
  union 
  select * 
  from 
  (
  select tab1.*
  from future_uplift tab1 left join future_uplift_filter tab2 on 
  tab1.calendar_yearweek = tab2.calendar_yearweek and 
  tab1.customer_planning_group = tab2.customer_planning_group and 
  tab1.format = tab2.format and 
  tab1.lead_sku = tab2.lead_sku and 
  tab1.active_address = tab2.active_address and 
  tab1.activation_code = tab2.activation_code 
  where tab2.check  is null
  )
  
  """

// COMMAND ----------

val sqldf_full = spark.sql(query_full)

// COMMAND ----------

//result export to ETL\Result

// COMMAND ----------

def exportToBlobStorage_Baltika: String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure"   

try {

sqldf_full.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , writePath+"/"+fname)   
dbutils.fs.rm(readPath , recurse = true) 
if (row_count > 0) Result = "Success" else println("The file " +writePath+"/"+fname + " is empty !" )
} 
catch {
  case e:FileNotFoundException => println("Error, " + e)
  case e:WorkflowException  => println("Error, " + e)
}
  
  Result
  
}

//dbutils.notebook.exit(Result)



// COMMAND ----------

//export to CAP

// COMMAND ----------

val query_incremental = s"""
select calendar_yearmonth as partition_name, *
from (
  select * from promo_main_info
  union 
  select * 
  from 
  (
  select tab1.*
  from future_uplift tab1 left join future_uplift_filter tab2 on 
  tab1.calendar_yearweek = tab2.calendar_yearweek and 
  tab1.customer_planning_group = tab2.customer_planning_group and 
  tab1.format = tab2.format and 
  tab1.lead_sku = tab2.lead_sku and 
  tab1.active_address = tab2.active_address and 
  tab1.activation_code = tab2.activation_code 
  where tab2.check  is null
  )
) tab  
""" + 
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth >= year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 """ 
 else ""}

// COMMAND ----------

val sqldf_incremental = spark.sql(query_incremental)

// COMMAND ----------

def exportToBlobStorage (type_of_ETL:Int): String = { 

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure" 
val partition_field = "partition_name"
val export_format = "com.databricks.spark.csv"
val export_delimiter = Character.toString(7.toChar)

var readPath_ETL = if (type_of_ETL == 0) readPath else if (type_of_ETL == 1) readPath_GBS else null
var writePath_ETL= if (type_of_ETL == 0) writePath_СAP else if (type_of_ETL == 1) writePath_GBS else null

try {
  sqldf_incremental
  .coalesce(1)
  .write.mode("overwrite")
  .format(export_format)
  .option("header", "true")
  .option("inferSchema", "true")
  .option("delimiter", export_delimiter)
  .partitionBy(partition_field)
  .save(readPath_ETL)

  val name : String = "part-00000"   
  val path_list : Seq[String] = dbutils.fs.ls(readPath_ETL).map(_.path).filter(_.contains(partition_field))

  for (path <- path_list) {
   var partition_name = path.replace(readPath_ETL + "/" + partition_field + "=", "").replace("/", "")
   var file_list : Seq[String] = dbutils.fs.ls(path).map(_.path).filter(_.contains(name)) 
   var read_name =  if (file_list.length >= 1 ) file_list(0).replace(path + "/", "") 
   var fname = "PROMODIRECT_" + partition_name + "_RU_DCD"+ ".csv" 
   dbutils.fs.mv(read_name.toString , writePath_ETL+"/"+fname) 
    }
  dbutils.fs.rm(readPath_ETL , recurse = true) 
  Result = "Success" 
  } 
catch {
    case e:FileNotFoundException => println("Error, " + e)
    case e:WorkflowException  => println("Error, " + e)
  }

  Result
}



// COMMAND ----------

val Result = 
if (type_of_ETL == 0) { if ( exportToBlobStorage_Baltika == "Success" && exportToBlobStorage(0) == "Success") "Success"  else "Failure"  }
else if (type_of_ETL == 1) exportToBlobStorage(1)
else if (type_of_ETL == 2) { if ( exportToBlobStorage_Baltika == "Success" &&  exportToBlobStorage(0) == "Success" && exportToBlobStorage(1) == "Success") "Success" else "Failure" }
else "Unexpected parameter"

dbutils.notebook.exit(Result)