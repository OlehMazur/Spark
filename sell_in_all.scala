// Databricks notebook source
//Configuration
val storage_account_name = "staeeprodbigdataml2c"
val storage_account_access_key = "EHYumrwso4XLSUHpvLptI33z7mumiZwZOErjrlP8FiW51Bb6NS2PaWJsqW9hsMttbZizgQjUexFZfZDBQJebYw=="
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".blob.core.windows.net",
  storage_account_access_key)

// COMMAND ----------

//constants

// COMMAND ----------

val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/test_res.csv"
val writePath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/Result" //ETL/Result //etl_fbkp
val fname = "Sell_in_RU_p2_with_formats_All.csv"
val file_location_path = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/"

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

val file_location = file_location_path +"MD_Clients.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_Clients")

// COMMAND ----------

//Sell-in

// COMMAND ----------

val file_location = file_location_path + "Sell_in_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Orders_Main")

// COMMAND ----------

//Divisions

// COMMAND ----------

val file_location = file_location_path +"Division.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Divisions")

// COMMAND ----------

//PlantID

// COMMAND ----------

val file_location = file_location_path+ "PlantID.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("plant_id_info")

// COMMAND ----------

//Actionid_wheader

// COMMAND ----------

val file_location = file_location_path + "Action_MT_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Action")

// COMMAND ----------

//CPG_Formats

// COMMAND ----------

val file_location = file_location_path + "CPG_Formats.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("CPG_Formats")

// COMMAND ----------

//R&W_MBO

// COMMAND ----------

val file_location = file_location_path + "R&W_MBO.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MBO")

// COMMAND ----------

//seas_coef

// COMMAND ----------

val file_location = file_location_path + "seas_coef.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("seas_coef")

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

//sell-in with formats

// COMMAND ----------

 val sqldf = spark.sql( """
 select 
 tab.MonthId, tab.WeekId, tab.Client, tab.ActiveAdress,tab.AdressID, tab.SKU_Lead_ID, tab.Promo_Marker, sum (tab.Actual_Volume) as Actual_Volume
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
            group by  tab.MonthId, tab.WeekId, tab.Client, tab.AdressID, tab.ActiveAdress, tab.SKU_Lead_ID, tab.Promo_Marker
            """)
sqldf.createOrReplaceTempView("input_source_table")

// COMMAND ----------

val sqldf = spark.sql(
  """
select 
calendar_yearmonth, calendar_yearweek,  lead_sku, active_sku,  plant_name,  plant, division, customer_planning_group, format, concat(customer_planning_group, '_', format) as cpg_format,
sum (total_shipments_volume_hl) total_shipments_volume_hl

from (

select tab.Year_Month calendar_yearmonth, tab.Year_Week calendar_yearweek, replace (replace (tab.Lead_SKUID ,51070074, 510700 ) ,38070074, 380700 ) lead_sku, 
if(isnull(acts.SKU_Lead_ID), 0, 1) active_sku,  tab.ActiveAdress plant_name, plant_id_info.Plant_ID plant, Divisions.Division division, tab.Client customer_planning_group, 

if (
tab.Client = 'X5 Retail Group' or tab.Client = 'Магнит (Тандер)' ,  
  if(cl.New2 = 'СМ/ГМ', 'Супермаркет', if(isnull(cl.New2) or cl.New2 = "\\N", 'Минимаркет', cl.New2 )) , 
  if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
  ) format , 

tab.AdressID, tab.Actual_Volume/10 total_shipments_volume_hl 

from (

select source.Year_Month, source.Year_Week, source.Lead_SKUID, source.ActiveAdress, source.Client, source.AdressID, sum (source.Actual_Volume) Actual_Volume

from (

      select 
      date_info.MonthId  Year_Month,
      date_info.cl_Week Year_Week ,     
      date_info.Lead_SKUID_int Lead_SKUID,
      ActiveAddress_ID_info.ActiveAddress_ID,
      date_info.ActiveAdress,
      date_info.AdressID,
      date_info.Client, 
      if (isnull(orders.Actual_Volume), 0,orders.Actual_Volume) as Actual_Volume , 
      if (isnull(orders.Promo_Volume), 0, orders.Promo_Volume) as Promo_Volume,
      key_promo_info.Activation_Code,
      city_info.City
      /*[start block] year_month, year_week with unique combination of lead SKU, ActiveAddress, Client, Brand */
      from 
      (
          select distinct 
          date_info.MonthId, 
          date_info.WeekId cl_Week,   
          date_info.ActiveAdress,  
          date_info.AdressID,  
          date_info.Client, 
          date_info.Lead_SKUID_int 
         
          from (
                  select  
                  DateKey, 
                  MonthId , 
                  WeekId, 
                  keys_info.ActiveAdress,  
                  keys_info.AdressID,
                  keys_info.Client, 
                  keys_info.Lead_SKUID_int,        
                  
                  cast (concat(left(DateKey, 4), '-', 
                  left(replace(DateKey, left(DateKey, 4), ''), 2),'-', left(replace(DateKey, left(DateKey, 6), ''), 2)) as date)date
                  from Calendar     
                  cross join 
                  (
                  select  
                  cl.ActiveAdress, 
                  o.AdressID,
                  o.Client , 
                  ls.Lead_SKU_ID as Lead_SKUID_int 
                  
                  from orders_main o 
                  left join (select distinct cast (SKU_ID as int) SKU_ID, cast(SKU_Lead_ID as int) Lead_SKU_ID from md_sku) ls 
                  on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = ls.SKU_ID 
                  left join md_clients cl 
                  on o.AdressID =cl.AdressCode 
                  left join (select cast (SKU_ID as int) SKU_ID_Int from  md_sku) md_sku 
                  on  ls.Lead_SKU_ID = md_sku.SKU_ID_Int   
                  group by ls.Lead_SKU_ID,  cl.ActiveAdress,  o.Client , o.AdressID
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
      select  orders.MonthId, orders.WeekId, orders.Client, orders.ActiveAdress, orders.Lead_SKU_ID , orders.Lead_SKU_ID_Int, orders.AdressID,
      sum(orders.Actual_Volume) Actual_Volume , sum( orders.Promo_Volume) Promo_Volume
      from (

      select tab_main.MonthId,  tab_main.WeekId, tab_main.Client, tab_main.ActiveAdress,tab_main.AdressID, tab_main.SKU_Lead_ID Lead_SKU_ID ,
      cast(tab_main.SKU_Lead_ID as int) Lead_SKU_ID_Int, 
      if(isnull(sum (tab_main.Actual_Volume)) , 0, sum (tab_main.Actual_Volume)) as Actual_Volume, 
      if(isnull(sum (tab_main.Actual_Volume_With_Promo)) , 0, sum (tab_main.Actual_Volume_With_Promo)) as Promo_Volume
      from (

        select tab1.*, tab2.Actual_Volume_With_Promo
        from 
        (
          select MonthId,  WeekId,  Client, ActiveAdress, AdressID, SKU_Lead_ID, Promo_Marker, Actual_Volume from input_source_table       
         ) tab1

        left join 

        (
        select MonthId,  WeekId,  Client, ActiveAdress, AdressID, SKU_Lead_ID, Promo_Marker, Actual_Volume as Actual_Volume_With_Promo from input_source_table where Promo_Marker = 1
        ) tab2
        on 
         tab1.MonthId = tab2.MonthId and tab1.WeekId = tab2.WeekId  and tab1.Client = tab2.Client and tab1.ActiveAdress = 
        tab2.ActiveAdress and tab1.AdressID = tab2.AdressID and tab1.SKU_Lead_ID =tab2.SKU_Lead_ID and tab1.Promo_Marker = tab2.Promo_Marker 

          ) tab_main 

          group by  tab_main.MonthId, tab_main.WeekId,  tab_main.Client, tab_main.ActiveAdress, tab_main.AdressID, tab_main.SKU_Lead_ID 

        ) orders
        group by orders.MonthId, orders.WeekId,  orders.Client, orders.ActiveAdress, orders.Lead_SKU_ID , orders.Lead_SKU_ID_Int, orders.AdressID

      ) orders 
      on 
      date_info.MonthId = orders.MonthId and
      date_info.cl_Week = orders.WeekId and
      date_info.Lead_SKUID_int = orders.Lead_SKU_ID_int and 
      date_info.ActiveAdress = orders.ActiveAdress and
      date_info.Client = orders.Client and 
      date_info.AdressID = orders.AdressID
      /*[end block] orders with actual volume and promo volume */ 

      left join 
      /*[start block] promo code */ 
      (
      select tab.MonthId, tab.WeekId,  tab.Client, tab.ActiveAdress, tab.SKU_Lead_ID, tab.Promo_Ded_ID Activation_Code , 
      cast(tab.SKU_Lead_ID as int) Lead_SKU_ID_Int, tab.AdressID
      from (
            select calendar.MonthId, calendar.WeekId, o.Client, cl.ActiveAdress , s.SKU_Lead_ID, o.Actual_Volume,
            o.Promo_Ded_ID, o.AdressID,
            row_number() over (partition by  calendar.MonthId, calendar.WeekId,  o.Client, s.SKU_Lead_ID, cl.ActiveAdress, o.AdressID  order by o.Actual_Volume desc ) as key_promo
            from orders_main o left join md_clients cl on o.AdressID =cl.AdressCode 
            left join md_sku s on cast(replace(o.SKUID, left(o.SKUID, 1), '') as int) = cast(s.SKU_ID as int)
            --left join lead_sku ls on s.SKUID = ls.SKU_ID
            left join calendar on year(o.Date)*10000 +month(o.Date)*100 + day(o.Date)  = calendar.DateKey
            where if(isnull(int(o.Promo_Ded_ID)), 0,1) > 0 and o.Actual_Volume > 0 
            ) tab
      where tab.key_promo = 1      
      ) key_promo_info
      on
      date_info.MonthId = key_promo_info.MonthId and
      date_info.cl_week = key_promo_info.WeekId and
      date_info.Lead_SKUID_int = key_promo_info.Lead_SKU_ID_int and 
      date_info.ActiveAdress = key_promo_info.ActiveAdress and
      date_info.Client = key_promo_info.Client and 
      date_info.AdressID = key_promo_info.AdressID
      /*[end block] promo code */ 

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

group by  source.Year_Month, source.Year_Week, source.Lead_SKUID, source.ActiveAdress, source.Client, source.AdressID
having (Actual_Volume) > 0

) tab

left join plant_id_info on tab.ActiveAdress = plant_id_info.ActiveAdress
left join Divisions on Divisions.WH = tab.ActiveAdress
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

left join md_clients cl on tab.AdressID =cl.AdressCode 

left join CPG_Formats cpgf on  tab.Client = cpgf.CPG

where 
tab.Year_Week >= 201601  and plant_id_info.ActiveAdress <> 'Нет активного адреса'  

) main

group by 
calendar_yearmonth, calendar_yearweek,  lead_sku, active_sku,  plant_name,  plant, division, customer_planning_group, format 
  
  """
  )

sqldf.createOrReplaceTempView("result_source_table")


// COMMAND ----------

//MBO

// COMMAND ----------

val sqldf = spark.sql("""
select distinct st.WeekId calendar_yearweek, m.CPG customer_planning_group  , s.SKU_Lead_ID lead_sku , 1 as MBO
from MBO m 
left join calendar st 
on  
st.DateKey >=  year(to_date(m.start_shipment, 'dd.MM.yyyy'))*10000 +month(to_date(m.start_shipment, 'dd.MM.yyyy'))*100 + day(to_date(m.start_shipment, 'dd.MM.yyyy')) 
and
st.DateKey <=  year(to_date(m.end_shipment, 'dd.MM.yyyy'))*10000 +month(to_date(m.end_shipment, 'dd.MM.yyyy'))*100 + day(to_date(m.end_shipment, 'dd.MM.yyyy'))
left join MD_SKU s on s.SKU_ID = int(if(left(m.sku_code,1) = "=", replace(m.sku_code,left(m.sku_code,1), ''), m.sku_code ))
""")
sqldf.createOrReplaceTempView("MBO_info")


// COMMAND ----------

//result export

// COMMAND ----------

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure"   

try {
val sqldf = spark.sql(
  """
  select distinct
  
  s.* , if (MBO_info.MBO is null, 0, MBO_info.MBO) mbo_programs , replace(coalesce(seas_coef_info.Seas_coef , seas_coef_info_core.Seas_coef), ',', '.' ) seas_coef
  
  from result_source_table s
  
  left join MBO_info 
  on 
  s.customer_planning_group = MBO_info.customer_planning_group and 
  s.lead_sku = MBO_info.lead_sku and
  s.calendar_yearweek = MBO_info.calendar_yearweek 
  
  left join 
  
  (
   select distinct lead_sku, subbrand_name subbrand  from MD_SKU_RU
   ) Subbrand_info
  
   on s.lead_sku = Subbrand_info.lead_sku
  
  left join 
  
   (
   select distinct MonthId, int(right(MonthId, 2))  month_num
   from calendar 
   ) month_num_info
  
   on s.calendar_yearmonth = month_num_info.MonthId

   left join 
   
   (select distinct s.Subbrand,  s.Month, s.Seas_coef, d.Division division_correct 
   from seas_coef s left join divisions d on s.Division = d.Region_Bi
   ) seas_coef_info 
   on 

   Subbrand_info.Subbrand = seas_coef_info.Subbrand and 
   s.division = seas_coef_info.division_correct and
   month_num_info.month_num = seas_coef_info.Month 
  
   left join 
   (select distinct  s.Month, s.Seas_coef, d.Division division_correct 
   from seas_coef s left join divisions d on s.Division = d.Region_Bi
   where s.Category = 'Core' 
   ) seas_coef_info_core 
   on
  
   s.division = seas_coef_info_core.division_correct and
   month_num_info.month_num = seas_coef_info_core.Month 
  
  """
  )
sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

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

dbutils.notebook.exit(Result)

