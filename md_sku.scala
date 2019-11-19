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
val fname = "MD_SKU_RU.csv"

// COMMAND ----------

//MD_SKU_TO.csv 

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/MD_SKU_TO.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU_TO_TV")

// COMMAND ----------

val sqldf = spark.sql("""  

select lead_sku, int(unit_per_pack) as  unit_per_pack, global_bev_cat_name, base_unit_of_measure_characteristic, sku_volume_in_litres, alcohol_percentage, int(number_of_base_units_per_pallet) as number_of_base_units_per_pallet , brand_name, subbrand_name, 
if(price_segment = '' or isnull(price_segment) , 'не определено', price_segment ) price_segment

from (

select  
int(regexp_replace(if(left(FCBaseWareId,1) = "=", replace(FCBaseWareId,left(FCBaseWareId,1), ''), FCBaseWareId ), "[^0-9.]", ""  ))as lead_sku, 
if (MultipackTypeId = 'Single', 1, PouringVolume/TareVolume ) as unit_per_pack,
ProductCategoryId as global_bev_cat_name,
'L' as base_unit_of_measure_characteristic,
PouringVolume as sku_volume_in_litres,
Alcohol as alcohol_percentage,
QuantitySKUPan  as number_of_base_units_per_pallet,
BrandName as brand_name,
GeneralizedName as subbrand_name,
ProductSegmentName as price_segment, 

row_number() over ( partition by  int(regexp_replace(if(left(FCBaseWareId,1) = "=", replace(FCBaseWareId,left(FCBaseWareId,1), ''), FCBaseWareId ), "[^0-9.]", ""  ))  
order by   PouringVolume, Alcohol, if (QuantitySKUPan = "\\N" or QuantitySKUPan = 0 , 1000000, QuantitySKUPan)   ) key2

From (

select 
if (SKUId= "\\N" or isnull(SKUId), 0,SKUId) as  SKUId , 
--SKUId,
if (SKUIdGeneralized = "\\N" or isnull(SKUIdGeneralized) , '', SKUIdGeneralized) as SKUIdGeneralized, 
--SKUIdGeneralized,
if (BrandName = "\\N" or isnull(BrandName), '',  BrandName) as BrandName,
--BrandName, 
if (GeneralizedName = "\\N" or isnull(GeneralizedName), '' ,GeneralizedName) as GeneralizedName, 
--GeneralizedName, 
if (FCBaseWareId= "\\N" or isnull(FCBaseWareId), 0,FCBaseWareId) as FCBaseWareId ,
--FCBaseWareId,
if (FCBaseWareName = "\\N" or isnull(FCBaseWareName) , '',  FCBaseWareName) as FCBaseWareName ,
--FCBaseWareName,
if (MultipackTypeId =  "\\N" or isnull (MultipackTypeId), '',  MultipackTypeId) as MultipackTypeId,
--MultipackTypeId,
if (ProductCategoryId = "\\N" or isnull (ProductCategoryId) , '',  ProductCategoryId) as ProductCategoryId,
---ProductCategoryId,

if (ProductSegmentName = "\\N" or isnull (ProductSegmentName) , '',  ProductSegmentName) as ProductSegmentName,
--if (ServiceableLife = "\\N" or isnull (ServiceableLife) , '',  ServiceableLife) as ServiceableLife,

if (isnull(float(PouringVolume)), 0,  round(float(PouringVolume),2)) as  PouringVolume, 
if (isnull(float(Alcohol)), 0,  float(Alcohol)) as Alcohol,
if (isnull(float(QuantitySKUPan)), 0,  float(QuantitySKUPan)) as  QuantitySKUPan,
if (isnull(float(TareVolume)), 0, float(TareVolume)) as TareVolume ,

 --float(PouringVolume)  as  PouringVolume, 
 --float(Alcohol) as Alcohol,
 --float(QuantitySKUPan) as  QuantitySKUPan,
 --float(TareVolume) as TareVolume
 
row_number() over ( partition by  SKUId  order by   if (QuantitySKUPan = "\\N" or QuantitySKUPan = 0 , 1000000, QuantitySKUPan) ) key

from MD_SKU_TO_TV 

group by SKUId, SKUIdGeneralized, BrandName, GeneralizedName, FCBaseWareId,FCBaseWareName, MultipackTypeId, ProductCategoryId, PouringVolume, Alcohol, QuantitySKUPan, TareVolume, ProductSegmentName
) tab
where tab.key = 1
) tab
where tab.key2 = 1 and lead_sku <> 0


""")

sqldf.createOrReplaceTempView("result_source_table")

// COMMAND ----------

val sqldf = spark.sql("""  

select lead_sku, shelf_life
from (
select lead_sku, shelf_life, row_number() over (partition by lead_sku order by shelf_life desc ) key
from (
select distinct 
int(regexp_replace(if(left(FCBaseWareId,1) = "=", replace(FCBaseWareId,left(FCBaseWareId,1), ''), FCBaseWareId ), "[^0-9.]", ""  ))as lead_sku,
if(shelf_life = 0, null, shelf_life) shelf_life
from (

select 
if (FCBaseWareId= "\\N" or isnull(FCBaseWareId), 0,FCBaseWareId) as FCBaseWareId  ,  
int(if (trim(replace(ServiceableLife,int(regexp_replace(ServiceableLife, "[^0-9.]", ""  )), '' )) = 'месяцев', 30 , 1)) * int(regexp_replace(ServiceableLife, "[^0-9.]", ""  )) shelf_life 
from MD_SKU_TO_TV
group by FCBaseWareId, ServiceableLife
  ) tab 
    ) tab
) tab 
where key = 1

""" )
sqldf.createOrReplaceTempView("lifeinfo")

// COMMAND ----------

val sqldf = spark.sql(""" 

select lead_sku, status
from (
select lead_sku, status, check, row_number() over (partition by lead_sku order by check desc ) key
from (
select distinct 
int(regexp_replace(if(left(FCBaseWareId,1) = "=", replace(FCBaseWareId,left(FCBaseWareId,1), ''), FCBaseWareId ), "[^0-9.]", ""  ))as lead_sku,
status, check 
from (

select 
if (FCBaseWareId= "\\N" or isnull(FCBaseWareId), 0,FCBaseWareId) as FCBaseWareId  ,  
  if (MatGlobStatDesc = "\\N", 'Not defined', if (MatGlobStatDesc = 'active', 'Active', MatGlobStatDesc)) status , 
  if (MatGlobStatDesc in ('Out Phasing', 'NPD', 'Active NPD', 'Active'), 1 , 0) check
from MD_SKU_TO_TV
group by FCBaseWareId, MatGlobStatDesc
  ) tab 
    ) tab
) tab 
where key = 1

""" )
sqldf.createOrReplaceTempView("status_info")


// COMMAND ----------

val sqldf = spark.sql(""" 
select r.*, i.shelf_life, s.status
from result_source_table r 
left join lifeinfo i on  r.lead_sku = i.lead_sku
left join status_info s on r.lead_sku = s.lead_sku
""") 
sqldf.createOrReplaceTempView("result")

// COMMAND ----------

//result export

// COMMAND ----------

import com.databricks.WorkflowException
import java.io.FileNotFoundException

var Result = "Failure"   

try {
val sqldf = spark.sql(
  """
  select * from result 
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

