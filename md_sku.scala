// Databricks notebook source
// MAGIC %run /Users/o_mazur@carlsberg.ua/dcd_etl_functions

// COMMAND ----------

//util variables

// COMMAND ----------

val job = "dcd_notebook_workflow_hfa_All"
val notebook = "md_sku"
val notebook_start_time  = LocalDateTime.now
val fname = "MD_SKU_RU.csv"
val readPath = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/" + job
val readPath_GBS = "wasbs://dcd@prdcbwesa01.blob.core.windows.net/RU/ru_tmp/" +job

// COMMAND ----------

//Init log DB  

// COMMAND ----------

// MAGIC %sql use etl_info

// COMMAND ----------

//start logging

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

//MD_SKU_TO.csv 

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "MD_SKU_TO.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU_TO_TV")

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
ProductSegmentName as price_segment

From (

select 
--if (SKUId= "\\N" or isnull(SKUId), 0,SKUId) as  SKUId , 
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
 
row_number() over ( partition by  int(regexp_replace(if(left(FCBaseWareId,1) = "=", replace(FCBaseWareId,left(FCBaseWareId,1), ''), FCBaseWareId ), "[^0-9.]", ""  )) 
order by  PouringVolume desc, Alcohol desc, TareVolume desc, BrandName desc, GeneralizedName desc, MultipackTypeId desc, if (QuantitySKUPan = "\\N" or QuantitySKUPan = 0 , 1000000, QuantitySKUPan) 
) key

from MD_SKU_TO_TV 

group by /*SKUId,*/ SKUIdGeneralized, BrandName, GeneralizedName, FCBaseWareId,FCBaseWareName, MultipackTypeId, ProductCategoryId, PouringVolume, Alcohol, QuantitySKUPan, TareVolume, ProductSegmentName
) tab
where tab.key = 1 
) tab
where lead_sku <> 0
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
//sqldf.createOrReplaceTempView("result")

// COMMAND ----------

//Final testing before result export

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fname, sqldf, readPath)

// COMMAND ----------

val sqldf_result = load_preliminary_result(fname)

// COMMAND ----------

save_to_log_start_result_testing(job,notebook)

// COMMAND ----------

if (Test_Number_of_Rows(job,notebook, sqldf_result, fname) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result, "lead_sku", fname ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

//end Final testing before result export

// COMMAND ----------

val Result = 
if (type_of_ETL == 0) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname, sqldf, readPath ) == "Success" &&      
      exportToBlobStorage_CAP_hole_file (job,notebook,0, sqldf, "SKUSCLEAN" , readPath, readPath_GBS) == "Success" 
      )  "Success" else "Failure"  }
else if (type_of_ETL == 1) { 
  if (exportToBlobStorage_CAP_hole_file (job,notebook,1, sqldf, "SKUSCLEAN" , readPath, readPath_GBS) == "Success"
     ) "Success" else  "Failure"  }
else if (type_of_ETL == 2) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname, sqldf, readPath  ) == "Success" && 
      exportToBlobStorage_CAP_hole_file (job,notebook,1, sqldf, "SKUSCLEAN" , readPath, readPath_GBS ) == "Success"  
     ) "Success" else "Failure" }
else "Unexpected parameter"


//save to log
save_to_log_last_step(job,notebook,Result,notebook_start_time)

dbutils.notebook.exit(Result)