// Databricks notebook source
// MAGIC %run /Users/o_mazur@carlsberg.ua/dcd_etl_functions

// COMMAND ----------

spark.conf.set( "spark.sql.shuffle.partitions", 100)

// COMMAND ----------

//util variables

// COMMAND ----------

val job = "dcd_notebook_workflow_hfa_All"
val notebook = "hfa_all"
val notebook_start_time  = LocalDateTime.now

val writePath_tmp = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp"
val fname = "HFA_RU_All.csv"
val fname_tmp = "HFA_RU_tmp_All.csv"
val fname_tmp_promo = "HFA_RU_tmp_All_promo.csv"
val fname_tmp_uplift = "HFA_RU_tmp_All_promo_uplift.csv"
val file_location_path_tmp = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp"
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

//numbers of week back from current date, if "0" - current week

// COMMAND ----------

val numbers_of_weeks_back: Int = 0

// COMMAND ----------

//numbers of week ahead from current date

// COMMAND ----------

val numbers_of_weeks_ahead: Int = 78

// COMMAND ----------

val source_file_name = "CPG_rename.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("CPG_rename")

// COMMAND ----------

//log for source files

// COMMAND ----------

//Sell-in

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

//numbers of week ahead from current date

// COMMAND ----------

val mv  =LocalDateTime.now.plusWeeks(-1 * numbers_of_weeks_back)
val current_minus_n_weeks = mv.getYear * 10000 + mv.getMonthValue *100 +mv.getDayOfMonth
val query_mv = s"select DayName as report_date, WeekId from Calendar where datekey = $current_minus_n_weeks" 
val sqldf_mv = spark.sql(query_mv)
sqldf_mv.createOrReplaceTempView("report_week_info")

// val pp  =LocalDateTime.now.plusWeeks(numbers_of_weeks_ahead)
// val current_plus_n_weeks = pp.getYear * 10000 + pp.getMonthValue *100 +pp.getDayOfMonth
// val query = s"select WeekId from Calendar where datekey = $current_plus_n_weeks" 
// val sqldf = spark.sql(query)
// sqldf.createOrReplaceTempView("till_week_info")

val pp  = mv.plusWeeks(numbers_of_weeks_ahead)
val current_plus_n_weeks = pp.getYear * 10000 + pp.getMonthValue *100 +pp.getDayOfMonth
val query = s"select WeekId from Calendar where datekey = $current_plus_n_weeks" 
val sqldf = spark.sql(query)
sqldf.createOrReplaceTempView("till_week_info")


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

//MD_SKU_To

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "MD_SKU_TO.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MD_SKU_TO")

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

//MD_Clients

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name =  "MD_Clients.csv"
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

//Divisions

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "Division.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Divisions")

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

 //PlantID

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

//ETL/Result/Sell_in_RU_p2_with_formats.csv  !!! etl_fbkp for Test

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

//PromoDSD.csv

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "PromoDSD_All.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("PromoDSD")

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
week_forecast,
SKU_name,
SGP,
if (isnull(r.CPG_new), cl.Client_name, r.CPG_old )  Client_name,
Region,
Format_TT,
ActivationID,
Begin_delivery,
End_Delivery,
BL_week,
Promo_week,
SKUID
from PromoDSD cl left join  (select distinct * from  CPG_rename ) r on cl.Client_name = r.CPG_new
""")                      
sqldf.createOrReplaceTempView("PromoDSD")

// COMMAND ----------

//Action_MT

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "Action_MT_All.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Action")

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
SKUID,
Mechanik,
Start_Orders,
End_Orders,
Begin_action,
End_action,
In_voice,
ActivationID,
Format,
DeliveryType,
Region,
OP,
REP,
if (isnull(r.CPG_new), cl.Client, r.CPG_old )  Client,
Shelf_Discount,
Discount,
Actual_Volume,
Forecast_Volume,
Price,
Mechanik_new

from Action cl left join  (select distinct * from  CPG_rename ) r on cl.Client = r.CPG_new
""")                      
sqldf.createOrReplaceTempView("Action")

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "PromoDRP.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("PromoDRP")

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
Date,
SGP,
SKUID,
SKU,
if (isnull(r.CPG_new), cl.Client_Name, r.CPG_old )  Client_Name,
Format,
Activ_code,
Whs_FC_vol
from PromoDRP cl left join  (select distinct * from  CPG_rename ) r on cl.Client_Name = r.CPG_new
""")                      
sqldf.createOrReplaceTempView("PromoDRP")

// COMMAND ----------

//ActiveAdress_SGP.csv

// COMMAND ----------

// val start_time = LocalDateTime.now

// val is_regularly_updated_table = false
// val source_file_name = "ActiveAdress_SGP.csv"
// val file_location = source_file_location + source_file_name
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
// df.createOrReplaceTempView("ActiveAdress_SGP")

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

// //some tables are not updating on a regular base
// if (!get_last_modified_date_for_blob(source_file_name).equals(current_date) && is_logic_changed == 0 && !is_regularly_updated_table  ) {
//   save_to_log_if_source_is_not_updated(job,notebook,start_time, source_file_name )
// }



// COMMAND ----------

//Fc_Hist

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

//RP_patch

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "RP_Patch.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("RP_Patch")

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

//R&W_MBO

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name = "R&W_MBO.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("MBO")

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

//seas_coef

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name = "seas_coef.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("seas_coef")

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

//MD_SKU_RU

// COMMAND ----------

//val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "MD_SKU_RU.csv"
val file_location = writePath +"/" + source_file_name
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

//Auto_Orders

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name = "Auto_Orders.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("Auto_Orders")

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

//CPG_Formats

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name ="CPG_Formats.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("CPG_Formats")

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

//seas_sku

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = false
val source_file_name ="seas_sku.csv"
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

//SGP_B_A_ActiveAddress

// COMMAND ----------

val start_time = LocalDateTime.now

val is_regularly_updated_table = true
val source_file_name = "SGP_B_A_ActiveAddress.csv"
val file_location = source_file_location + source_file_name
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("SGP_B_A_ActiveAddress")

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

//1.	Выделить все комбинации Plant/CPG/Format/Division/Lead_SKU за последние 3 месяца в файле «Sell-in.csv» + .	Выделить все комбинации Plant/CPG/Format/Division/Lead_SKU из файла «FC_Hist.csv»

// COMMAND ----------

val sqldf = spark.sql(
"""
select distinct 
plant, customer_planning_group, format, division, lead_sku
from Sell_in_RU_p2_with_formats 
where 
calendar_yearmonth >= year(date_add((select report_date from report_week_info),-60))*100 + month(date_add((select report_date from report_week_info),-60)) and
calendar_yearmonth <= year((select report_date from report_week_info))*100 + month((select report_date from report_week_info))

union 

select distinct 
plant_id_info.Plant_ID as plant, fc.client as  customer_planning_group, 

if ( fc.client = 'X5 Retail Group' or fc.client = 'Магнит (Тандер)', 'Минимаркет', 
 if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
) as format, 

d.Division as division , int(if(left(s.SKU_Lead_ID ,1) = "=", replace(s.SKU_Lead_ID ,left(s.SKU_Lead_ID ,1), ''), s.SKU_Lead_ID  )) lead_sku
from Fc_Hist fc left join Divisions d on fc.address = d.WH left join plant_id_info on fc.address = plant_id_info.ActiveAdress
left join MD_SKU s on int(if(left(fc.sku_code,1) = "=", replace(fc.sku_code,left(fc.sku_code,1), ''), fc.sku_code )) = int(if(left(s.SKU_ID,1) = "=", replace(s.SKU_ID,left(s.SKU_ID,1), ''), s.SKU_ID ))
left join CPG_Formats cpgf on fc.client = cpgf.CPG

where fc.client in (select distinct customer_planning_group from Sell_in_RU_p2_with_formats )
"""
  
)
sqldf.createOrReplaceTempView("key_info")

// COMMAND ----------

// MAGIC %sql 
// MAGIC select distinct address
// MAGIC from (
// MAGIC select 
// MAGIC fc.address, fc.sku_code, plant_id_info.ActiveAdress, plant_id_info.Plant_ID as plant, fc.client as  customer_planning_group, 
// MAGIC 
// MAGIC if ( fc.client = 'X5 Retail Group' or fc.client = 'Магнит (Тандер)', 'Минимаркет', 
// MAGIC  if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
// MAGIC ) as format, 
// MAGIC 
// MAGIC d.Division as division , int(if(left(s.SKU_Lead_ID ,1) = "=", replace(s.SKU_Lead_ID ,left(s.SKU_Lead_ID ,1), ''), s.SKU_Lead_ID  )) lead_sku
// MAGIC from Fc_Hist fc left join Divisions d on fc.address = d.WH left join plant_id_info on fc.address = plant_id_info.ActiveAdress
// MAGIC left join MD_SKU s on int(if(left(fc.sku_code,1) = "=", replace(fc.sku_code,left(fc.sku_code,1), ''), fc.sku_code )) = int(if(left(s.SKU_ID,1) = "=", replace(s.SKU_ID,left(s.SKU_ID,1), ''), s.SKU_ID ))
// MAGIC left join CPG_Formats cpgf on fc.client = cpgf.CPG
// MAGIC 
// MAGIC where fc.client in (select distinct customer_planning_group from Sell_in_RU_p2_with_formats )
// MAGIC ) tab
// MAGIC where 
// MAGIC plant is null 

// COMMAND ----------

//2.	Cross join полученного списка с неделями /текущая неделя/ – 522019

// COMMAND ----------

val sqldf = spark.sql("""
select key_info.*, week.WeekId calendar_yearweek, week.MonthId calendar_yearmonth
from key_info cross join 
(select distinct WeekId, MonthId
from Calendar 
where DateKey >= year((select report_date from report_week_info))*10000 + month((select report_date from report_week_info))*100 + day((select report_date from report_week_info))
and WeekId <= (select distinct WeekId from till_week_info) --201952 
)week
 """)
sqldf.createOrReplaceTempView("key_info_with_week")                  

// COMMAND ----------

//history

// COMMAND ----------

val sqldf = spark.sql("""

select  f.calendar_yearmonth, f.calendar_yearweek, f.lead_sku,  f.plant, f.division, f.customer_planning_group, f.format , 
b.total_shipments_volume_hl as total_shipments_volume_hl_initial , if (b.total_shipments_volume_hl <= 10000, b.total_shipments_volume_hl, 10000) total_shipments_volume_hl

from (

select f.calendar_yearmonth, f.calendar_yearweek, f.lead_sku,  f.plant, f.division, f.customer_planning_group, f.format
from Sell_in_RU_p2_with_formats f 
where f.calendar_yearweek < ( select distinct WeekId from Calendar where DateKey =  year((select report_date from report_week_info))*10000 + month((select report_date from report_week_info))*100 + day((select report_date from report_week_info)) )

union 

select distinct 
c.MonthId calendar_yearmonth,  c.WeekId calendar_yearweek, int(if(left(s.SKU_Lead_ID ,1) = "=", replace(s.SKU_Lead_ID ,left(s.SKU_Lead_ID ,1), ''), s.SKU_Lead_ID  )) lead_sku, 
plant_id_info.Plant_ID as plant, d.Division as division , fc.client as  customer_planning_group, 

if ( fc.client = 'X5 Retail Group' or fc.client = 'Магнит (Тандер)', 'Минимаркет', 
  if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )

) as format  

from Fc_Hist fc left join Divisions d on fc.address = d.WH left join plant_id_info on fc.address = plant_id_info.ActiveAdress
left join MD_SKU s on int(if(left(fc.sku_code,1) = "=", replace(fc.sku_code,left(fc.sku_code,1), ''), fc.sku_code )) = int(if(left(s.SKU_ID,1) = "=", replace(s.SKU_ID,left(s.SKU_ID,1), ''), s.SKU_ID ))
left join (select distinct Week, WeekId,MonthId from Calendar) c on date_format(fc.week_date, 'dd.MM.yy')  = c.Week
left join CPG_Formats cpgf on fc.client = cpgf.CPG

where 
fc.client in (select distinct customer_planning_group from Sell_in_RU_p2_with_formats )
and c.WeekId < ( select distinct WeekId from Calendar where DateKey =  year((select report_date from report_week_info))*10000 + month((select report_date from report_week_info))*100 + day((select report_date from report_week_info)) )

) f
left join Sell_in_RU_p2_with_formats b

on 
 f.calendar_yearmonth = b.calendar_yearmonth and
 f.calendar_yearweek = b.calendar_yearweek and
 f.lead_sku =  b.lead_sku and
 f.plant =  b.plant and 
 f.division =  b.division  and
 f.customer_planning_group = b.customer_planning_group and 
 f.format =  b.format
 

""") 
sqldf.createOrReplaceTempView("key_info_with_week_hist")   

// COMMAND ----------

//3.	Подготовить финальный узел как у «open_orders_etl_tuesday_with_format.csv» для периода /текущая неделя/  – 522019 на основе файла «Sell-in.csv», где load week равен прошлой неделе

// COMMAND ----------

val sqldf = spark.sql("""

select lead_sku, customer_planning_group, plant_name, plant, load_week, calendar_yearweek, calendar_yearmonth, division, format, open_orders_promo, open_orders - open_orders_promo as open_orders
from 
(
select 
replace (replace (int(if(left(md_sku.SKU_Lead_ID,1) = "=", replace(md_sku.SKU_Lead_ID,left(md_sku.SKU_Lead_ID,1), ''), md_sku.SKU_Lead_ID )) ,51070074, 510700 ) ,38070074, 380700 ) lead_sku , ow.Client customer_planning_group ,  cl.ActiveAdress plant_name, plant_id_info.Plant_ID plant,   tuesday_info.weekid load_week,  calendar.WeekId calendar_yearweek, calendar.MonthId calendar_yearmonth, dev.division,
sum (if ( ow.Promo_BL = "\\N" or Promo_BL is null, 0, Order_Volume/10 ) ) open_orders_promo, sum (Order_Volume/10) open_orders,
if(tuesday_info.weekid = calendar.WeekId, 1, 0) exclude_week, 

--if(cl.New2 = 'СМ/ГМ', 'Супермаркет', cl.New2) format

if (
ow.Client = 'X5 Retail Group' or ow.Client = 'Магнит (Тандер)' ,  
  if(cl.New2 = 'СМ/ГМ', 'Супермаркет', if(isnull(cl.New2) or cl.New2 = "\\N", 'Минимаркет', cl.New2 )) , 
  if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
  ) format

from orders_wpromo ow left join md_clients cl on ow.AdressID = cl.AdressCode left join plant_id_info on cl.ActiveAdress = plant_id_info.ActiveAdress 
left join md_sku on if ( isnull(cast( left(ow.SKUID,1) as int)), cast(replace(ow.SKUID, left( ow.SKUID, 1), '') as int), cast(ow.SKUID as int) ) = 
    int(if(left(md_sku.SKU_ID,1) = "=", replace(md_sku.SKU_ID,left(md_sku.SKU_ID,1), ''), md_sku.SKU_ID ))
left join Divisions dev on  cl.ActiveAdress = dev.WH left join calendar on year(ow.Date)*10000 +month(ow.Date)*100 + day(ow.Date)  = calendar.DateKey
left join (
  select datekey tuesday, monthid, weekid
  from calendar
  where 
  dayofweek(concat( left(DateKey, 4),  '-',  left(replace(DateKey, left(DateKey, 4), ''), 2) , '-', left(replace(DateKey, left(DateKey, 6), ''), 2))) -1  = 2 --Tuesday/Friday
  and 
   cast (concat( left(DateKey, 4),  '-',  left(replace(DateKey, left(DateKey, 4), ''), 2) , '-', 
            left(replace(DateKey, left(DateKey, 6), ''), 2)) as date) <= (select max(Date) from  orders_wpromo)

) tuesday_info
on 
year(ow.OrderDate)*10000 +month(ow.OrderDate)*100 + day(ow.OrderDate) <= tuesday_info.tuesday 
and tuesday_info.tuesday  < year(ow.Date)*10000 +month(ow.Date)*100 + day(ow.Date)

left join CPG_Formats cpgf on ow.Client = cpgf.CPG

where 
plant_id_info.Plant_ID is not null 
and md_sku.SKU_Lead_ID is not null
--and dev.division in ('C', 'NW')
and if(year(ow.OrderDate)*10000 +month(ow.OrderDate)*100 + day(ow.OrderDate) <= tuesday_info.tuesday and tuesday_info.tuesday < year(ow.Date)*10000 +month(ow.Date)*100 + day(ow.Date), 1, 0) = 1 

and calendar.WeekId >= (select  WeekId from Calendar where DateKey = year((select report_date from report_week_info))*10000 + month((select report_date from report_week_info))*100 + day((select report_date from report_week_info))) 

and tuesday_info.weekid = (select max(WeekId) from Calendar where WeekId  <= ( select distinct WeekId from Calendar where DateKey =  year((select report_date from report_week_info))*10000 + month((select report_date from report_week_info))*100 + day((select report_date from report_week_info)) )) --current or prev week

group by 
md_sku.SKU_Lead_ID, ow.Client, cl.ActiveAdress, plant_id_info.Plant_ID, calendar.WeekId , tuesday_info.weekid, calendar.MonthId, dev.division, cl.New2, cpgf.format
)tab
where tab.exclude_week = 0
""")
sqldf.createOrReplaceTempView("open_orders")   

// COMMAND ----------

//4.	Left join 2. с 3. по Plant/CPG/Format/Division/Lead_SKU/Calendar_yearweek

// COMMAND ----------

val sqldf = spark.sql("""

select key_info_with_week.plant, key_info_with_week.customer_planning_group,  key_info_with_week.format , key_info_with_week.division, key_info_with_week.lead_sku, key_info_with_week.calendar_yearweek, key_info_with_week.calendar_yearmonth
, open_orders.load_week,  open_orders.open_orders_promo, open_orders.open_orders, prev_week_info.load_week_if_null 
from key_info_with_week left join open_orders
on 
key_info_with_week.lead_sku = open_orders.lead_sku and
key_info_with_week.plant = open_orders.plant and
key_info_with_week.customer_planning_group = open_orders.customer_planning_group and
key_info_with_week.format = open_orders.format   and
key_info_with_week.division = open_orders.division and
key_info_with_week.calendar_yearweek = open_orders.calendar_yearweek 
cross join (select max(WeekId) load_week_if_null from Calendar where WeekId  <= ( select distinct WeekId from Calendar where DateKey =  year((select report_date from report_week_info))*10000 + month((select report_date from report_week_info))*100 + day((select report_date from report_week_info)) )) prev_week_info
""")
sqldf.createOrReplaceTempView("combined_info")   



// COMMAND ----------

//hist

// COMMAND ----------

val sqldf = spark.sql("""
select f.plant, f.customer_planning_group, f.format , f.division, f.lead_sku, f.calendar_yearweek, f.calendar_yearmonth
, null as load_week,  null as open_orders_promo, null as open_orders,  f.total_shipments_volume_hl , total_shipments_volume_hl_initial , load_week_info.load_week_if_null 
from key_info_with_week_hist f 
cross join 
(select max(WeekId) load_week_if_null  from Calendar where WeekId  <= ( select distinct WeekId from Calendar where DateKey =  year((select report_date from report_week_info))*10000 + month((select report_date from report_week_info))*100 + day((select report_date from report_week_info)) )) as load_week_info
""")
sqldf.createOrReplaceTempView("combined_info_hist")  


// COMMAND ----------

//5.	Отфильтровать PromoDSD.csv по 8 клиентам p2 и сгруппировать по измерениям week_forecast, SKU_name, SGP, Client_name, Format_TT, ActivationID, суммировав Promo_week и BL_week

// COMMAND ----------

val sqldf = spark.sql("""
select 
/*week_forecast, date_format(week_forecast, 'dd.MM.yy') as week_forecast_key,*/ Calendar.WeekId,  SKU_name, SGP, Client_name, 

--if (Format_TT = "\\N" or isnull(Format_TT),"не определено", Format_TT ) Format_TT , 

if ( Client_name = 'X5 Retail Group' or Client_name = 'Магнит (Тандер)', 
  /*'Минимаркет'*/
 if( (Format_TT <> 'Гипермаркет'  and Format_TT <> 'Супермаркет') or Format_TT is null, 'Минимаркет', Format_TT)  , 
 if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
) as Format_TT, 

ActivationID, sum (Promo_week) Promo_week, sum (BL_week) BL_week
from PromoDSD 
left join Calendar on week_forecast = Calendar.DayName --on date_format(week_forecast, 'dd.MM.yy') = Calendar.Week
left join CPG_Formats cpgf on  Client_name = cpgf.CPG
where client_name in  (select distinct customer_planning_group from Sell_in_RU_p2_with_formats )
group by /*week_forecast,*/ Calendar.WeekId,  SKU_name, SGP, Client_name, Format_TT, ActivationID, cpgf.format

union 

--PromoDRP

select 
/*cast(to_date(cl.Week, 'dd.MM.yy') as timestamp) week_forecast  ,cl.Week week_forecast_key ,*/ cl.WeekId,  drp.SKU SKU_name, drp.SGP, drp.Client_Name, 

--drp.Format Format_TT, 

if ( Client_name = 'X5 Retail Group' or Client_name = 'Магнит (Тандер)', 
  /*'Минимаркет'*/
 if( (drp.Format <> 'Гипермаркет' and drp.Format <> 'Супермаркет') or drp.Format is null, 'Минимаркет', drp.Format) , 
 if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
) as Format_TT, 

drp.Activ_code ActivationID, sum (Whs_FC_vol)  Promo_week, null BL_week
from PromoDRP drp 
left join Calendar cl on  cast(to_date(drp.Date , 'dd.MM.yyyy') as timestamp) = cl.DayName
left join CPG_Formats cpgf on  Client_name = cpgf.CPG
where 
drp.client_name in  (select distinct customer_planning_group from Sell_in_RU_p2_with_formats )
and drp.Activ_code = 200 or drp.Activ_code = 500
group by /*cl.Week,*/cl.WeekId, drp.SKU, drp.SGP, drp.Client_Name, drp.Format, drp.Activ_code, cpgf.format

""")
sqldf.createOrReplaceTempView("promoDSD_info")  

// COMMAND ----------

//6.	Группировать Action_MT.csv по ActivationID, взяв первое по Mechanik, Start_Orders, End_Orders, Begin_action, End_action, In_voice, Format, Mechanik_new, взяв максимум по Shelf_Discount, Discount и суммировав Forecast_Volume

// COMMAND ----------

val sqldf = spark.sql("""
select tab.*
from (
select 
ActivationID as ActivationID_ac_in , Mechanik, Start_Orders,End_Orders,Begin_action, End_action, In_voice, Format, Mechanik_new, DeliveryType, datediff(end_action , begin_action)+1 length_promo, 
max(if (shelf_discount = "\\N" or shelf_discount is null, 0, shelf_discount )) shelf_discount , 
max(if (discount = "\\N" or discount is null, 0, discount )) discount,
sum(Forecast_Volume) forecast_volume, 
row_number() over ( partition by   ActivationID   order by if(isnull(sum(Forecast_Volume)),0, sum(Forecast_Volume)) desc ) act_rank
from Action 
where DeliveryType = 'DSD'
group by ActivationID, Mechanik, Start_Orders,End_Orders,Begin_action, End_action, In_voice, Format, Mechanik_new, DeliveryType
) tab
where  tab.act_rank = 1 
""")
sqldf.createOrReplaceTempView("action_info") 


// COMMAND ----------

//7.	Left join 5. и 6. по коду ActivationID

// COMMAND ----------

val sqldf = spark.sql("""
select promoDSD_info.*, action_info.*
from promoDSD_info left join action_info on promoDSD_info.ActivationID = action_info.ActivationID_ac_in
""")
sqldf.createOrReplaceTempView("combined_promoDSD_action_info") 

// COMMAND ----------

//8.	Фильтровать ActiveAdress_SGP по Other=1

// COMMAND ----------

// val sqldf = spark.sql("""
// select * from ActiveAdress_SGP where other = 1 
// """)
// sqldf.createOrReplaceTempView("active_adress_SGP") 

// COMMAND ----------

val sqldf = spark.sql(""" select ActiveAdress, SGP_Before
  from (
  select ba.ActiveAdress, ba.SGP_Before,  row_number() over ( partition by  ba.SGP_Before order by  sum (s.total_shipments_volume_hl) desc) row_num
  from SGP_B_A_ActiveAddress ba left join Sell_in_RU_p2_with_formats s on s.plant_name = ba.ActiveAdress
  group by  ba.SGP_Before, ba.ActiveAdress
  ) tab 
  where 
  tab.row_num = 1
  """)
sqldf.createOrReplaceTempView("active_adress_SGP") 

// COMMAND ----------

//9.	Left join 7. И 8. по SGP

// COMMAND ----------

val sqldf = spark.sql("""
select combined_promoDSD_action_info.*, active_adress_SGP.ActiveAdress
from combined_promoDSD_action_info left join active_adress_SGP on combined_promoDSD_action_info.SGP = active_adress_SGP.SGP_Before --active_adress_SGP.producerName1
""")
sqldf.createOrReplaceTempView("combined_promoDSD_action_info_active_adress_SGP") 

// COMMAND ----------

// 10.	Группировать MD_SKU_TO.csv по FCBaseWareName, взяв первое по FCBaseWareId

// COMMAND ----------

val sqldf = spark.sql("""
select FCBaseWareName, FCBaseWareId , int(if(left(FCBaseWareId,1) = "=", replace(FCBaseWareId,left(FCBaseWareId,1), ''), FCBaseWareId ))as Lead_SKU_ID
from MD_SKU_TO 
group by FCBaseWareName, FCBaseWareId
""")
sqldf.createOrReplaceTempView("md_sku_info") 

// COMMAND ----------

//11.	Left join 9. и 10. по FCBaseWareName= SKU_name

// COMMAND ----------

val sqldf = spark.sql("""
select combined_promoDSD_action_info_active_adress_SGP.* , md_sku_info.FCBaseWareId, md_sku_info.Lead_SKU_ID
from combined_promoDSD_action_info_active_adress_SGP left join md_sku_info on md_sku_info.FCBaseWareName = combined_promoDSD_action_info_active_adress_SGP.SKU_name
""")
sqldf.createOrReplaceTempView("combined_promoDSD_action_info_active_adress_SGP_md_SKU") 

// COMMAND ----------

//12.	Сформировать в 11. числовое поле Lead_SKU из FCBaseWareId

// COMMAND ----------

//done in step 10

// COMMAND ----------

//13.	Сформировать в 12. promo_length = End_action - Begin_action+1

// COMMAND ----------

//done in step 6

// COMMAND ----------

//14.	Left join 13. c PlantID.csv по Plant_name (? or ActiveAdress )= ActiveAdress

// COMMAND ----------

val sqldf = spark.sql("""
select combined_promoDSD_action_info_active_adress_SGP_md_SKU.* , plant_id_info.Plant_ID , Divisions.Division
from combined_promoDSD_action_info_active_adress_SGP_md_SKU 
left join plant_id_info on combined_promoDSD_action_info_active_adress_SGP_md_SKU.ActiveAdress = plant_id_info.ActiveAdress
left join Divisions on combined_promoDSD_action_info_active_adress_SGP_md_SKU.ActiveAdress = Divisions.WH
""")
sqldf.createOrReplaceTempView("combined_promoDSD_action_info_active_adress_SGP_md_SKU_plant_division") 

// COMMAND ----------

//15.	Left join 14. c Division.csv по Plant=Plant

// COMMAND ----------

//done is step 14

// COMMAND ----------

//16.	Left join 4. и 14. По Plant/CPG= Client_name/Format= Format_TT /Division= Division /Lead_SKU/Calendar_yearweek= week_forecast

// COMMAND ----------

val sqldf = spark.sql("""

select distinct
'RU' as country_name,
combined_info.calendar_yearmonth as calendar_yearmonth,
combined_info.calendar_yearweek as calendar_yearweek,
if (isnull(combined_info.load_week), combined_info.load_week_if_null , combined_info.load_week) load_week, 
combined_info.lead_sku lead_sku,
combined_info.customer_planning_group customer_planning_group,
combined_info.format,
combined_info.plant plant,
combined_info.division,
s.ActivationID activation_code, 
--s.ActiveAdress active_address,

if(s.mechanik_new is not null and s.mechanik_new <> "\\N" and s.mechanik_new <> '', if(lower(s.mechanik_new) = "ценовое промо+паллеты+каталог", "Ценовое промо+Паллеты+Каталог", s.mechanik_new ), 
     if(s.ActivationID = 900 , "Client Promo",
       if (isnull(s.ActivationID), null,
         if(s.mechanik is null or s.mechanik = "DIOT_MT_Лок.промо_конечный потребитель" or s.mechanik = "кастомер промо" or s.mechanik = "Каст.промо_контракт" or 
         s.mechanik = "тактические доп.активности" , "Другое", 
         if(lower(s.mechanik) = "ценовое промо+паллеты+каталог"  ,  "Ценовое промо+Паллеты+Каталог", 
           if(s.mechanik = "ценовое промо+каталог_не использовать для 2017 года", "Ценовое промо+Каталог", 
             if(s.mechanik ="ценовое промо_не использовать для 2017 года", "Ценовое промо", 
               if(s.mechanik = "In Out", "Ценовое промо",
                 if (s.mechanik = "контрактованные паллеты_не использовать для 2017 года", "Паллеты",
                   if(s.mechanik = "Ценовое Промо Indirect для смешанного типа доставки", "Ценовое промо" ,
                     if(s.mechanik = "ценовое промо+паллеты_не использовать для 2017 года","Ценовое промо+Паллеты",
                       if(s.mechanik = "контрактованные паллеты+цен промо_не использовать для 2017 года", "Ценовое промо+Паллеты",
                         if(s.mechanik = "контрактованные паллеты+цен промо+каталог_не использовать для 2017 года" ,"Ценовое промо+Паллеты+Каталог",
                           if(s.ActivationID = 200, "Опт", 
                             if(s.ActivationID = 500, "Expo",s.mechanik            
            ))))))))))))))) mechanism  , 

s.length_promo,
s.shelf_discount,
s.discount,
--s.forecast_volume,
--s.Promo_week promo_week,
--s.BL_week bl_week,
combined_info.open_orders_promo, 
combined_info.open_orders,
'' as total_sales_volume,
'' as total_shipments_volume_hl


from combined_info
left join combined_promoDSD_action_info_active_adress_SGP_md_SKU_plant_division s on
combined_info.plant = s.Plant_ID and
combined_info.customer_planning_group = s.Client_name  and
combined_info.format =  s.Format_TT   and
combined_info.division = s.Division  and
combined_info.lead_sku = s.Lead_SKU_ID  and
combined_info.calendar_yearweek = s.WeekId 


"""
)

sqldf.createOrReplaceTempView("Result") 


// COMMAND ----------

//hist

// COMMAND ----------

val sqldf = spark.sql("""
select distinct
'RU' as country_name,
 calendar_yearmonth,
 calendar_yearweek,
 if (isnull(combined_info_hist.load_week), combined_info_hist.load_week_if_null , combined_info_hist.load_week) load_week,
 --null as load_week,
 lead_sku,
 customer_planning_group,
 format,
 plant,
 division,
 null as activation_code, 
 null as number_of_promo,
 --null active_address, --???
 null as mechanism  , 
 null as length_promo,
 null as shelf_discount,
 null as discount,
 open_orders_promo, 
 open_orders,
 total_shipments_volume_hl_initial as total_sales_volume,
 total_shipments_volume_hl_initial as total_shipments_volume_hl

from combined_info_hist
""") 
sqldf.createOrReplaceTempView("Result_hist") 


// COMMAND ----------

//FC

// COMMAND ----------

val sqldf = spark.sql("""

select tab.*
from (
select fc.version, year(fc.version)*10000 + month(fc.version)*100 + day(fc.version) as version_key , fc.client, fc.address, 
fc.sku_code, int(if(left(fc.sku_code,1) = "=", replace(fc.sku_code,left(fc.sku_code,1), ''), fc.sku_code )) sku_code_int,
fc.week_date, year(fc.week_date)*10000 + month(fc.week_date)*100 + day(fc.week_date) as week_date_key,  date_format(fc.week_date, 'dd.MM.yy') week_date_format,
fc.week_num,   sum (fc.vol_dal)/10 vol_hl , 
if ( (year(fc.version)*10000 + month(fc.version)*100 + day(fc.version) = year(fc.week_date)*10000 + month(fc.week_date)*100 + day(fc.week_date) )  or 
(year(fc.version)*10000 + month(fc.version)*100 + day(fc.version) = (select max(year(version)*10000 + month(version)*100 + day(version)) from Fc_Hist  ))
, 1, 0 ) FC_filter
from Fc_Hist fc 
group by fc.version, fc.client, fc.address, fc.sku_code, fc.week_date, fc.week_num 
) tab
where tab.FC_filter = 1 
""")
sqldf.createOrReplaceTempView("FC_input_new")

// COMMAND ----------

val sqldf = spark.sql("""
select p.*, int(if(left(s.SKU_Lead_ID ,1) = "=", replace(s.SKU_Lead_ID ,left(s.SKU_Lead_ID ,1), ''), s.SKU_Lead_ID  )) lead_sku_code_int , c.WeekId, c.MonthId , plant.Plant_ID
from FC_input_new p 
left join MD_SKU s on p.sku_code_int = int(if(left(s.SKU_ID,1) = "=", replace(s.SKU_ID,left(s.SKU_ID,1), ''), s.SKU_ID ))
left join (select distinct Week, WeekId,MonthId from Calendar) c on p.week_date_format = c.Week
left join plant_id_info plant on p.address = plant.ActiveAdress
""")
sqldf.createOrReplaceTempView("FC_result_new")


// COMMAND ----------

val sqldf = spark.sql("""
select WeekId calendar_yearweek , lead_sku_code_int lead_sku , Plant_ID plant , client, sum (vol_hl) as benchmark_forecast
from FC_result_new 
group by WeekId, lead_sku_code_int, Plant_ID, client
""") 
sqldf.createOrReplaceTempView("FC_main")

// COMMAND ----------

//Active SKU

// COMMAND ----------

// val file_location = writePath + "/"+ "training.csv"
// val file_type = "csv"
// val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ",").option("header", "true").load(file_location)
// df.createOrReplaceTempView("FilterFromYGroup")

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
  calendar.MonthID >= year(date_add((select report_date from report_week_info),-365))*100 + month(date_add((select report_date from report_week_info),-365)) 
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
  calendar.MonthID >= year(date_add((select report_date from report_week_info),-90))*100 + month(date_add((select report_date from report_week_info),-90)) 
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
  calendar.MonthID >= year(date_add((select report_date from report_week_info),-90))*100 + month(date_add((select report_date from report_week_info),-90)) 
  group by   o.Client , calendar.MonthID
  having (Actual_Volume > 0)   
  ) tab  
) tab
group by Client
having (NumOfSkippedMonth = 0)

""")
sqldf.createOrReplaceTempView("Active_CPG_Only")



// COMMAND ----------

//HFA_RU

// COMMAND ----------

var Result = "Failure"  
val sqldf = spark.sql("""  

select 
tab.country_name, tab.calendar_yearmonth, tab.calendar_yearweek, tab.load_week, tab.lead_sku,  tab.customer_planning_group, 

--if (isnull(tab.format) or tab.format = "\\N", 'LKA',tab.format)

if (
tab.customer_planning_group = 'X5 Retail Group' or tab.customer_planning_group = 'Магнит (Тандер)' ,  
  tab.format, 
  if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
  )
format, 

--concat(tab.customer_planning_group, '_', if (isnull(tab.format) or tab.format = "\\N", 'LKA',tab.format)) as cpg_format,

concat(tab.customer_planning_group, '_', 

if (
tab.customer_planning_group = 'X5 Retail Group' or tab.customer_planning_group = 'Магнит (Тандер)' ,  
  tab.format, 
  if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
  )
) cpg_format,


tab.plant, tab.division, tab.activation_code, tab.number_of_promo, tab.mechanism, if (isnull(tab.length_promo),0,tab.length_promo) length_promo  , if (isnull(tab.shelf_discount) or tab.shelf_discount > 1, 0, tab.shelf_discount ) shelf_discount , if (isnull(tab.discount), 0,tab.discount) discount , tab.open_orders_promo, tab.open_orders, tab.total_sales_volume as total_shipments, tab.total_shipments_volume_hl as total_shipments_volume, tab.benchmark_forecast

from (
select 
tab.country_name, tab.calendar_yearmonth, tab.calendar_yearweek, tab.load_week, tab.lead_sku,  tab.customer_planning_group, 

if (tab.format = 'не определено' and tab.customer_planning_group = 'Дикси', 'Супермаркет', 
  if (tab.format = 'не определено' and tab.customer_planning_group = 'Бристоль', 'Супермаркет',
    if (tab.format = 'не определено' and tab.customer_planning_group = 'Семья', 'LKA', 
      if (tab.format = 'не определено' and tab.customer_planning_group = 'Красное и Белое', 'Супермаркет',
        if (tab.format = 'не определено' and tab.customer_planning_group = 'X5 Retail Group', 'Минимаркет',
          if (tab.format = 'не определено' and tab.customer_planning_group = 'METRO', 'Гипермаркет', 
            if (tab.format = 'не определено' and tab.customer_planning_group = 'Лента', 'Гипермаркет',
              if(tab.format = 'не определено' and tab.customer_planning_group = 'Магнит (Тандер)', 'Минимаркет', 
                tab.format
              )))))))) format

, tab.plant, tab.division, tab.activation_code, tab.number_of_promo, tab.mechanism, tab.length_promo, tab.shelf_discount, tab.discount, tab.open_orders_promo, tab.open_orders, tab.total_sales_volume, tab.total_shipments_volume_hl, tab.benchmark_forecast


from (
select 
r.* ,if ( ( r.format <> 'Минимаркет' and (r.customer_planning_group = 'Магнит (Тандер)' or r.customer_planning_group = 'X5 Retail Group') ) , 0 , fc.benchmark_forecast) as benchmark_forecast
from 
  (
  
    select tab.country_name, tab.calendar_yearmonth, tab.calendar_yearweek, tab.load_week, tab.lead_sku,  tab.customer_planning_group, tab.format, tab.plant, tab.division, tab.activation_code, tab.number_of_promo, tab.mechanism, tab.length_promo, tab.shelf_discount, tab.discount, tab.open_orders_promo, tab.open_orders, tab.total_sales_volume,  tab.total_shipments_volume_hl
    from
    (select *, row_number() over ( 
    partition by  calendar_yearweek, load_week ,  lead_sku, customer_planning_group, format, plant, division  
    order by activation_code desc ) number_of_promo from Result
    ) tab
    --where tab.number_of_promo = 1 --при этом строки файла HFA задублируются по полю ActivationID. Добавить номер промо на объекте для проверки итоговых сумм
  
  union 
  
  select * from Result_hist
  ) r
left join FC_main fc on 
r.calendar_yearweek = fc.calendar_yearweek and
r.lead_sku = fc.lead_sku and
r.plant = fc.plant and
r.customer_planning_group = fc.client
) tab
  )tab  
  
  left join CPG_Formats cpgf on  tab.customer_planning_group = cpgf.CPG
  
  --where
  
  --(int(tab.lead_sku) in (select distinct int(SKU_Lead_ID) from Active_SKU_Only)) and
  --(tab.plant in (select distinct Plant_ID from Active_Plant_Only where Plant_ID is not null )) and 
  --(tab.customer_planning_group in (select distinct Client from Active_CPG_Only where Client is not null))
  
  
  
""")

sqldf.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter", ";").save(readPath)

val name : String = "part-00000"  
val file_list : Seq[String] = dbutils.fs.ls(readPath).map(_.path).filter(_.contains(name))
val read_name = if (file_list.length >= 1 ) file_list(0).replace(readPath + "/", "")
val row_count = spark.read.format("csv").option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_list(0)).count   
dbutils.fs.mv(readPath+"/"+read_name , writePath_tmp+"/"+fname_tmp)   
dbutils.fs.rm(readPath , recurse = true) 
if (row_count > 0) Result = "Success" else println("The file " +writePath_tmp+"/"+fname_tmp + " is empty !" )
println("temp promo table has been created with status: " + Result )


//sqldf.createOrReplaceTempView("result_hfa_table")

// COMMAND ----------

//HFA_RU_tmp

// COMMAND ----------

val file_location = "wasbs://prod@staeeprodbigdataml2c.blob.core.windows.net/ETL/tmp/HFA_RU_tmp_All.csv"
val file_type = "csv"
val df = spark.read.format(file_type).option("inferSchema", "true").option("delimiter", ";").option("header", "true").load(file_location)
df.createOrReplaceTempView("result_hfa_table")

// COMMAND ----------

val sqldf = spark.sql("""
select WeekId calendar_yearweek , Lead_SKU_ID lead_sku, Plant_ID plant, count(distinct ActivationID ) promo_activities_of_competitors
from combined_promoDSD_action_info_active_adress_SGP_md_SKU_plant_division
group by WeekId, Lead_SKU_ID, Plant_ID
""")
sqldf.createOrReplaceTempView("promo_activities_of_Competitors_info")

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

//distance_between_promotions_info based on hfa

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
    

  from result_hfa_table s 
  left join Action a on s.activation_code = a.ActivationID
  where s.activation_code is not null
  group by   s.activation_code, s.plant, s.lead_sku, s.customer_planning_group, a.start_orders, a.end_orders

""")
sqldf.createOrReplaceTempView("distance_between_promotions_info_hfa")

// COMMAND ----------

//Auto Orders

// COMMAND ----------

val sqldf = spark.sql("""
select ao.Client customer_planning_group, cc.WeekId calendar_yearweek , s.SKU_Lead_ID lead_sku , p.Plant_ID plant, sum (replace(Vol_dal,',','.')) auto_orders
from Auto_Orders ao 
left join MD_SKU s on s.SKU_ID = int(if(left(ao.SKU_code,1) = "=", replace(ao.SKU_code,left(ao.SKU_code,1), ''), ao.SKU_code ))
left join MD_Clients cl on ao.Address = cl.AdressCode
left join plant_id_info p on  trim(p.ActiveAdress) = trim(cl.ActiveAdress)
left join  (select distinct week, WeekId from Calendar) cc on cc.week = date_format(to_date(ao.Week, 'dd.MM.yy'), 'dd.MM.yy')
where 
(ao.Version < (select max(Version) max_Version from  Auto_Orders) and ao.Version = ao.Week) 
or
(ao.Version = (select max(Version) max_Version from  Auto_Orders))
group by  ao.Client, cc.WeekId, s.SKU_Lead_ID, p.Plant_ID
""")
sqldf.createOrReplaceTempView("auto_orders_info")

// COMMAND ----------

//uplift

// COMMAND ----------

val sqldf = spark.sql("""

select 
calendar_yearweek ,  customer_planning_group, format,  lead_sku,  active_address, activation_code, plant, sum(uplift_hl) uplift_hl

from (

select 
pdah.WeekId calendar_yearweek , if(isnull(client_info.RKAName) or client_info.RKAName = "\\N" , pdah.Client_name, client_info.RKAName) customer_planning_group, 

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
    if( (pdah.Format_TT <> 'Гипермаркет'  and pdah.Format_TT <> 'Супермаркет') or pdah.Format_TT is null, 'Минимаркет', pdah.Format_TT), 
    if(isnull(cpgf.format) or cpgf.format = "\\N" or cpgf.format = 'Локальные клиенты' , 'LKA', if (right(cpgf.format,1) = 'ы', replace(cpgf.format,right(cpgf.format,1), '' ), cpgf.format ) )
  ) format,
  
  md_sku_info.Lead_SKU_ID lead_sku, 
  act_info.ActiveAdress active_address,
  pdah.ActivationID activation_code,
  plant_id_info.Plant_ID plant,
  
  round(
  sum(
  if(isnull(pdah.Promo_week) or pdah.Promo_week = "\\N", 0, pdah.Promo_week)  + 
  if(isnull(pdah.BL_week) or pdah.BL_week = "\\N", 0 , pdah.BL_week) 
  ) /10
  ,5) uplift_hl
  
from  promoDSD_info pdah 
--left join Calendar cl on pdah.week_forecast = cl.DayName
left join (select  Client /*, New2*/, RKAName from MD_Clients group by Client, RKAName /*, New2*/) client_info on pdah.Client_name = client_info.Client
left join CPG_Formats cpgf on if(isnull(client_info.RKAName) , pdah.Client_name, client_info.RKAName) = cpgf.CPG
left join md_sku_info on md_sku_info.FCBaseWareName = pdah.SKU_name

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

left join plant_id_info on act_info.ActiveAdress = plant_id_info.ActiveAdress

group by pdah.WeekId, pdah.Client_name, client_info.RKAName, /*client_info.New2,*/  pdah.Format_TT,  cpgf.format, md_sku_info.Lead_SKU_ID, act_info.ActiveAdress, pdah.ActivationID, plant_id_info.Plant_ID
) tab

group by calendar_yearweek ,  customer_planning_group, format,  lead_sku,  active_address, activation_code, plant


""")
sqldf.createOrReplaceTempView("uplift_info")

// COMMAND ----------

//Features

// COMMAND ----------

val query_full = 
  """ 
  select distinct
  s.*,
  
  if (isnull(METRO_super_Wednesday_info.METRO_super_Wednesday) , 0,  METRO_super_Wednesday_info.METRO_super_Wednesday ) METRO_super_Wednesday,
  
  if (s.customer_planning_group = 'X5 Retail Group' and rp.MTP is not null and isnull(rp.Lok), 1, 0) X5_MTP,
  if (s.customer_planning_group = 'X5 Retail Group' and rp.MTP is not null and rp.Lok is not null , 1, 0) X5_MTP_Loc,
  if (s.customer_planning_group = 'X5 Retail Group' and rp.OTP is not null and isnull(rp.Lok), 1, 0) X5_OTP,
  if (s.customer_planning_group = 'X5 Retail Group' and rp.OTP is not null and rp.Lok is not null, 1, 0) X5_OTP_Loc,
 
  if (pac.promo_activities_of_competitors is null, 0, pac.promo_activities_of_competitors) promo_activities_of_competitors ,
  
  if (MBO_info.MBO is null, 0, MBO_info.MBO) mbo_programs,
  
  replace(coalesce(seas_coef_info.Seas_coef , seas_coef_info_core.Seas_coef), ',', '.' ) seas_coef,
  
  dbp.distance_between_promotions,
  
  aoi.auto_orders,
  
  upi.uplift_hl
  
  from result_hfa_table s 
  
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
  ) rp 
  on s.activation_code = rp.Activ_id
  
  left join promo_activities_of_Competitors_info pac 
  on 
  s.plant = pac.plant and 
  s.lead_sku = pac.lead_sku and
  s.calendar_yearweek = pac.calendar_yearweek 
  
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
   
   left join 
    (
    select 
    activation_code, plant, lead_sku, customer_planning_group, distance_between_promotions
    from (
    select * , row_number() over (partition by activation_code, plant, lead_sku, customer_planning_group order by distance_between_promotions   ) row_num
    from distance_between_promotions_info_hfa 
    ) tab
    where row_num = 1
    ) dbp 
    on 
    s.activation_code = dbp.activation_code and
    s.plant = dbp.plant and 
    s.lead_sku = dbp.lead_sku and
    --s.calendar_yearweek = dbp.calendar_yearweek and
    s.customer_planning_group = dbp.customer_planning_group
    
    left join auto_orders_info aoi 
    on 
    s.customer_planning_group = aoi.customer_planning_group and 
    s.calendar_yearweek = aoi.calendar_yearweek and 
    s.lead_sku = aoi.lead_sku and
    s.plant = aoi.plant 
    
    left join uplift_info upi  
    on 
    s.activation_code = upi.activation_code and
    s.calendar_yearweek = upi.calendar_yearweek and
    s.customer_planning_group = upi.customer_planning_group and
    s.lead_sku = upi.lead_sku and 
    s.format = upi.format and 
    s.plant = upi.plant   
      
  """ 
  

// COMMAND ----------

val sqldf_full = spark.sql(query_full)

// COMMAND ----------

val query_incremental = 
  s"""
  select *
  from (
  
  select distinct
  s.calendar_yearmonth as partition_name, s.*,
  
  if (isnull(METRO_super_Wednesday_info.METRO_super_Wednesday) , 0,  METRO_super_Wednesday_info.METRO_super_Wednesday ) METRO_super_Wednesday,
  
  if (s.customer_planning_group = 'X5 Retail Group' and rp.MTP is not null and isnull(rp.Lok), 1, 0) X5_MTP,
  if (s.customer_planning_group = 'X5 Retail Group' and rp.MTP is not null and rp.Lok is not null , 1, 0) X5_MTP_Loc,
  if (s.customer_planning_group = 'X5 Retail Group' and rp.OTP is not null and isnull(rp.Lok), 1, 0) X5_OTP,
  if (s.customer_planning_group = 'X5 Retail Group' and rp.OTP is not null and rp.Lok is not null, 1, 0) X5_OTP_Loc,
 
  if (pac.promo_activities_of_competitors is null, 0, pac.promo_activities_of_competitors) promo_activities_of_competitors ,
  
  if (MBO_info.MBO is null, 0, MBO_info.MBO) mbo_programs,
  
  replace(coalesce(seas_coef_info.Seas_coef , seas_coef_info_core.Seas_coef), ',', '.' ) seas_coef,
  
  dbp.distance_between_promotions,
  
  aoi.auto_orders,
  
  upi.uplift_hl
  
  from result_hfa_table s 
  
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
  ) rp 
  on s.activation_code = rp.Activ_id
  
  left join promo_activities_of_Competitors_info pac 
  on 
  s.plant = pac.plant and 
  s.lead_sku = pac.lead_sku and
  s.calendar_yearweek = pac.calendar_yearweek 
  
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
   
   left join 
    (
    select 
    activation_code, plant, lead_sku, customer_planning_group, distance_between_promotions
    from (
    select * , row_number() over (partition by activation_code, plant, lead_sku, customer_planning_group order by distance_between_promotions   ) row_num
    from distance_between_promotions_info_hfa 
    ) tab
    where row_num = 1
    ) dbp 
    on 
    s.activation_code = dbp.activation_code and
    s.plant = dbp.plant and 
    s.lead_sku = dbp.lead_sku and
    --s.calendar_yearweek = dbp.calendar_yearweek and
    s.customer_planning_group = dbp.customer_planning_group
    
    left join auto_orders_info aoi 
    on 
    s.customer_planning_group = aoi.customer_planning_group and 
    s.calendar_yearweek = aoi.calendar_yearweek and 
    s.lead_sku = aoi.lead_sku and
    s.plant = aoi.plant 
    
    left join uplift_info upi  
    on 
    s.activation_code = upi.activation_code and
    s.calendar_yearweek = upi.calendar_yearweek and
    s.customer_planning_group = upi.customer_planning_group and
    s.lead_sku = upi.lead_sku and 
    s.format = upi.format and 
    s.plant = upi.plant   
    
    )tab
  """ +
{if (type_of_data_extract == 1) 
 s""" 
 where calendar_yearmonth >= 
 year(date_add(current_date(),-1 * $num_of_days_before_current_date))*100 + month(date_add(current_date(),-1 * $num_of_days_before_current_date))
 """ 
 else ""}
  

// COMMAND ----------

val sqldf_incremental = spark.sql(query_incremental)

// COMMAND ----------

//Final testing before result export

// COMMAND ----------

exportToBlobStorage_Baltika_Result_Before_Testing(job,notebook,fname, sqldf_full, readPath)

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

if (Test_Is_incomplete_period(job,notebook, sqldf_result, fname) == "FAILED") {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  //dbutils.notebook.exit("Failure")    
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result, "lead_sku", fname ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result, "cpg_format", fname ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

//uncomment after response

// COMMAND ----------

if (Test_Missing_Filed(job, notebook, sqldf_result, "plant", fname ) == "FAILED" ) {
  save_to_log_last_step(job,notebook,"Failure",notebook_start_time)
  //dbutils.notebook.exit("Failure")  
}

// COMMAND ----------

Test_Values_In_Correct_Range(job, notebook, sqldf_result, "monthly", "total_shipments_volume", 408353, 1129444, fname)

// COMMAND ----------

Test_Values_In_Correct_Range(job, notebook, sqldf_result, "monthly", "benchmark_forecast", 446962, 1291124, fname)

// COMMAND ----------

// end Final testing before result export

// COMMAND ----------

val Result = 
if (type_of_ETL == 0) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname, sqldf_full, readPath ) == "Success" &&
      exportToBlobStorage_CAP (job,notebook,0, sqldf_incremental, "HFADIRECT_" , readPath, readPath_GBS ) == "Success"
     )  "Success" else "Failure"  }
else if (type_of_ETL == 1) { 
  if (exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental, "HFADIRECT_" , readPath, readPath_GBS ) == "Success"
     ) "Success" else  "Failure"  }
else if (type_of_ETL == 2) { 
  if (exportToBlobStorage_Baltika (job,notebook,fname, sqldf_full, readPath ) == "Success" &&
      exportToBlobStorage_CAP (job,notebook,1, sqldf_incremental, "HFADIRECT_" , readPath, readPath_GBS ) == "Success"
     ) "Success" else "Failure" }
else "Unexpected parameter"


//save to log
save_to_log_last_step(job,notebook,Result,notebook_start_time)

dbutils.notebook.exit(Result)