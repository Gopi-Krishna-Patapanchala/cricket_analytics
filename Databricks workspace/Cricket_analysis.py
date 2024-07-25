# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Displaying all Mount point details

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####importing the information from config notebook

# COMMAND ----------

# MAGIC %run ./final_config_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####importing the information from transformation notebook

# COMMAND ----------

# MAGIC %run ./transformations_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ####importing the required packages

# COMMAND ----------

import sys
import re
import pandas as pd
from pyspark.sql import Row
from pyspark.sql.functions import split, col,trim,lower,regexp_replace,lit,posexplode,when,expr,upper,count
from pyspark.sql.types import StructType,IntegerType, DoubleType,StructField, StringType
from datetime import datetime
import smtplib
from email.mime.text import MIMEText


# COMMAND ----------

current_datetime = datetime.now()
formatted_datetime = current_datetime.strftime("%Y%m%d%H%M%S")
print("Execution start timestamp ", formatted_datetime)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Sending the E-mail communication for users stating that execution started

# COMMAND ----------

subject ="Cricket Analytics Exceution Started"
message = "Cricket Analytics Exceution Started {}".format(str(formatted_datetime))
send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop the Database cricket_analytics

# COMMAND ----------

spark.sql('DROP SCHEMA if exists cricket_analytics CASCADE;')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###creating the database cricket_analytics

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC create database if not exists cricket_analytics;

# COMMAND ----------

# MAGIC %sql
# MAGIC use cricket_analytics;

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating the Bronze Layer Player Master data external table

# COMMAND ----------

spark.sql(f"DROP TABLE  IF EXISTS cricket_analytics.bronze_layer_player_master_data;")
spark.sql(f"CREATE TABLE cricket_analytics.bronze_layer_player_master_data (ID INT,First_name STRING,Last_name STRING,DOB STRING,Playing_type STRING,Country_code INT) USING CSV OPTIONS ('delimiter' = ',','header' = 'false','inferSchema' = 'true') LOCATION 'dbfs:{raw_master_lookup}/*/*'")

# COMMAND ----------

# MAGIC %md 
# MAGIC ####creating the Bronze Layer Delta table for Player Master data

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE cricket_analytics.bronze_delta_player_master_data USING DELTA LOCATION 'dbfs:{raw_master_delta_lookup}' AS SELECT * FROM cricket_analytics.bronze_layer_player_master_data;")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cricket_analytics.bronze_delta_player_master_data;

# COMMAND ----------

master_df = spark.read.table("cricket_analytics.bronze_delta_player_master_data")
master_df = master_df.withColumn("batting_style",split(col("Playing_type"),",").getItem(0)).withColumn("bowling_style",split(col("Playing_type"),",").getItem(1)).drop("Playing_type")
master_df = master_df.toDF(*master_columns)
master_df.write.format("delta").mode("overwrite").save(refined_player_master_data_lookup)

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating the Bronze Layer Batting scorecard external table

# COMMAND ----------

spark.sql(f"DROP TABLE  IF EXISTS cricket_analytics.bronze_layer_batting_scorecard;")
spark.sql(f"CREATE TABLE cricket_analytics.bronze_layer_batting_scorecard (PLAYER_NAME STRING,DISMISSAL STRING,RUNS_SCORED INT,BALL_FACED INT,MINUTES_SEND INT,FOURS_SCORED INT, SIXES_SCORED INT, STRIKE_RATE FLOAT) USING CSV OPTIONS ('delimiter' = ',','header' = 'true') LOCATION 'dbfs:{raw_batting_scorecard_lookup}/*/*/'")

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE cricket_analytics.bronze_delta_batting_scorecard USING DELTA LOCATION 'dbfs:{raw_batting_scorecard_delta_lookup}' AS select * from  cricket_analytics.bronze_layer_batting_scorecard")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from cricket_analytics.bronze_delta_batting_scorecard;

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating the Bronze Layer Bowling scorecard external table

# COMMAND ----------

spark.sql(f"DROP TABLE  IF EXISTS cricket_analytics.bronze_layer_bowling_scorecard;")
spark.sql(f"CREATE TABLE cricket_analytics.bronze_layer_bowling_scorecard (PLAYER_NAME STRING,OVERS_BOWLED FLOAT,MAIDEN_OVER INT,RUNS_GIVEN INT,WICKETS_TAKEN INT,ECONOMY FLOAT, DOT_BALLS INT, FOURS_GIVEN INT,SIXES_GIVEN INT,WIDE_BALLS INT, NO_BALLS INT) USING CSV OPTIONS ('delimiter' = ',','header' = 'true') LOCATION 'dbfs:{raw_bowling_scorecard_lookup}/*/*/'")

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE cricket_analytics.bronze_delta_bowling_scorecard USING DELTA LOCATION 'dbfs:{raw_bowling_scorecard_delta_lookup}' AS SELECT * FROM cricket_analytics.bronze_layer_bowling_scorecard")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from cricket_analytics.bronze_delta_bowling_scorecard;

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating the Bronze Layer Team external table

# COMMAND ----------

spark.sql(f"DROP TABLE  IF EXISTS cricket_analytics.bronze_layer_team;")
spark.sql(f"CREATE TABLE cricket_analytics.bronze_layer_team (PLAYER_NAME STRING,PLAYER_ID INT, CAPTAIN_FLAG STRING) USING CSV OPTIONS ('delimiter' = ',','header' = 'true') LOCATION 'dbfs:{raw_team_lookup}/*/'")

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE cricket_analytics.bronze_delta_team USING DELTA LOCATION 'dbfs:{raw_team_delta_lookup}' AS SELECT * FROM cricket_analytics.bronze_layer_team")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * from cricket_analytics.bronze_delta_team;

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating the Bronze Layer Venue details external table

# COMMAND ----------

spark.sql(f"DROP TABLE  IF EXISTS cricket_analytics.bronze_layer_venue;")
spark.sql(f"CREATE TABLE cricket_analytics.bronze_layer_venue (TOSS STRING,SERIES STRING, SEASON STRING,PLAYER_OF_THE_MATCH STRING,MATCH_NUMBER STRING, MATCH_DATE STRING,POINTS STRING) USING CSV OPTIONS ('delimiter' = ',','header' = 'true') LOCATION 'dbfs:{raw_venue_lookup}/*/'")

# COMMAND ----------

spark.sql(f"CREATE OR REPLACE TABLE cricket_analytics.bronze_delta_venue USING DELTA LOCATION 'dbfs:{raw_venue_delta_lookup}' AS SELECT * FROM cricket_analytics.bronze_layer_venue ")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from cricket_analytics.bronze_delta_venue;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####Transformation start here 

# COMMAND ----------

#read the recon file
processed_match =[]  
spark.sql(f"DROP TABLE  IF EXISTS cricket_analytics.silver_processed_match_reconciliation;")
spark.sql(f"CREATE TABLE cricket_analytics.silver_processed_match_reconciliation (processed_match_details STRING,record_created_timestamp STRING)USING DELTA PARTITIONED BY (record_created_timestamp) OPTIONS (path = 'dbfs:{refined_reconciliation_lookup}') ") 
reconciliation_df = spark.read.table("cricket_analytics.silver_processed_match_reconciliation")
if reconciliation_df.count():
    distinct_values = reconciliation_df.select("processed_match_details").distinct().collect()
    processed_match = [row.processed_match_details for row in distinct_values]

# COMMAND ----------

available_match_details =[]
for folder in dbutils.fs.ls(raw_venue_lookup):
    available_match_details.append(folder[1].replace("/",""))
if len(processed_match):
    result_set = set(available_match_details) - set(processed_match)
    available_match_details = list(result_set)


# COMMAND ----------

len(available_match_details)

# COMMAND ----------


if len(available_match_details):
    count=1
    for match_details in available_match_details:
        print(f"i am count value --> {count}")
        count+=1
        try:
            team_name_1,team_name_2,match_id = match_details.split("-vs-")[0],match_details.split("-vs-")[1].rsplit("-",maxsplit=3)[0],match_details.split("-")[-1]
            for teams in team_details.values():
                if teams.lower() in team_name_1:
                    if "-" in teams:
                        teams = teams.replace("-"," ")
                    team_name_1 = teams
                if teams.lower() in team_name_2:
                    if "-" in teams:
                        teams = teams.replace("-"," ")
                    team_name_2 = teams
            player_file_path = [file for file in dbutils.fs.ls(raw_team_lookup+match_details) if file[1].endswith('.csv') and (file[1].lower().startswith("player"))]
            if len(player_file_path) ==1:
                player_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(player_file_path[0][0]).head(22)
                if len(player_df) != 0:
                    player_df = spark.createDataFrame(player_df)
                    player_df= trim_space(player_df,player_df.columns)
                    player_df = player_df.toDF(*player_columns)
                else:
                    print("Player file is empty for the match {}. skipping the process !!! \n".format(str(match_details)))
                    subject ="Unable to process the Player file. Skipping the process"
                    message = "Player file is empty for the match {}. skipping the process !!! \n".format(str(match_details))
                    send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)
                    continue
            else:
                print("Player file for the match {} is missing. Skipping the remaining process!!!!\n".format(str(match_details)))
                subject ="Missing the Player file. Skipping the process"
                message = "Player file for the match {} is missing. Skipping the remaining process!!!!\n".format(str(match_details))
                send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)
                continue

            if player_df.count() == 0:
                print("Player details are not loadded successfully for the match {}. Skipping the remaining process!!!!\n".format(str(match_details)))
                subject ="Player details file loading is Failed."
                message = "Player details are not loadded successfully for the match {}. Skipping the remaining process!!!!\n".format(str(match_details))
                send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)
                continue
            else:
                print("Player information loaded sucessfully for the match {}".format(match_details))
                player_df = player_df.withColumn("player_name", regexp_replace(col("player_name"), "-", " "))
            
            venue_file_path = [file for file in dbutils.fs.ls(raw_venue_lookup+match_details) if file[1].endswith('.csv')]
            if len(venue_file_path) ==1:
                venue_location = venue_file_path[0][1].replace(".csv","")
                split_result = venue_location.split("-")
                venue_id,venue_state,stadium = split_result[-1],split_result[-2],"_".join(split_result[:-2])
                
                venue_file_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(venue_file_path[0][0])
                venue_file_df= trim_space(venue_file_df,venue_file_df.columns)
            else:
                print("Venue information for the match {} is missing. Skipping the remaining process!!!\n".format(str(match_details)))
                subject ="venue information file is missing. Skipping the process"
                message = "Venue information for the match {} is missing or not found with proper naming conviention. Skipping the remaining process!!!\n".format(str(match_details))
                send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)
                continue
            if venue_file_df.count() == 0:
                print("venue details are not loaded successfully for the match {}. Skipping the remaining process!!!!\n".format(str(match_details)))
                subject ="venue details file loading is Failed."
                message = "venue details are not loaded successfully for the match {}. Skipping the remaining process!!!!\n".format(str(match_details))
                send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)
                continue
            else:
                print("venue information loaded sucessfully for the match {}".format(match_details))
                
            match_details_dict = venue_file_df.rdd.map(lambda row: row.asDict()).collect()
            match_details_dict=match_details_dict[0]
            match_details_dict["Points"] =match_details_dict["Points"].lower()
            if "won" not in match_details_dict["Points"] and "advanced" not in match_details_dict["Points"]:
                try:
                    if '2,' in match_details_dict["Points"] and  match_details_dict["Points"].replace(",","").split().count('2') == 1:
                        match_details_dict["own_team"] = [team.replace("2",'').strip() for team in match_details_dict["Points"].split(",") if '2' in team][0]
                    elif '4,' in match_details_dict["Points"]:
                        match_details_dict["own_team"] = [team.replace("4",'').strip() for team in match_details_dict["Points"].split(",") if '4' in team][0]
                    else:
                        raise Exception("tie check")
                except Exception as error:
                    match_details_dict["Points"] = match_details_dict["Points"].replace(",","").split()
                    if match_details_dict["Points"].count('1') == 2:
                         match_details_dict["own_team"] ="TIE"
                    elif match_details_dict["Points"].count('2') == 2:
                         match_details_dict["own_team"] ="TIE"
                    else:
                        raise Exception("Exception in Team won decision")
            else:
                match_details_dict["own_team"] = match_details_dict["Points"].lower().split(" ")[0]
            if 't20' in match_details_dict["Match_number"].lower():
                match_type = "T20"
                match_details_dict["Match_number"] =match_details_dict["Match_number"].lower().replace("t20","")
            else:
                match_type ="ODI"
            match_details_dict["Match_number"] = re.findall(r'\d+', match_details_dict["Match_number"])[0]
            if match_details_dict['Player_Of_The_Match'] is not None:
                man_of_the_match = match_details_dict['Player_Of_The_Match'].rsplit("-",maxsplit=1)
                match_details_dict["Player_Of_The_Match_player_name"] = man_of_the_match[0].strip()
                match_details_dict["Player_Of_The_Match_player_name"]=match_details_dict["Player_Of_The_Match_player_name"].replace("-"," ")
                match_details_dict["Player_Of_The_Match_player_id"]= man_of_the_match[1].strip()
            else:
                match_details_dict["Player_Of_The_Match_player_id"] = None
                match_details_dict["Player_Of_The_Match_player_name"]=None
            match_details_dict["Match_type"] = match_type
            toss_details = match_details_dict["Toss"].split(',')
            match_details_dict["toss_own_by"] = toss_details[0].strip()
            if "bat" in toss_details[1].strip():
                match_details_dict["toss_choice"] = "batting"
            else:
                match_details_dict["toss_choice"] = "bowling"
            
            print("Transformation process for venue file is completed successfully for the match {}".format(str(match_details)))

            batting_scorecard_file = [ file for file in dbutils.fs.ls(raw_batting_scorecard_lookup+match_details+"/scorecard/") if file[1].lower().startswith("batting")]
            if len(batting_scorecard_file)==0:
                print("Batting scorecard file are missing for the match {}. Skipping the remaining process!!!!\n".format(str(match_details)))
                subject ="Batting Scorecard file process Failed."
                message = "Batting scorecard file are missing for the match {}. Skipping the remaining process!!!!\n".format(str(match_details))
                send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)
                continue

            batting_scorecard_df = spark.createDataFrame([], schema=StructType([]))
            for file_path in batting_scorecard_file:
                temp = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path[0])
                if batting_scorecard_df.count() == 0:
                    batting_scorecard_df = temp
                    continue
                batting_scorecard_df = batting_scorecard_df.union(temp)
            if batting_scorecard_df.count()==0:
                print("batting scorecard details are not loaded successfully for the match {}. Skipping the remaining process!!!!\n".format(str(match_details)))
                subject ="Batting Scorecard file process skipped."
                message = "Batting scorecard details are not loaded successfully for the match {}. Skipping the remaining process!!!!".format(str(match_details))
                send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)
                continue
            else:
                print("Batting Scorecard information loaded sucessfully for the match {}".format(match_details))

            batting_scorecard_df= trim_space(batting_scorecard_df,batting_scorecard_df.columns)
            batting_scorecard_df = batting_scorecard_df.toDF(*batting_columns)
            remove_check_list =["\\(c\\)","†",]
            for substring in remove_check_list:
                batting_scorecard_df = batting_scorecard_df.withColumn("player_name", regexp_replace(col("player_name"), substring, ""))
            batting_scorecard_df= trim_space(batting_scorecard_df,batting_scorecard_df.columns)
            batting_scorecard_df = batting_scorecard_df.withColumn("player_name", regexp_replace(col("player_name"), "[^a-zA-Z0-9 ]", ""))
            batting_scorecard_df = batting_scorecard_df.withColumn("player_name", lower(col('player_name')))
            batting_scorecard_df = batting_scorecard_df.withColumn("player_name", regexp_replace(col("player_name"), "-", " "))
            batting_scorecard_df = batting_scorecard_df.withColumn("dismissal", regexp_replace(col("dismissal"), "-", " "))
            print("Initial cleaning process for batting scorecard file completed")

            bowling_scorecard_file = [ file for file in dbutils.fs.ls(raw_bowling_scorecard_lookup+match_details+"/scorecard/") if file[1].lower().startswith("bowling")]
            if len(bowling_scorecard_file)==0:
                print("Bowling scorecard file are missing for the match {}.. Skipping the remaining process!!!!".format(str(match_details)))
                subject ="Bowling Scorecard file process Failed."
                message = "Bowling scorecard details are not loaded successfully for the match {}. Skipping the remaining process!!!!".format(str(match_details))
                send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)
                continue
            bowling_scorecard_df = spark.createDataFrame([], schema=StructType([]))
            for file_path in bowling_scorecard_file:
                temp = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(file_path[0])
                if bowling_scorecard_df.count() == 0:
                    bowling_scorecard_df = temp
                    continue
                bowling_scorecard_df = bowling_scorecard_df.union(temp)
            if bowling_scorecard_df.count()==1:
                print("Bowling scorecard details are not loaded successfully for the match {}. Skipping the remaining process!!!!".format(str(match_details)))
                subject ="Bowling Scorecard file process skipped."
                message = "Bowling scorecard details are not loaded successfully for the match {}. Skipping the remaining process!!!!".format(str(match_details))
                send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)
                continue
            else:
                print("Bowling Scorecard information loaded sucessfully for the match {}".format(match_details))

            bowling_scorecard_df= trim_space(bowling_scorecard_df,bowling_scorecard_df.columns)
            bowling_scorecard_df = bowling_scorecard_df.toDF(*bowling_columns)
            bowling_scorecard_df = bowling_scorecard_df.withColumn("player_name", lower(col('player_name')))
            bowling_scorecard_df = bowling_scorecard_df.withColumn("player_name", regexp_replace(col("player_name"), "-", " "))
            player_df = player_df.join(bowling_scorecard_df,"player_name","full")
        
            bowling_stands_cols = bowling_scorecard_df.columns
            bowling_stands_cols.remove("player_name")

            player_df = player_df.join(batting_scorecard_df,"player_name","full")

            batting_stands_cols = batting_scorecard_df.columns
            batting_stands_cols.remove("player_name")
            batting_stands_cols.remove("dismissal")
            columns_fill_na =[]
            columns_fill_na.extend(bowling_stands_cols)
            columns_fill_na.extend(batting_stands_cols)

            player_df = fill_nan(player_df,columns_fill_na,1)
            player_df = player_df.na.fill({"dismissal": "not out"})
            player_dict = {}
            for row in player_df.select('player_name', 'player_id').collect():
                player_name = row.player_name
                player_id = row.player_id
                player_dict[player_name] = player_id
            
            player_df = player_df.withColumn('dismissal',when(col('dismissal').like('%run out%'), 'runout').otherwise(col('dismissal')))
            temp = player_df.filter(~col("dismissal").isin(["not out","runout","timed out","absent hurt","retired hurt","absent"]))
            player_df = player_df.filter(col("dismissal").isin(["not out","runout","timed out","absent hurt","retired hurt","absent"]))
            
            data = [ row.dismissal for row in temp.select("dismissal").collect()]
            dismissed_bowler,catch_dictionary = fielding_stats(data)
            result = []
            bowled_player_id =[]
            for player in dismissed_bowler:
                for player_name in  player_dict.keys():
                    if player.lower() in player_name:
                        result.append(player_name)
                        bowled_player_id.append(player_dict[player_name])
                        break
            
            #catch player fetch
            catch_taken = {}
            for player in catch_dictionary.keys():
                for player_name in  player_dict.keys():
                    if player.lower() in player_name:
                        catch_taken[player_dict[player_name]]=catch_dictionary[player]
                        break

            temp = temp.toPandas()
            temp["dismissal"]=result
            temp["dismissal_bowler_id"]= bowled_player_id
            catch_df = pd.DataFrame(list(catch_taken.items()), columns=['player_id', 'catch_taken'])
            catch_df = spark.createDataFrame(catch_df)
            temp = spark.createDataFrame(temp)
            player_df = player_df.withColumn("dismissal_bowler_id",lit(None))
            player_df = player_df.union(temp)

            player_df = player_df.join(catch_df,'player_id',"left")
            player_df = player_df.na.fill({"catch_taken": 0})

            player_df = player_df.withColumn("team_1",lit(team_name_1)).withColumn("team_2",lit(team_name_2)).withColumn("match_id",lit(match_id))
            player_df = player_df.withColumn("venue_id",lit(venue_id)).withColumn("venue_location",lit(venue_state)).withColumn("stadium",lit(stadium))
            
           # master_df_temp = spark.read.table("cricket_analytics.bronze_delta_player_master_data")
            
            master_df_temp = master_df.select("player_id","player_first_name","player_DOB","country_representative","batting_style","bowling_style")
            master_df_temp =master_df_temp.withColumnRenamed('country_representative','country_representative_code')
            master_df_temp_bowler = master_df_temp.select("player_id","bowling_style")
            master_df_temp_bowler=master_df_temp_bowler.withColumnRenamed("bowling_style","outed_bowler_style").withColumnRenamed('player_id','dismissal_bowler_id')
            
            player_df = player_df.join(master_df_temp,"player_id","inner")
            temp = player_df.filter(~col("dismissal").isin(["not out","runout","timed out","absent hurt","retired hurt","absent"]))
            player_df = player_df.filter(col("dismissal").isin(["not out","runout","timed out","absent hurt","retired hurt","absent"]))
            player_df = player_df.withColumn('outed_bowler_style',lit(None))
            columns_order = player_df.columns

            temp = temp.join(master_df_temp_bowler,"dismissal_bowler_id","inner")
            temp = temp.select(*columns_order)
            player_df=player_df.union(temp)

            #adding information from venue file
            match_summary_df = player_df.withColumn("series_name",lit(match_details_dict['Series'])).withColumn('season',lit(match_details_dict['Season'])).withColumn('match_id',lit(match_details_dict['Match_number'])).withColumn('match_date',lit(match_details_dict['Match_date'])).withColumn('own_team',lit(match_details_dict['own_team'])).withColumn('player_of_the_match_player_name',lit(match_details_dict['Player_Of_The_Match_player_name'])).withColumn('player_of_the_match_player_id',lit(match_details_dict['Player_Of_The_Match_player_id'])).withColumn('toss_own_by',lit(match_details_dict['toss_own_by'])).withColumn('toss_choice',lit(match_details_dict['toss_choice'])).withColumn('Match_type',lit(match_details_dict["Match_type"])).withColumn('country_representative',lit(None))
            
            for code, team_name in team_details.items():
                match_summary_df = match_summary_df.withColumn("country_representative", when(match_summary_df["country_representative_code"] == code, team_name).otherwise(match_summary_df["country_representative"]))

            match_summary_df = match_summary_df.withColumn("match_day",split(col("match_date")," ").getItem(0)).withColumn("match_month",split(col("match_date")," ").getItem(1)).withColumn("match_year",split(col("match_date")," ").getItem(2)).drop("match_date")

            decimal_pattern = "\\.\\d+"

            match_summary_df = match_summary_df.withColumn("match_day", expr("CASE WHEN match_day RLIKE '{}' THEN NULL ELSE match_day END".format(decimal_pattern))).withColumn("match_month", expr("CASE WHEN match_month RLIKE '{}' THEN NULL ELSE match_month END".format(decimal_pattern))).withColumn("match_year",expr("CASE WHEN match_year RLIKE '{}' THEN NULL ELSE match_year END".format(decimal_pattern)))


            match_summary_df = match_summary_df.select(*match_summary_columns)
            match_summary_df = type_cast(match_summary_df,decimal_columns,1)
            match_summary_df = type_cast(match_summary_df,no_decimal_columns,2)
            decimal_columns.extend(no_decimal_columns)
            match_summary_df = fill_nan(match_summary_df,decimal_columns,1)
            match_summary_df = fill_nan(match_summary_df,match_summary_df.columns,2)
            match_summary_df = cols_upper(match_summary_df,string_columns)
            match_summary_df = match_summary_df.withColumn("record_created_timestamp",lit(formatted_datetime))
            if match_summary_df.count() ==22:
                print("writting block")
                match_summary_df = replace_information(replace_information_dict["season"],match_summary_df,"season")
                match_summary_df = replace_information(replace_information_dict["series_name"],match_summary_df,"series_name")
                match_summary_df = match_summary_df.withColumn("match_day", when(col("match_day") == 0, 1).otherwise(col("match_day")))
                match_summary_df = match_summary_df.withColumn("match_month", when(col("match_month").isNull(), "JANUARY").otherwise(col("match_month")))
                match_summary_df = match_summary_df.withColumn("match_year", when(col("match_year")==0, col("season")).otherwise(col("match_year")))
                match_summary_df.write.format("delta").mode("append").partitionBy("record_created_timestamp","series_name","season","match_type","match_id").save(refined_match_summary)
                schema = ["processed_match_details"]
                rows = [Row(processed_match_details=match_details)]
                processed_match_details = spark.createDataFrame(rows, schema)
                processed_match_details =processed_match_details.withColumn("record_created_timestamp",lit(formatted_datetime))
                processed_match_details.write.format("delta").mode("append").partitionBy("record_created_timestamp").save(refined_reconciliation_lookup)
                subject ="Match summary process completed successfully"
                message = "Match summary file is generated successfully for the match {}.".format(str(match_details))
                print("E-mail communication send \n")
            else:
                print("Failed")
                subject ="Match summary process completed Failed"
                message = "Match summary file is not generated successfully for the match {}. Due to unexpected record count found".format(str(match_details))
                send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)
                schema = ["processed_match_details"]
                rows = [Row(processed_match_details=match_details)]
                processed_match_details = spark.createDataFrame(rows, schema)
                processed_match_details.write.format("delta").mode("append").save("/mnt/silver-layer/fall_2023_finals/junk/")
                continue
        except Exception as error:
            print("Exception occur for the match {} due to {}".format(str(match_details),str(error)))
            sys.exit(1)

else:
    print("Nothing to Process as new match")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Loading and creating the external silver table for players master data 

# COMMAND ----------

spark.sql(f"DROP TABLE  IF EXISTS cricket_analytics.silver_player_master_data;")
spark.sql(f"CREATE TABLE cricket_analytics.silver_player_master_data USING DELTA OPTIONS ('path'='dbfs:{refined_player_master_data_lookup}')")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) from cricket_analytics.silver_player_master_data;

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Loading and creating the external silver table for Match Summary data 

# COMMAND ----------

spark.sql(f"DROP TABLE  IF EXISTS cricket_analytics.silver_match_summary;")
spark.sql(f"CREATE TABLE cricket_analytics.silver_match_summary USING DELTA OPTIONS ('path'='dbfs:{refined_match_summary}')")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(*) from cricket_analytics.silver_match_summary;

# COMMAND ----------

schema = StructType([
    StructField("Team_ID", IntegerType(), True),
    StructField("Team_Name", StringType(), True)
])
data = [(team_id, team_name) for team_id, team_name in team_details.items()]
team_df = spark.createDataFrame(data, schema=schema)
team_df.createOrReplaceTempView("team_details")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From team_details;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Caching the main tables

# COMMAND ----------

# MAGIC %sql
# MAGIC cache table cricket_analytics.silver_player_master_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_analytics.silver_player_master_data limit 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC cache table cricket_analytics.silver_match_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM cricket_analytics.silver_match_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating the Dimension tables

# COMMAND ----------

try:
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/dim_match_type", True)
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/dim_series_names_info", True)
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/dim_seasons_info", True)
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/dim_match_details", True)
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/dim_team_info", True)
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/dim_player_details", True)
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/dim_toss_choice", True)
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/dim_venue_location", True)
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/dim_stadium", True)
    dbutils.fs.rm('dbfs:/user/hive/warehouse/cricket_analytics.db/dim_date',True)
    
except Exception as e:
    print(f"Error: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Dimensional table for Match Type

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.dim_match_type;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.dim_match_type (match_type_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),match_type_hash_value STRING NOT NULL, match_type STRING NOT NULL);

# COMMAND ----------

spark.sql('''
    MERGE INTO cricket_analytics.dim_match_type AS Target
    USING (
        SELECT DISTINCT
            MD5(match_type) AS new_sno,
            match_type
        FROM
            cricket_analytics.silver_match_summary
    ) AS Source
    ON Target.match_type = Source.match_type
    WHEN NOT MATCHED THEN
        INSERT(match_type_hash_value, match_type)
        VALUES (Source.new_sno, Source.match_type);
''')


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from cricket_analytics.dim_match_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Dimensional table for Series information

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.dim_series_names_info;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.dim_series_names_info(series_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),series_name_hash_value STRING NOT NULL, series_name STRING NOT NULL);

# COMMAND ----------

spark.sql('''
    MERGE INTO cricket_analytics.dim_series_names_info AS Target
    USING (
        SELECT DISTINCT
            MD5(series_name) AS hash_series_name,
            series_name
        FROM
            cricket_analytics.silver_match_summary
    ) AS Source
    ON Target.series_name = Source.series_name
    WHEN NOT MATCHED THEN
        INSERT(series_name_hash_value, series_name)
        VALUES (Source.hash_series_name, Source.series_name);
''')


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cricket_analytics.dim_series_names_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Dimensional table for Season information

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.dim_seasons_info;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.dim_seasons_info(season_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),season_hash_value STRING NOT NULL, season INT NOT NULL);

# COMMAND ----------

spark.sql('''
    MERGE INTO cricket_analytics.dim_seasons_info AS Target
    USING (
        SELECT DISTINCT
            MD5(CAST(season AS STRING)) AS hash_season,
            season
        FROM
            cricket_analytics.silver_match_summary
    ) AS Source
    ON Target.season = Source.season
    WHEN NOT MATCHED THEN
        INSERT(season_hash_value, season)
        VALUES (Source.hash_season, Source.season);
''')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_analytics.dim_seasons_info order by season;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Dimensional table for Match Details by fetching the match type, series, season information

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.dim_match_details;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.dim_match_details(match_surrogate_key  BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1) ,match_details_hash_value STRING NOT NULL, match_number INT NOT NULL,match_type_ref_key STRING NOT NULL,series_name_ref_key String NOT NULL,season_ref_key STRING NOT NULL);

# COMMAND ----------

spark.sql("""
    INSERT INTO cricket_analytics.dim_match_details (
        match_details_hash_value,
        match_number,
        match_type_ref_key,
        series_name_ref_key,
        season_ref_key
    )
    SELECT DISTINCT
        MD5(CONCAT_WS('-', mt.match_type, sn.series_name, s.season, ms.match_id)) AS match_surrogate_key,
        ms.match_id,
        mt.match_type_hash_value,
        sn.series_name_hash_value,
        s.season_hash_value
    FROM
        cricket_analytics.silver_match_summary ms
    JOIN
        cricket_analytics.dim_match_type mt
        ON ms.match_type = mt.match_type
    JOIN
        cricket_analytics.dim_series_names_info sn
        ON ms.series_name = sn.series_name
    JOIN
        cricket_analytics.dim_seasons_info s
        ON ms.season = s.season
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * FROM cricket_analytics.dim_match_details limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Dimensional table for Team information

# COMMAND ----------

# MAGIC %sql 
# MAGIC DROP TABLE IF EXISTS cricket_analytics.dim_team_info;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.dim_team_info(team_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),team_hash_value STRING NOT NULL,team_name STRING NOT NULL,team_code INT NOT NULL);

# COMMAND ----------

spark.sql("""
    MERGE INTO cricket_analytics.dim_team_info AS Target
    USING (
        SELECT DISTINCT
            MD5(CONCAT_WS('-', Team_ID, Team_Name)) AS team_key,
            Team_ID,
            UPPER(REGEXP_REPLACE(Team_Name, '-', ' ')) AS Team_Name
        FROM
            team_details
    ) AS Source
    ON Target.team_surrogate_key = Source.team_key
    WHEN NOT MATCHED THEN
        INSERT(team_hash_value, team_name, team_code)
        VALUES (Source.team_key, Source.Team_Name, Source.Team_ID);
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_analytics.dim_team_info;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Dimensional table for Player information

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.dim_player_details;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.dim_player_details(player_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1) ,player_hash_value STRING NOT NULL,player_id INT NOT NULL,player_first_name STRING NOT NULL,player_last_name STRING NOT NULL,date_of_birth DATE NOT NULL,batting_style STRING NOT NULL,bowling_style STRING NOT NULL )

# COMMAND ----------

spark.sql('''
    MERGE INTO cricket_analytics.dim_player_details AS Target
    USING (
        SELECT DISTINCT
            MD5(CAST(player_id AS STRING)) AS hash_value,
            player_id,
            player_first_name,
            player_last_name,
            COALESCE(player_DOB, "1975-01-01") AS player_DOB,
            COALESCE(batting_style,"RIGHT-HAND BAT") AS batting_style,
            COALESCE(bowling_style,"RIGHT-ARM OFFBREAK") AS bowling_style
        FROM
            cricket_analytics.silver_player_master_data
    ) AS Source
    ON Target.player_id = Source.player_id
    WHEN NOT MATCHED THEN
        INSERT(player_hash_value, player_id, player_first_name, player_last_name, date_of_birth,batting_style,bowling_style)
        VALUES (Source.hash_value, Source.player_id, Source.player_first_name, Source.player_last_name, Source.player_DOB,Source.batting_style,Source.bowling_style);
''')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From cricket_analytics.dim_player_details limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Dimensional table for date dimensions

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.dim_date;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.dim_date(date_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),date_hash_value STRING NOT NULL,match_date INT NOT NULL,match_month STRING NOT NULL,match_year INT NOT NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO cricket_analytics.dim_date AS Target
# MAGIC USING (
# MAGIC   SELECT DISTINCT MD5(CONCAT_WS("-",match_day,match_month,match_year)) AS date_surrogate_key,
# MAGIC   match_day,
# MAGIC   match_month,
# MAGIC   match_year FROM cricket_analytics.silver_match_summary
# MAGIC ) AS Source
# MAGIC ON Target.date_surrogate_key = Source.date_surrogate_key
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (date_hash_value,match_date,match_month,match_year)
# MAGIC   VALUES (Source.date_surrogate_key,Source.match_day,Source.match_month,Source.match_year)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_analytics.dim_date

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Dimensional table for Toss choice

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.dim_toss_choice;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.dim_toss_choice(toss_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1) ,toss_hash_value STRING NOT NULL, toss_choice STRING NOT NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO cricket_analytics.dim_toss_choice AS Target
# MAGIC USING(
# MAGIC   SELECT DISTINCT Md5(toss_choice) as hash_value,
# MAGIC   toss_choice
# MAGIC   FROM cricket_analytics.silver_match_summary
# MAGIC ) AS Source
# MAGIC ON Target.toss_choice = Source.toss_choice
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT(toss_hash_value,toss_choice)
# MAGIC   VALUES(Source.hash_value,Source.toss_choice)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_analytics.dim_toss_choice

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Dimensional table for Venu location

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.dim_venue_location;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.dim_venue_location(venue_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),venue_hash_value STRING NOT NULL, venue_name STRING NOT NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO cricket_analytics.dim_venue_location AS Target
# MAGIC USING(
# MAGIC   SELECT DISTINCT MD5(venue_location) as hash_value,
# MAGIC   venue_location
# MAGIC   FROM cricket_analytics.silver_match_summary
# MAGIC ) AS Source
# MAGIC ON Target.venue_name = Source.venue_location
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT(venue_hash_value,venue_name)
# MAGIC   VALUES(Source.hash_value,Source.venue_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_analytics.dim_venue_location

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Dimensional table for Stadium information

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.dim_stadium;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.dim_stadium(stadium_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),stadium_hash_value STRING NOT NULL,stadium_id INT NOT NULL,stadium_name STRING NOT NULL, venue_ref_key STRING NOT NULL);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO cricket_analytics.dim_stadium(stadium_hash_value,stadium_id,stadium_name,venue_ref_key)
# MAGIC SELECT DISTINCT 
# MAGIC     MD5(CONCAT_WS("-",ms.venue_id,ms.stadium,vl.venue_hash_value)) AS hash_value,
# MAGIC     ms.venue_id,
# MAGIC     ms.stadium,
# MAGIC     vl.venue_hash_value
# MAGIC FROM 
# MAGIC   cricket_analytics.silver_match_summary ms
# MAGIC JOIN
# MAGIC   cricket_analytics.dim_venue_location vl
# MAGIC ON
# MAGIC   vl.venue_name = ms.venue_location

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_analytics.dim_stadium

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating the Fact tables

# COMMAND ----------

try:
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/fact_match_info", True)
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/fact_batting_scorecard", True)
    dbutils.fs.rm("dbfs:/user/hive/warehouse/cricket_analytics.db/fact_bowling_scorecard", True)
except Exception as e:
    print(f"Error: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Fact less Fact table for match level information

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.fact_match_info;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.fact_match_info(match_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),match_ref_key STRING NOT NULL,toss_choice_ref_key STRING NOT NULL,match_date_ref_key STRING NOT NULL,player_of_the_match_ref_key STRING NOT NULL,team_1_ref_key STRING NOT NULL,team_2_ref_key STRING NOT NULL,toss_own_team_ref_key STRING NOT NULL,match_own_team_ref_key STRING NOT NULL)

# COMMAND ----------

spark.sql("""
    INSERT INTO cricket_analytics.fact_match_info (
        match_ref_key,
        toss_choice_ref_key,
        match_date_ref_key,
        player_of_the_match_ref_key,
        team_1_ref_key,
        team_2_ref_key,
        toss_own_team_ref_key,
        match_own_team_ref_key
    )
    SELECT DISTINCT
        match_details.match_details_hash_value,
        toss_selection.toss_hash_value,
        date_info.date_hash_value,
        pd.player_hash_value,
        team_info_1.team_hash_value,
        team_info_2.team_hash_value,
        toss_own.team_hash_value,
        own_team_info.team_hash_value
    FROM
        cricket_analytics.silver_match_summary match_summary
    JOIN
        cricket_analytics.dim_match_details match_details
        ON match_details.match_details_hash_value = MD5(CONCAT_WS("-", match_summary.match_type, match_summary.series_name, match_summary.season, match_summary.match_id))
    JOIN
        cricket_analytics.dim_player_details pd
        ON pd.player_id = match_summary.player_of_the_match_player_id
    JOIN
        cricket_analytics.dim_team_info team_info_1
        ON team_info_1.team_name = match_summary.team_1
    JOIN
        cricket_analytics.dim_team_info team_info_2
        ON team_info_2.team_name = match_summary.team_2
    JOIN
        cricket_analytics.dim_team_info toss_own
        ON toss_own.team_name = match_summary.toss_own_by
    JOIN
        cricket_analytics.dim_team_info own_team_info
        ON own_team_info.team_name = match_summary.own_team
    JOIN
        cricket_analytics.dim_toss_choice toss_selection
        ON toss_selection.toss_choice = match_summary.toss_choice
    JOIN
        cricket_analytics.dim_date date_info
        ON date_info.date_hash_value = MD5(CONCAT_WS("-", match_summary.match_day, match_summary.match_month, match_summary.match_year))
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from cricket_analytics.fact_match_info limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Fact table for batting scorecard

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.fact_batting_scorecard;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.fact_batting_scorecard (batting_scorecard_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),match_ref_key INTEGER NOT NULL,match_type_ref_key INTEGER NOT NULL,player_ref_key INTEGER NOT NULL,runs_scored INTEGER NOT NULL,ball_faced INTEGER NOT NULL,minutes_send INTEGER NOT NULL,fours_scored INTEGER NOT NULL,sixes_scored INTEGER NOT NULL,strike_rate DOUBLE NOT NULL,catch_taken INTEGER NOT NULL,dimissial_player_name STRING NOT NULL,dimissial_player_id INTEGER NOT NULL)

# COMMAND ----------

spark.sql('''
INSERT INTO cricket_analytics.fact_batting_scorecard(
    match_ref_key,
    match_type_ref_key,
    player_ref_key,
    runs_scored,
    ball_faced,
    minutes_send,
    fours_scored,
    sixes_scored,
    strike_rate,
    catch_taken,
    dimissial_player_name,
    dimissial_player_id
)
SELECT 
    match_details.match_surrogate_key,
    match_type_details.match_type_surrogate_key,
    player_details.player_surrogate_key,
    match_summary.runs_scored,
    match_summary.balls_faced,
    match_summary.minutes_send,
    match_summary.fours_count,
    match_summary.six_count,
    match_summary.strike_rate,
    match_summary.catch_taken,
    match_summary.dismissal,
    match_summary.dismissal_bowler_id
FROM cricket_analytics.silver_match_summary match_summary
JOIN cricket_analytics.dim_match_type match_type_details
  ON match_type_details.match_type_hash_value = MD5(match_summary.match_type)
JOIN cricket_analytics.dim_match_details match_details
  ON match_details.match_details_hash_value = MD5(CONCAT_WS("-", match_summary.match_type, match_summary.series_name, match_summary.season, match_summary.match_id))
JOIN cricket_analytics.dim_player_details player_details
  ON player_details.player_id = match_summary.player_id
''')


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_analytics.fact_batting_scorecard limit 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the Fact table for bowling scorecard

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS cricket_analytics.fact_bowling_scorecard;
# MAGIC CREATE TABLE IF NOT EXISTS cricket_analytics.fact_bowling_scorecard (bowling_scorecard_surrogate_key BIGINT GENERATED BY DEFAULT AS IDENTITY(START WITH 1 INCREMENT BY 1),match_ref_key INTEGER NOT NULL,match_type_ref_key INTEGER NOT NULL,player_ref_key INTEGER NOT NULL,over_bowled DOUBLE NOT NULL,maiden_over INTEGER NOT NULL,wickets_taken INTEGER NOT NULL,economy DOUBLE NOT NULL,dots_balls_bowled INTEGER NOT NULL,fours_scored_batter INTEGER NOT NULL,sixs_scored_batter INTEGER NOT NULL, wide_ball_count INTEGER NOT NULL,no_ball_count INTEGER NOT NULL,runs_given INTEGER NOT NULL)

# COMMAND ----------

spark.sql('''
INSERT INTO cricket_analytics.fact_bowling_scorecard(
       match_ref_key,
       match_type_ref_key,
       player_ref_key,
       over_bowled,
       maiden_over,
       wickets_taken,
       economy,
       dots_balls_bowled,
       fours_scored_batter,
       sixs_scored_batter,
       wide_ball_count,
       no_ball_count,
       runs_given
)
SELECT 
    match_details.match_surrogate_key,
    match_type.match_type_surrogate_key,
    player_details.player_surrogate_key,
    match_summary.overs_bowled,
    match_summary.maiden_over,
    match_summary.wickets_taken,
    match_summary.economy,
    match_summary.dots_ball_bowled,
    match_summary.fours_scored_by_batter,
    match_summary.six_scored_by_batter,
    match_summary.wide_ball,
    match_summary.no_balls,
    match_summary.runs_given
FROM cricket_analytics.silver_match_summary match_summary
JOIN cricket_analytics.dim_match_type match_type
  ON match_type.match_type_hash_value = MD5(match_summary.match_type)
JOIN cricket_analytics.dim_match_details match_details
  ON match_details.match_details_hash_value = MD5(CONCAT_WS("-", match_summary.match_type, match_summary.series_name, match_summary.season, match_summary.match_id))
JOIN cricket_analytics.dim_player_details player_details
  ON player_details.player_id = match_summary.player_id
  ''')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Glod Layer Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Format wise analysis 

# COMMAND ----------

format_wise_analysis_data = spark.sql("""
    SELECT 
        season,
        series_name,
        team_name,
        dim_match_type.match_type as flag,
        team_code,
        count(*) as match_win_count
    FROM cricket_analytics.fact_match_info
    JOIN cricket_analytics.dim_match_details
        ON cricket_analytics.dim_match_details.match_details_hash_value = cricket_analytics.fact_match_info.match_ref_key
    JOIN cricket_analytics.dim_match_type
        ON cricket_analytics.dim_match_details.match_type_ref_key = cricket_analytics.dim_match_type.match_type_hash_value
    JOIN cricket_analytics.dim_seasons_info
        ON cricket_analytics.dim_seasons_info.season_hash_value = cricket_analytics.dim_match_details.season_ref_key
    JOIN cricket_analytics.dim_series_names_info
        ON cricket_analytics.dim_series_names_info.series_name_hash_value = cricket_analytics.dim_match_details.series_name_ref_key
    JOIN cricket_analytics.dim_team_info
        ON cricket_analytics.dim_team_info.team_hash_value = cricket_analytics.fact_match_info.toss_own_team_ref_key
    GROUP BY season, series_name, team_name, team_code, flag
""")

# COMMAND ----------

display(format_wise_analysis_data)

# COMMAND ----------

if len(dbutils.fs.ls(format_wise_analysis)):
     dbutils.fs.rm(format_wise_analysis,True)
format_wise_analysis_data.write.csv(format_wise_analysis+"/data", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Batting Analysis for Glod Layer

# COMMAND ----------


temp = spark.sql('''
SELECT  
        CONCAT_WS("-", player_details.player_first_name, player_details.player_last_name) as player_name,
        player_ref_key,
        match_type_details.match_type as flag,
        season_info.season as season_key,
        dimissial_player_id
    FROM 
        fact_batting_scorecard
    JOIN 
        dim_player_details player_details
        ON player_details.player_surrogate_key = fact_batting_scorecard.player_ref_key
    JOIN 
        dim_match_type match_type_details
        ON match_type_details.match_type_surrogate_key = fact_batting_scorecard.match_type_ref_key
    JOIN 
        dim_match_details match_details_info
        ON match_details_info.match_surrogate_key = fact_batting_scorecard.match_ref_key
    JOIN 
        dim_seasons_info season_info
        ON season_info.season_hash_value = match_details_info.season_ref_key
                 ''')
outed_batsmean = temp.filter(col("dimissial_player_id")!=0)
player_details = spark.read.table("dim_player_details")
outed_bowler_type = outed_batsmean.join(player_details,outed_batsmean.dimissial_player_id == player_details.player_id,"inner")
outed_bowler_type = outed_bowler_type.select("player_name","player_ref_key","flag","season_key","bowling_style")
outed_bowler_type = outed_bowler_type.withColumn("bowling_style", lower(col("bowling_style")))
pivotedDF = (outed_bowler_type
             .groupBy("season_key", "player_name", "player_ref_key", "flag")
             .pivot("bowling_style")
             .agg(count("player_name")))
pivotedDF=pivotedDF.fillna(0)

# COMMAND ----------

player_statss_df = spark.sql('''
WITH PlayerStats AS (
    SELECT  
        CONCAT_WS("-", player_details.player_first_name, player_details.player_last_name) as player_name,
        player_ref_key,
        match_type_details.match_type as flag,
        season_info.season as season_key,
        SUM(runs_scored) as total_runs,
        SUM(fours_scored) as total_fours,
        SUM(sixes_scored) as total_sixes,
        SUM(catch_taken) as total_catch_taken,
        AVG(strike_rate) as avg_strike_rate,
        RANK() OVER (PARTITION BY season_info.season, match_type_details.match_type ORDER BY SUM(runs_scored) DESC) as runs_rank,
        RANK() OVER (PARTITION BY season_info.season, match_type_details.match_type ORDER BY AVG(strike_rate) DESC) as strike_rate_rank,
        RANK() OVER (PARTITION BY season_info.season, match_type_details.match_type ORDER BY SUM(fours_scored) DESC) as fours_rank,
        RANK() OVER (PARTITION BY season_info.season, match_type_details.match_type ORDER BY SUM(sixes_scored) DESC) as sixes_rank
    FROM 
        fact_batting_scorecard
    JOIN 
        dim_player_details player_details
        ON player_details.player_surrogate_key = fact_batting_scorecard.player_ref_key
    JOIN 
        dim_match_type match_type_details
        ON match_type_details.match_type_surrogate_key = fact_batting_scorecard.match_type_ref_key
    JOIN 
        dim_match_details match_details_info
        ON match_details_info.match_surrogate_key = fact_batting_scorecard.match_ref_key
    JOIN 
        dim_seasons_info season_info
        ON season_info.season_hash_value = match_details_info.season_ref_key
    GROUP BY 
        player_name,player_ref_key, flag, season_key,flag
)

SELECT 
    player_name,
    player_ref_key,
    flag,
    season_key,
    total_runs,
    total_fours,
    total_sixes,
    total_catch_taken,
    avg_strike_rate,
    runs_rank,
    strike_rate_rank,
    fours_rank,
    sixes_rank
FROM 
    PlayerStats;

''')

# COMMAND ----------

final_result_batting_analysis = player_statss_df.join(pivotedDF,on=["player_ref_key","season_key","flag","player_name"],how="left")
final_result_batting_analysis = final_result_batting_analysis.fillna(0)

# COMMAND ----------

if len(dbutils.fs.ls(batting_analysis)):
     dbutils.fs.rm(batting_analysis,True)
final_result_batting_analysis.write.csv(batting_analysis+"/data", header=True)

# COMMAND ----------

display(final_result_batting_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Bowling Performance Aanlysis

# COMMAND ----------

bowling_analysis_df = spark.sql('''
SELECT
  CONCAT_WS("-", player_details.player_first_name, player_details.player_last_name) as Player_name,
  match_type_details.match_type as flag,
  season_info.season as season_key,
  SUM(over_bowled) as total_bowled_overs,
  SUM(maiden_over) as total_maiden_bowled_overs,
  SUM(economy) as Economy,
  SUM(dots_balls_bowled) as total_dot_bowled,
  SUM(fours_scored_batter) as fours_given,
  SUM(sixs_scored_batter) as six_given,
  SUM(wide_ball_count) as total_wide_balls_bowled,
  SUM(no_ball_count) as total_no_balls_bowled,
  SUM(runs_given) as total_runs_given
FROM fact_bowling_scorecard
JOIN dim_player_details player_details
  ON player_details.player_surrogate_key = fact_bowling_scorecard.player_ref_key
JOIN dim_match_type match_type_details
  ON match_type_details.match_type_surrogate_key = fact_bowling_scorecard.match_type_ref_key
JOIN dim_match_details match_details_info
  ON match_details_info.match_surrogate_key = fact_bowling_scorecard.match_ref_key
JOIN dim_seasons_info season_info
  ON season_info.season_hash_value = match_details_info.season_ref_key 
GROUP BY Player_name, flag, season_key
HAVING
  SUM(over_bowled) != 0 OR
  SUM(maiden_over) != 0 OR
  SUM(economy) != 0 OR
  SUM(dots_balls_bowled) != 0 OR
  SUM(fours_scored_batter) != 0 OR
  SUM(sixs_scored_batter) != 0 OR
  SUM(wide_ball_count) != 0 OR
  SUM(no_ball_count) != 0 OR
  SUM(runs_given) != 0;
''')

# COMMAND ----------

if len(dbutils.fs.ls(bowling_analysis)):
     dbutils.fs.rm(bowling_analysis,True)
bowling_analysis_df.write.csv(bowling_analysis+"/data", header=True)

# COMMAND ----------

display(bowling_analysis_df)

# COMMAND ----------

subject ="Cricket Analytics Exceution Completed"
message = "Cricket Analytics Exceution Completed {}".format(str(formatted_datetime))
send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Unit testing

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Test Case-1 Checking the distinct match processed count for each format type

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from (SELECT DISTINCT match_id, match_type FROM cricket_analytics.silver_match_summary)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cricket_analytics.dim_match_details

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Test Case-2 Checking Null values in Dimension Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM cricket_analytics.dim_match_type WHERE match_type_surrogate_key IS NULL OR match_type_hash_value IS NULL OR match_type IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_seasons_info WHERE season_surrogate_key IS NULL OR season_hash_value IS NULL OR season IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_series_names_info WHERE series_surrogate_key IS NULL OR series_name_hash_value IS NULL OR series_name IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_match_details WHERE match_surrogate_key IS NULL OR match_details_hash_value IS NULL OR match_number IS NULL OR match_number IS NULL OR match_type_ref_key IS NULL OR series_name_ref_key IS NULL OR season_ref_key IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_player_details WHERE player_surrogate_key IS NULL OR player_hash_value IS NULL OR player_id IS NULL OR player_first_name IS NULL OR player_last_name IS NULL OR date_of_birth IS NULL OR batting_style IS NULL OR bowling_style IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_date WHERE date_surrogate_key IS NULL OR date_hash_value IS NULL OR match_date IS NULL OR match_month IS NULL OR match_year IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_venue_location WHERE venue_surrogate_key IS NULL OR venue_hash_value IS NULL OR venue_name IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_stadium where stadium_surrogate_key IS NULL OR stadium_hash_value IS NULL OR stadium_id IS NULL OR stadium_name IS NULL OR venue_ref_key IS NULL 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_toss_choice WHERE toss_surrogate_key IS NULL OR toss_hash_value IS NULL OR toss_choice IS NULL

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Test Case-3 Record Mapping

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM fact_batting_scorecard;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM fact_bowling_scorecard

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM silver_match_summary

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Test Case-4 batting performance on format and series wise checking

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_player_details where player_surrogate_key = 883

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *  FROM fact_batting_scorecard where player_ref_key = 883 and match_ref_key = 494
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from silver_match_summary where match_id in (SELECT match_number FROM dim_match_details where match_surrogate_key = 494) and player_id =34102

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Test Case-5 Bowling Performance on format and series wise checking

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dim_player_details where player_surrogate_key = 977

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *  FROM fact_bowling_scorecard where player_ref_key = 977 and match_ref_key = 485
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from silver_match_summary where match_id in (SELECT match_number FROM dim_match_details where match_surrogate_key = 485) and player_id =625383

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test case-6 Email Communication

# COMMAND ----------


