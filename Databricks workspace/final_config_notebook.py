# Databricks notebook source
player_columns =["player_name","player_id","captain_flag"]

batting_columns = ["player_name","dismissal","runs_scored","balls_faced","minutes_send","fours_count","six_count","strike_rate"]

bowling_columns = ["player_name","overs_bowled","maiden_over","runs_given","wickets_taken","economy","dots_ball_bowled","fours_scored_by_batter","six_scored_by_batter","wide_ball","no_balls"]

master_columns = ["player_id","player_first_name","player_last_name","player_DOB","country_representative","batting_style","bowling_style"]

match_summary_columns = ["match_id","match_type","series_name","season","team_1","team_2","toss_own_by","toss_choice","own_team","match_day","match_month","match_year","player_of_the_match_player_name","player_of_the_match_player_id","venue_id","venue_location","stadium","player_id","player_name","player_DOB","country_representative","country_representative_code","captain_flag","batting_style","runs_scored","balls_faced","minutes_send","fours_count","six_count","strike_rate","dismissal","dismissal_bowler_id","outed_bowler_style","catch_taken","bowling_style","overs_bowled","maiden_over","runs_given","wickets_taken","economy","dots_ball_bowled","fours_scored_by_batter","six_scored_by_batter","wide_ball","no_balls"]

string_columns = ["match_type","series_name","season","team_1","team_2","toss_own_by","toss_choice","own_team","match_month","player_of_the_match_player_name","venue_location","stadium","player_name","country_representative","batting_style","captain_flag","dismissal","outed_bowler_style","bowling_style",]

no_decimal_columns = ["overs_bowled","economy","strike_rate"]

decimal_columns = ["maiden_over","runs_given","wickets_taken","dots_ball_bowled","fours_scored_by_batter","six_scored_by_batter","wide_ball","no_balls","catch_taken","runs_scored","balls_faced","minutes_send","fours_count","six_count","match_day","match_year","match_id","player_of_the_match_player_id","venue_id","player_id","country_representative_code","dismissal_bowler_id"]

# COMMAND ----------

team_details = {

40:"Afghanistan",
2:"Australia",
25:"Bangladesh",
1:"England",
6:"India",
29:"Ireland",
5:"New-Zealand",
7:"Pakistan",
3:"South-Africa",
8:"Sri-Lanka",
4:"West-Indies",
9:"Zimbabwe",
15:"Netherlands",
26:"kenya",
27:"united-arab-emirates",
30:"scotland",
17:"canada",
12:"bermuda",
335974:"chennai-super-kings",
335970:"royal-challengers-bangalore",
335975:"delhi-capitals",
335978:"mumbai-indians",
968721:"rising-pune-supergiants",
628333:"sunrisers-hyderabad",
335973:"punjab-kings",
335971:"kolkata-knight-riders",
335977:"rajasthan-royals",
474668:"kochi-tuskers-kerala",
474666:"pune-warriors",
968725:"gujarat-lions",
28:"namibia"
}

# COMMAND ----------

replace_information_dict ={
    "season":
    
    {
            "2006-07-01":"2006",
            "2019":"2019",
            "2010-11-01":"2011",
            "2023/24":"2023",
            "2014/15":"2015",
            "2002-03-01":"2003",
            "2021":"2021",
            "2007-08-01":"2007",
            "2009-10-01":"2009",
            "1987/88":"1987",
            "1995/96":"1995",
            "1991/92":"1991",
            "2020/21":"2020"
    },
    'series_name': 
        {
        
            'ICC CRICKET WORLD CUP': ['ICC WORLD CUP'],
            'PEPSI INDIAN PREMIER LEAGUE':['INDIAN PREMIER LEAGUE']
        
        }

}

# COMMAND ----------

raw_mount_point = "/mnt/bronze-layer/fall_2023_finals/"
raw_master_lookup = raw_mount_point+"players-details/"
raw_match_details = raw_mount_point+"match-details/"
raw_batting_scorecard_lookup=raw_mount_point+"match-details/batting-scorecard/"
raw_bowling_scorecard_lookup=raw_mount_point+"match-details/bowling-scorecard/"
raw_team_lookup=raw_mount_point+"match-details/team-details/"
raw_venue_lookup=raw_mount_point+"match-details/venue-details/"

#delta lookup
raw_master_delta_lookup = raw_mount_point+"players-details-delta/"
raw_batting_scorecard_delta_lookup=raw_mount_point+"match-details/delta/batting-scorecard/"
raw_bowling_scorecard_delta_lookup=raw_mount_point+"match-details/delta/bowling-scorecard/"
raw_team_delta_lookup=raw_mount_point+"match-details/delta/team-details/"
raw_venue_delta_lookup=raw_mount_point+"match-details/delta/venue-details/"


# COMMAND ----------

refined_mount_point = "/mnt/silver-layer/fall_2023_finals/"
refined_player_master_data_lookup = refined_mount_point+"player-master-data/"
refined_match_summary = refined_mount_point+"match-summaries/"
refined_reconciliation_lookup = refined_mount_point+"reconciliation/processed-match-details/"

# COMMAND ----------

aggregated_mount_point = "/mnt/gold-layer/fall_2023_finals/"
format_wise_analysis = aggregated_mount_point+"formate-wise-analysis/"
batting_analysis = aggregated_mount_point+"batting_analysis/"
bowling_analysis = aggregated_mount_point+"bowling_analysis/"

# COMMAND ----------

from_mail_id ="abc@gmail.com"
from_mail_id_password="XXX XXX XXX"
to_email_id = "xyz@gmail.com"
