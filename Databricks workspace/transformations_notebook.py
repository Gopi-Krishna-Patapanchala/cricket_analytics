# Databricks notebook source
def trim_space(df,col_names):
    try:
        for col_name in col_names:
            df = df.withColumn(col_name, trim(col(col_name)))
        return df
    except Exception as error:
        print("Exception occur due to {}".format(str(error)))
        sys.exit(1)

# COMMAND ----------

def type_cast(df,columns,flag):
    try:
        for col in columns:
            if flag == 1:
                df = df.withColumn(col, df[col].cast(IntegerType())) 
            elif flag == 2:
                df = df.withColumn(col, df[col].cast(DoubleType()))
        return df
    except Exception as error:
        print("Exception occur due to {}".format(str(error)))
        sys.exit(1)

# COMMAND ----------

def fill_nan(df,cols,flag):
    try:
        for col_name in cols:
            if flag ==1:
                df = df.withColumn(col_name, when(col(col_name).isNull(), 0).otherwise(col(col_name)))
            elif flag == 2:
                df = df.withColumn(col_name, when(col(col_name) == "-", 0).otherwise(col(col_name)))
        return df
    except Exception as error:
        print("Exception occur due to {0}".format(str(error)))
        sys.exit(1)

# COMMAND ----------

def fielding_stats(data):
    try:
        bad_chars = [';', ':', '!', "*","†","‚","'"]
        caught_taken={}
        bowler =[]
        # stump_count = {}
        for row in data:
            if  row.startswith("c"):
                if 'c & b' in row:
                    row = row.replace('c & b','b')
                split_text = row.split(" ")
                split_text = [ "bowler"if i=="b" else i for i in split_text]
                split_text = [split_text[split_text.index("bowler")-1],split_text[split_text.index("bowler")+1]]
                split_text =[ player for player in split_text if len(player)>1]
                if len(split_text)>=1:
                    player = split_text[0].strip()
                    player = ''.join(letter for letter in player if not letter in bad_chars)
                    if  player in caught_taken.keys():
                        caught_taken[player]+=1
                    else:
                        caught_taken[player]=1
                bowl_player = split_text[-1]
                bowl_player = ''.join(letter for letter in bowl_player if not letter in bad_chars)
                bowler.append(bowl_player.strip())
            
            elif row.startswith("b") or row.startswith("l")  :
                replace_text = row.split(" ")
                replace_text=replace_text[-1].strip()
                replace_text = ''.join(letter for letter in replace_text if not letter in bad_chars)
                bowler.append(replace_text)

            elif row.startswith("st") or row.startswith("hit"):
                split_text = row.split(" ")
                split_text = [ "bowler" if i == "b" else i for i in split_text]
                bowl_player = split_text[split_text.index("bowler")+1]
                bowl_player = ''.join(letter for letter in bowl_player if letter in bad_chars)
                bowler.append(bowl_player.strip())
        return bowler,caught_taken
    except Exception as error:
        print("Exception occur due to {0}".format(str(error)))
        sys.exit(1)

# COMMAND ----------

def send_mail(from_mail_id,from_mail_id_password,to_email_id,subject,message):
    try:
        msg = MIMEText(message,"html")
        msg['Subject'] = subject
        msg['To']=to_email_id
        msg['From']=from_mail_id
        gmail = smtplib.SMTP('smtp.gmail.com',587)
        gmail.ehlo()
        gmail.starttls()
        gmail.login(from_mail_id,from_mail_id_password)
        gmail.send_message(msg)
    except Exception as error:
        print("Exception occur due to {0}".format(str(error)))
        sys.exit(1)

    

# COMMAND ----------

def cols_upper(df,cols):
    try:
        for col_name in cols:
            df = df.withColumn(col_name, upper(col(col_name).cast("string")))
        return df
    except Exception as error:
        print("Exception occur due to {0}".format(str(error)))
        sys.exit(1)

# COMMAND ----------

def replace_information(replace_information_dict,df,flag):
    try:
        if flag == "season":
                for kk_key, value in replace_information_dict.items():
                            df = df.withColumn(
                                flag,
                                when(col(flag) == kk_key, value).otherwise(col(flag))
                            )
        elif flag == "series_name":
                for old_value, new_values in replace_information_dict.items():
                    for new_value in new_values:
                            df = df.withColumn(
                                flag,
                                when(col(flag) == old_value, new_value).otherwise(col(flag))
                            )
        return df
    except Exception as error:
        print("Exception as error due to {0}".format(str(error)))
        sys.exit(1)
