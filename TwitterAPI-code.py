import psycopg2
import tweepy 
import csv
import pandas as pd
import datetime

#提取API驗證
API_KEY = ""
API_SECRET = ""
BEARER_TOKEN = ""
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""

#登入API
auth = tweepy.OAuthHandler(API_KEY,API_SECRET)
auth.set_access_token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)
search_key ="UserInfo"


try:
    UserName = input('Enter The Search Name:')
    
    #生成並打開一個新CSV ---(用戶資料)
    csvFile = open(search_key+".csv","a+",newline="",encoding="utf-8")
    csvWriter = csv.writer(csvFile)
    
    
    NameID=api.user_timeline(screen_name=UserName)
    for UesrID in NameID:
        User_ID=(UesrID.user.id)
    #API提取 UserName CreatTime Location Description
    statuses = api.lookup_users(user_id=[User_ID])
    for tweet in statuses:
        tweets = [tweet.id,tweet.name,tweet.created_at,tweet.location,tweet.description]
    
    #把資料 寫入CSV
        csvWriter.writerow(tweets)
    
    #======================================================================================================================================
    
    search_key ="NetWorkInfo"
    
    #生成並打開一個新CSV ---- (社交信息)
    csvFile = open(search_key+".csv","a+",newline="",encoding="utf-8")
    csvWriter = csv.writer(csvFile)
    
    #API提取 Followers Following
    statuses = api.lookup_users(user_id=[User_ID])
    for tweet in statuses:
    #    print(tweet)
        tweets = [tweet.id,tweet.followers_count,tweet.friends_count]
        tweets.append(str(datetime.datetime.today()))
    #把資料 寫入CSV
        csvWriter.writerow(tweets)
    
    #======================================================================================================================================
    search_key ="covid_19"
    
    #生成並打開一個新CSV ---- (Covid-19)
    csvFile = open(search_key+".csv","a+",newline="",encoding="utf-8")
    csvWriter = csv.writer(csvFile)
    
    #提取 所有 JOE的推文 和 推文時間
    for tweet in tweepy.Cursor(api.user_timeline,screen_name=UserName,count=8000).items():
        tweets = [tweet.text.encode("utf-8"),tweet.created_at,tweet.user.id]
        
        csvWriter.writerow(tweets)
    #========================================================
    #把Covid-19.CSV 讀取成PD 
    df = pd.read_csv('covid_19.csv')
    #加入標題
    df.columns =['text','time','tweetid']
    #把TEXT的英文字 全部轉成細階英文    方便之後的SQL Search
    df['text'] = df['text'].str.lower()
    
    
    #登入PostgreSQL 自己的用戶資料
    hostname = 'localhost'
    database = 'demo'
    username = 'postgres'
    pwd = '10248tiger'
    port_id = 5432
    conn = None
    cur = None
    
    
    #連接至SQL
    try:
        conn = psycopg2.connect(
            host = hostname,
            dbname = database,
            user = username,
            password = pwd,
            port = port_id
        )
    
        cur = conn.cursor()
    
    #刪除重覆的表單 (covid_19)
        cur.execute('DROP TABLE IF EXISTS covid_19')
    #刪除重覆的表單-----(networkinfo)
        cur.execute('DROP TABLE IF EXISTS networkinfo')
    #刪除重覆的表單----(userinfo)
        cur.execute('DROP TABLE IF EXISTS userinfo')
        
    #生成表單(userinfo)
        create_script = '''CREATE TABLE IF NOT EXISTS userinfo(
            tweetid int NOT NULL PRIMARY KEY,    
            tweetname varchar(50),
            createdate varchar(50),
            location varchar(100),
            description varchar(300)
            )'''
        cur.execute(create_script)
        
    #打開CSV(UserInfo)
        my_file = open('UserInfo.csv')
        print('file opened in memory')
        
    #把整個CSV 上傳到DB
        SQL_STATEMENT = """ 
        COPY userinfo FROM STDIN WITH
        CSV
        DELIMITER AS ','
        """
        cur.copy_expert(sql=SQL_STATEMENT,file=my_file)
        print('file copied to db')
    
        conn.commit()
    
        print('import UserInfo to db completed')
        
    #===========================================================
    
    #生成表單------(networkinfo)
        create_script = '''CREATE TABLE IF NOT EXISTS networkinfo(
            tweetid int REFERENCES userinfo(tweetid),
            followers int,
            following int,
            getdate varchar(50))'''
        cur.execute(create_script)
    #打開CSV-----(NetWorkinfo)
        my_file = open('NetWorkinfo.csv')
        print('file open in memory')
    #把整個CSV 上傳到DB
        SQL_STATEMENT = """ 
        COPY NetWorkinfo FROM STDIN WITH
        CSV
        DELIMITER AS ','
        """
        cur.copy_expert(sql=SQL_STATEMENT,file=my_file)
        print('file copied to db')
    
        conn.commit()
     
        print('import to NetWorkinfo db completed')
        
    #=======================================================================================
    
    
    # Create Table --- (covid-19)
        cur.execute('''
    		CREATE TABLE IF NOT EXISTS covid_19 (
                text varchar(500),
                time varchar,
    			tweetid int REFERENCES userinfo(tweetid))
                   ''')
    
    # 把DF的資料 依照標題 一行一行加入SQL
        sql = "INSERT INTO covid_19 (text, time, tweetid) VALUES (%s, %s, %s)"
        for row in df.itertuples():
            val = (row.text, row.time, row.tweetid)
            cur.execute(sql, val)
         
        conn.commit()
        cur.close()
        conn.close()
        print('import covid_19 to db completed')
    
    
    #偵測錯誤信息
    except Exception as error:
        print(error)

except Exception as error:
    print(error)
          
else:
    print('upload completed')
    