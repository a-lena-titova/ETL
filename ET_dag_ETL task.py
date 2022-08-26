# Импортируем всё то, что нам нужно для создания DAG-а и его «содержания»
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import pandahouse
import pandas as pd

# Задаем параметры для запроса данных из CH
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'simulator_20220620',
                      'user':'student', 
                      'password':'dpo_python_2020'
                     }

# Задаем параменты для доступа к тестовой базе
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student-rw', 
                      'password':'656e2b0c9c'
                     }

# Задаем дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e.titova-8',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 5, 7),
}

# Задаем интервал запуска DAG
schedule_interval = '0 10 * * *'

# Пишем DAG cо всеми нужными тасками 
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_titova8_v4():
    
    # Выгружаем нужные данные из feed_actions
    @task()
    def extract_feed():
    
        query = """SELECT user_id,
                    toDate(time) AS event_date,
                    multiIf(gender=='1', 'male', 'female') as gender,
                    multiIf(age < 18, '0 - 17', age >= 18 and age < 30, '18-29', age >= 30 and age < 46, '30-45', age >= 46 and age < 60, '46-59', '60+') as age_group,
                    os,
                    countIf(action='view') AS views,
                    countIf(action='like') AS likes

                FROM {db}.feed_actions
                where toDate(time) = yesterday()
                GROUP BY user_id, event_date, gender, os, age_group"""
        df_feed = pandahouse.read_clickhouse(query, connection=connection)
        return df_feed
    
    # Выгружаем нужные данные из message_actions
    @task()
    def extract_message():
        
        query = """SELECT user_id,
                    event_date,
                    gender,
                    age_group,
                    os,
                    messages_sent,
                    messages_received,
                    users_sent,
                    users_received

                    FROM

                    (SELECT user_id,
                    toDate(time) AS event_date,
                    multiIf(gender=='1', 'male', 'female') as gender,
                    multiIf(age < 18, '0 - 17', age >= 18 and age < 30, '18-29', age >= 30 and age < 46, '30-45', age >= 46 and age < 60, '46-59', '60+') as age_group,
                    os,
                    count(user_id) AS messages_sent,
                    uniq(reciever_id) AS users_sent

                    FROM {db}.message_actions
                    where toDate(time) = yesterday()
                    GROUP BY user_id, event_date, gender, os, age_group) as t1 

                    JOIN

                    (SELECT reciever_id as user_id,
                            count(user_id) as messages_received,
                            uniq(user_id) as users_received
                    FROM {db}.message_actions
                    where toDate(time) = yesterday()
                    GROUP BY user_id) as t2

                    ON t1.user_id = t2.user_id"""
        df_message = pandahouse.read_clickhouse(query, connection=connection)
        return df_message
     
    # Объединяем выгрузки в одну
    @task()
    def tranform_union(df_feed, df_message):
        df_union = df_feed.merge(df_message, how='outer', on=['user_id', 'event_date', 'gender', 'age_group', 'os'])
        return df_union

    # Считаем лайки, посланные и полученные сообщения в разрезе пола пользователя
    @task()
    def transfrom_gender(df_union):
        df_union_gender = df_union.copy()
        df_union_gender['metrics'] = 'gender'
        df_union_gender = df_union_gender[['event_date', 'metrics','gender', 'views', 'likes', 'messages_sent', 'messages_received', 'users_sent', 'users_received']]\
            .groupby(['event_date', 'metrics', 'gender'])\
            .sum()\
            .reset_index()\
            .rename(columns = {'gender':'submetrics'})
        return df_union_gender
    
    # Считаем лайки, посланные и полученные сообщения в разрезе возраста пользователя
    @task()
    def transfrom_age(df_union):
        df_union_age = df_union.copy()
        df_union_age['metrics'] = 'age'
        df_union_age = df_union_age[['event_date', 'metrics','age_group', 'views', 'likes', 'messages_sent', 'messages_received', 'users_sent', 'users_received']]\
            .groupby(['event_date', 'metrics', 'age_group'])\
            .sum()\
            .reset_index()\
            .rename(columns = {'age_group':'submetrics'})
        return df_union_age
    
     # Считаем лайки, посланные и полученные сообщения в разрезе os пользователя
    @task()
    def transfrom_os(df_union):
        df_union_os = df_union.copy()
        df_union_os['metrics'] = 'os'
        df_union_os = df_union_os[['event_date', 'metrics','os', 'views', 'likes', 'messages_sent', 'messages_received', 'users_sent', 'users_received']]\
            .groupby(['event_date', 'metrics', 'os'])\
            .sum()\
            .reset_index()\
            .rename(columns = {'os':'submetrics'})
        return df_union_os
    
    # Объединяем вычисления в одну таблицу
    @task()
    def transform_union_fin(df_union_gender, df_union_age, df_union_os):
        df_union_fin = pd.concat([df_union_gender, df_union_age, df_union_os]).reset_index(drop=True)
        return df_union_fin
    
    # финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse
    @task
    def load(df_union_fin):
        pandahouse.to_clickhouse(df_union_fin, 'etitova_homework6_fin', index=False, connection=connection_test)
    
    # Инициализируем таски:
    df_feed = extract_feed()
    df_message = extract_message()
    
    df_union = tranform_union(df_feed, df_message)
    
    df_union_gender = transfrom_gender(df_union)
    df_union_age = transfrom_age(df_union)
    df_union_os = transfrom_os(df_union)
    
    df_union_fin = transform_union_fin(df_union_gender, df_union_age, df_union_os)
    
    load(df_union_fin)
    
dag_titova8_v4 = dag_titova8_v4()
