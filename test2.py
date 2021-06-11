

# pylint: disable=missing-function-docstring

# [START tutorial]
# [START import_module]
import json
import feedparser
from datetime import datetime
from datetime import timedelta
import time
import re
import ssl
from urllib import parse
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from pymongo import MongoClient
from airflow.models.connection import Connection

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'values': {'name': 'Michael Foord',
               'location': 'Northampton',
               'language': 'Python' },
    'data': parse.urlencode({'name': 'Michael Foord', 'location': 'Northampton',
                                    'language': 'Python'}).encode('ascii'),
    'headers': {'User-Agent': 'Mozilla/5.0'}

}
# [END default_args]


# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval='*/60 * * * *', start_date=datetime.now() - timedelta(days=1), tags=['RJK','NEWS','NEWSIS','RSS'])
def SC_NEWS_NEWSIS_20210427():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]

    # [START extract]
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """

        url_dict = {'속보': 'https://newsis.com/RSS/sokbo.xml',
                    '전국': 'https://newsis.com/RSS/country.xml',
                    '정치': 'https://newsis.com/RSS/politics.xml',
                    '경제': 'https://newsis.com/RSS/economy.xml',
                    '사회': 'https://newsis.com/RSS/society.xml',
                    '광장': 'https://newsis.com/RSS/square.xml',
                    '세계': 'https://newsis.com/RSS/international.xml',
                    '산업': 'https://newsis.com/RSS/industry.xml',
                    '스포츠': 'https://newsis.com/RSS/sports.xml',
                    '문화': 'https://newsis.com/RSS/culture.xml',
                    '연예': 'https://newsis.com/RSS/entertain.xml',
                    '포토': 'https://newsis.com/RSS/photo.xml',
                    '위클리뉴시스': 'https://newsis.com/RSS/newsiseyes.xml'}

        emailReg = re.compile('[a-zA-Z0-9_-]+@[a-z]+.[a-z]+')
        bodyReg = re.compile('(<p)[\S\s]+(p>)')
        order_data_dict = {}
        ct = 0
        for i in url_dict.items():
            if hasattr(ssl, '_create_unverified_context'):
                ssl._create_default_https_context = ssl._create_unverified_context
            d = feedparser.parse(i[1])
            for p in d.entries:
                temp_dict = {}
                temp_dict['title'] = p.title.replace('\"', '').replace('&quot', '').replace('<br />', '').replace(
                    '&#039', ' ').replace('\"', '').replace("\'", '').replace(';', '').replace('&amp;', '&').strip()
                temp_dict['company'] = '뉴시스'
                temp_dict['reporter'] = p.author.replace('기자', '').strip()

                is_email = emailReg.findall(p.summary.replace('(', '').replace(')', ''))
                if is_email:
                    temp_dict['reporter_mail'] = is_email[-1]
                else:
                    temp_dict['reporter_mail'] = ''
                temp_dict['portal'] = 'newsis'
                temp_dict['category'] = i[0]
                temp_dict['url'] = p.link
                temp_dict['date'] = time.mktime(
                    datetime.strptime(p.published.split('+')[0].strip(), '%a, %d %b %Y %H:%M:%S').timetuple())
                temp_dict['body'] = bodyReg.sub('', p.summary).replace('\n', '').replace('&#039', ' ').replace('&quot',
                                                                                                               '').replace(
                    '<br />', '').replace('\"', '').replace("\'", '').replace(';', '').replace('&amp;', '&').strip()

                order_data_dict[p.link] = temp_dict
                ct+= 1
        return order_data_dict
    # [END extract]

    # [START transform]
    @task()
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """

        print("Total number of news:", len(order_data_dict))

        return order_data_dict

    # [END transform]

    # [START load]
    @task()
    def load(order_data_dict: dict):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """
        db_connections = Connection.get_connection_from_secrets(conn_id="crawling_poc")

        connection = 'mongodb' + Connection.get_uri(db_connections)[5:]

        mongo_conn = MongoClient(connection)

        # schema name
        my_database = mongo_conn['news']

        # table name
        my_collections = my_database['newsis']

        for i in order_data_dict.items():
            # i[0] 은 url
            query = {"_id": i[0]}
            # i[1] 은 한개의 뉴스기사
            values = i[1]
            temp_time = i[1]['date']
            temp_time = datetime.fromtimestamp(temp_time)
            values['date'] = temp_time
            # 중복입력시 에러발생하기 때문에 try문으로 insert
            try:
                my_collections.insert_one(dict(query, **values))
            except Exception as e:
                print(e)
                pass

    # [END load]

    # [START main_flow]
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary)
    # [END main_flow]


# [START dag_invocation]
SC_NEWS_NEWSIS = SC_NEWS_NEWSIS_20210427()
# [END dag_invocation]

# [END tutorial]

