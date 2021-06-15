from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.utils.dates import days_ago
from datetime import datetime
import time
from urllib import parse
import requests
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
from airflow.models.connection import Connection

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


@dag(default_args=default_args, schedule_interval=None, start_date=datetime(2021, 6, 14, 1, 0), params={'basedate': 'Do_not_use'})

def SC_NEWS_ALL_DAUM_20210430():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple ETL data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/stable/tutorial_taskflow_api.html)
    """


    @task()
    def extract():
        """
         #### Extract task
         A simple Extract task to get data ready for the rest of the data
         pipeline. In this case, getting data is simulated by reading from a
         hardcoded JSON string.
         """

        categories = ['politics', 'economic', 'society', 'culture', 'digital']
        news_dict = {}

        url = 'http://media.daum.net/breakingnews/{}'

        context = get_current_context()
        dag_run = context["dag_run"]
        # dagrun_conf = dag_run.conf
        # date_test = dagrun_conf['basedate']

        date_test = dag_run.execution_date.strftime('%Y%m%d')
        is_digit = '[^\d\s]'
        emailReg = re.compile('[a-zA-Z0-9_-]+@[a-z]+.[a-z]+.[a-z]+')

        ct = 0
        for category in categories:
            page_number = 1
            max_page_number = 1

            while page_number <= max_page_number:
                target_url = url.format(category) + f'?page={page_number}' + f'&regDate={date_test}'
                search_url = requests.get(target_url, headers={'User-Agent': 'Mozilla/5.0'})
                soup = BeautifulSoup(search_url.content, 'html.parser')
                links = soup.find('ul', {'class': 'list_news2 list_allnews'}).find_all('li')

                if page_number % 10 == 1:
                    print(category, ':', page_number)
                    test = [int(x) for x in re.sub(is_digit, '', (
                        re.sub('\s+', ' ', soup.find('div', {'class': 'paging_news'}).get_text()))).split(' ') if x]
                    max_page_number = max(test)

                for link in links:
                    news_url = link.find('a').attrs['href']
                    news_company = link.select_one('span', {'class': 'info_news'}).get_text().split('·')[0].strip()
                    news_link = requests.get(news_url, headers={'User-Agent': 'Mozilla/5.0'})
                    news_html = BeautifulSoup(news_link.content, 'html.parser')
                    try:
                        idx = news_url.split('/')[-1]
                        title = news_html.find('h3', {'class': 'tit_view'}).get_text().replace('\\"', '').replace('"',
                                                                                                                  '')
                        date = news_html.find('span', {'class': 'num_date'}).get_text()
                        temp_text = news_html.find('div', {'class': 'article_view'}).get_text().replace('\n',
                                                                                                        '').replace(
                            '"', '')
                        image = ''
                        if news_html.find('p', {'class': "link_figure"}):
                            image = news_html.find('p', {'class': "link_figure"}).img['src'].split('=')[-1]
                        else:
                            image = 'video'
                        reporter = news_html.find('span', {'class': 'txt_info'}).get_text()
                        name = re.findall('[가-힣]{3,}', reporter)  # [], ['홍길동']
                        if name:
                            reporter = name[0]
                        else:
                            reporter = ''

                        article = re.sub('\s+', ' ', temp_text)
                        is_email = emailReg.findall(article.replace('(', '').replace(')', ''))
                        reporter_email = ''
                        if is_email:
                            reporter_email = is_email[-1]
                        else:
                            reporter_email = ''
                        news_dict[news_url] = {
                            'title': title,
                            'category': category,
                            'repoter': reporter,
                            'repoter_email': reporter_email,
                            'portal': 'daum',
                            'date': time.mktime(datetime.strptime(date, '%Y. %m. %d. %H:%M').timetuple()),
                            'url': news_url,
                            'image': image,
                            'company': news_company,
                            'body': article
                        }
                        ct += 1

                    except Exception as e:
                        print(e)
                        pass
                page_number += 1
                print('page_number:', page_number)

        return news_dict
    # [END extract]

    # [START transform]

    @task()
    def transform(news_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        print(len(news_dict.items()))

        transformed_news_dict = news_dict

        return transformed_news_dict

    @task()
    def load(transformed_news_dict: dict):
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
        my_collections = my_database['daum_category']

        for i in transformed_news_dict.items():
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

    a1 = extract()
    a2 = transform(a1)
    load(a2)

SC_NEWS_ALL_DAUM =SC_NEWS_ALL_DAUM_20210430()

