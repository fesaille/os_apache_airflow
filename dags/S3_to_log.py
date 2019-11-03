# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""

from datetime import datetime, timedelta

import airflow
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.hooks.S3_hook import S3Hook

from os import environ


PythonOperator.ui_color = "#4584b6"
PythonOperator.ui_fgcolor = "#ffffff"



BashOperator.ui_color = "#000000"
BashOperator.ui_fgcolor = "#ffffff"



# From https://stackoverflow.com/questions/56626258
def list_connections(**context):

    from airflow import settings
    from airflow.models import Connection

    session = settings.Session() # get the session

    if not 'minio' in [c.conn_id for c in session.query(Connection).all()]:
        minio = Connection(conn_id='minio',
                           conn_type='s3',
                           login=environ['MINIO_ACCESS_KEY'],
                           password=environ['MINIO_SECRET_KEY'],
                           extra='{"host": "http://minio:9000"}')

        #create a connection object
        session.add(minio)
        session.commit() # it will insert the connection object programmatically.



    # if not 'mongo_atlas' in [c.conn_id for c in session.query(Connection).all()]:
    #     atlas = Connection(conn_id='mongo_atlas',
    #                        conn_type='mongo',
    #                        login='m220student',
    #                        password='m220password',
    #                        port=27017,
    #                        host='baobab-crx55.mongodb.net')
    #     session.add(atlas)
    #     session.commit() # it will insert the connection object programmatically.

    if not 'mongo_local' in [c.conn_id for c in session.query(Connection).all()]:
        atlas = Connection(conn_id='mongo_local',
                           conn_type='mongo',
                           login=environ['MONGO_INITDB_ROOT_USERNAME'],
                           password=environ['MONGO_INITDB_ROOT_PASSWORD'],
                           port=27017,
                           host='mongo',
                           schema=environ['MONGO_INITDB_DATABASE'])
        session.add(atlas)
        session.commit() # it will insert the connection object programmatically.



# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'execution_timeout': timedelta(seconds=10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}

dag = DAG(
    '_S3_to_log',
    default_args=default_args,
    description='Test grabing data from S3',
    schedule_interval=timedelta(days=1),
)

test_minio_connection = PythonOperator(
    task_id='test_minio_connection',
    python_callable=list_connections,
    provide_context=True,
    dag=dag
)

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)


t1.doc_md = """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
"""

dag.doc_md = __doc__

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 1',
    dag=dag,
)

templated_command = """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
    dag=dag,
)


from airflow.models import BaseOperator
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.utils.decorators import apply_defaults

class MongoListDatabasesOperator(BaseOperator):
    ui_color = '#589636'

    @apply_defaults
    def __init__(self, conn_id, *args, **kwargs):
       super().__init__(*args, **kwargs)
       self.conn_id = conn_id

    def execute(self, context):
        mongo = MongoHook(conn_id=self.conn_id, )
        mongo.uri, dbname = mongo.uri.rsplit("/", maxsplit=1)
        # conn = mongo.get_conn()

        # return conn.list_database_names()

        posts = mongo.get_collection("posts", dbname)

        import datetime
        post = {"author": "Mike",
            "text": "My first blog post!",
            "tags": ["mongodb", "python", "pymongo"],
            "date": datetime.datetime.utcnow()}
        # posts = db.posts
        post_id = posts.insert_one(post).inserted_id
        # collection = mongo.get_collection('people', mongo_db='starwars')
        # res = collection.find_one()
        # return str(res['_id'])
        return str(post_id)



class S3ListOrCreateOperator(BaseOperator):
    template_fields = ('bucket', 'prefix', 'delimiter')  # type: Iterable[str]
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix='',
                 delimiter='',
                 aws_conn_id='aws_default',
                 verify=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.aws_conn_id = aws_conn_id
        self.verify = verify

    def execute(self, context):
        hook = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)

        self.log.info(
            'Getting the list of files from bucket: %s in prefix: %s (Delimiter {%s)',
            self.bucket, self.prefix, self.delimiter
        )

        try:
            docs = hook.list_keys(
                bucket_name=self.bucket,
                prefix=self.prefix,
                delimiter=self.delimiter)
        except Exception:
            hook.create_bucket(bucket_name=self.bucket)
            return


s3_file = S3ListOrCreateOperator(
    task_id='list_3s_files',
    bucket='cat',
    delimiter='/',
    aws_conn_id='minio',
    dag=dag
)


# aggregate_query = [
#     { "$match": {
#         "stars": { $gt: 2 } } },   { $sort: { stars: 1 } },   { $group: { _id: "$cuisine", count: { $sum: 1 } } } ]

mongo_db_test = MongoListDatabasesOperator(
    task_id='list_mongo_files',
    conn_id='mongo_local',
    dag=dag
)

test_minio_connection >> s3_file >> mongo_db_test >> t1 >> t2 >> t3
