from airflow import settings
from airflow.models import Connection

session = settings.Session() # get the session

if not 'minio' in [c.conn_id for c in session.query(Connection).all()]:
    minio = Connection(conn_id='minio',
                        conn_type='s3',
                        login='minio',
                        password='MWYyZDFlMmU2N2Rm',
                            extra='{"host": "http://minio-heaux.apps.ca-central-1.starter.openshift-online.com"}')

    #create a connection object