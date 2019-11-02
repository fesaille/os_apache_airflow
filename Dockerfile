FROM puckel/docker-airflow

USER root

ARG AIRFLOW_DEPS="s3"

# ARG AIRFLOW_VERSION=1.10.6
RUN pip install -U apache-airflow[crypto,postgres,hive,jdbc,ssh${AIRFLOW_DEPS:+,}${AIRFLOW_DEPS}]


RUN pip install \
        pymongo \
        distributed \
	&& rm -rf \
        /tmp/* \
        /var/tmp/*

RUN pip uninstall -y psycopg2 && \
        pip install psycopg2-binary

COPY script/airflow.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh

USER airflow

# Enable airflow auto-completion
RUN register-python-argcomplete airflow >> ~/.bashrc
