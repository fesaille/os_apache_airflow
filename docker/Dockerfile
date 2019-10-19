FROM puckel/docker-airflow

USER root

RUN pip install 'apache-airflow[s3]' \
        'apache-airflow[kerberos]' \
        && rm -rf \
        /tmp/* \
        /var/tmp/*

USER airflow