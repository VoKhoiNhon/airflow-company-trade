FROM apache/airflow:2.10.3 as airflow_custom
USER root
COPY ./requirements.txt /opt/airflow/requirements.txt
RUN sudo apt update -y \
	&& sudo apt install -y python3-pip \
	&& sudo pip install -r /opt/airflow/requirements.txt
