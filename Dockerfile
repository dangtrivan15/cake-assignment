FROM apache/airflow:2.9.0-python3.10
# This is the newest Python 3.10 version at the time of doing this assignment.

RUN mkdir /tmp/cake-assignment

COPY setup.py /tmp/cake-assignment/setup.py

COPY cake_airflow_custom_package /tmp/cake-assignment/cake_airflow_custom_package

RUN pip install /tmp/cake-assignment

