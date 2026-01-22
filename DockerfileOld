#FROM registry.fedoraproject.org/fedora:40
FROM fedora:latest

RUN dnf update -y && dnf install -y python3 python3-pip gcc make curl bash && dnf clean all

RUN useradd --create-home cdeuser
WORKDIR /home/cdeuser

RUN mkdir /home/cdeuser/.cde /home/cdeuser/.cdp

RUN pip3 install --no-cache-dir jupyter \
  jupyterlab
RUN pip3 install numpy \
  pandas \
  matplotlib \
  scikit-learn

# Copy files and directories with ownership set to cdeuser
COPY --chown=cdeuser:cdeuser img /home/cdeuser/img
COPY --chown=cdeuser:cdeuser de-pipeline-manufacturing /home/cdeuser/de-pipeline-manufacturing
COPY --chown=cdeuser:cdeuser de-pipeline-bank /home/cdeuser/de-pipeline-bank
COPY --chown=cdeuser:cdeuser setup /home/cdeuser/setup
COPY --chown=cdeuser:cdeuser config.yaml /home/cdeuser/.cde/config.yaml
COPY --chown=cdeuser:cdeuser credentials /home/cdeuser/.cdp/credentials
COPY --chown=cdeuser:cdeuser cde /usr/bin/cde
COPY --chown=cdeuser:cdeuser cdeconnect.tar.gz /home/cdeuser/cdeconnect.tar.gz
COPY --chown=cdeuser:cdeuser pyspark-3.5.1.tar.gz /home/cdeuser/pyspark-3.5.1.tar.gz
COPY --chown=cdeuser:cdeuser Iceberg_TimeTravel_PySpark_banking.ipynb /home/cdeuser/Iceberg_TimeTravel_PySpark_banking.ipynb
COPY --chown=cdeuser:cdeuser Iceberg_TimeTravel_PySpark_manufacturing.ipynb /home/cdeuser/Iceberg_TimeTravel_PySpark_manufacturing.ipynb
COPY --chown=cdeuser:cdeuser pyspark-app.py /home/cdeuser/pyspark-app.py

USER root
RUN chown -R cdeuser:cdeuser /home/cdeuser &&\
    chmod -R u+rw /home/cdeuser
USER cdeuser

EXPOSE 8888

CMD ["bash", "-c", "jupyter-lab --ip='0.0.0.0' --port=8888 --no-browser"]

#CMD ["python3", "-m", "jupyterlab", "--ip='0.0.0.0'", "--port=8888", "--allow-root"]

#ADD img /home/cdeuser/img
#ADD de-pipeline-manufacturing /home/cdeuser/de-pipeline-manufacturing
#ADD de-pipeline-bank /home/cdeuser/de-pipeline-bank
#ADD setup /home/cdeuser/setup
#ADD config.yaml /home/cdeuser/.cde/config.yaml
#ADD credentials /home/cdeuser/.cdp/credentials
#ADD cde /usr/bin/cde
#COPY cdeconnect.tar.gz /home/cdeuser/cdeconnect.tar.gz
#COPY pyspark-3.5.1.tar.gz /home/cdeuser/pyspark-3.5.1.tar.gz
#ADD Iceberg_TimeTravel_PySpark_banking.ipynb /home/cdeuser/Iceberg_TimeTravel_PySpark_banking.ipynb
#ADD Iceberg_TimeTravel_PySpark_manufacturing.ipynb /home/cdeuser/Iceberg_TimeTravel_PySpark_manufacturing.ipynb
#ADD pyspark-app.py /home/cdeuser/pyspark-app.py

#RUN chmod a+rw /home/cdeuser/Iceberg_TimeTravel_PySpark_banking.ipynb
#RUN chmod a+rw /home/cdeuser/Iceberg_TimeTravel_PySpark_manufacturing.ipynb

#RUN chmod 777 /home/cdeuser/.cde
#RUN chmod 777 /home/cdeuser/.cdp
#RUN chmod 777 /home/cdeuser/
