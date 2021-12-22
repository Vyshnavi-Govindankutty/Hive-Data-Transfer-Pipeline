FROM apache/airflow:2.1.3


USER root


RUN apt-get update && \
apt-get install -y software-properties-common && \
apt-get install -y wget && \
apt install -y default-jdk


ARG SPARK_VERSION="3.1.2"
ARG HADOOP_VERSION="3.2"
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
RUN export JAVA_HOME
ENV PATH $PATH:$JAVA_HOME/bin
ENV SPARK_HOME /usr/local/spark


RUN cd "/tmp" && \
        wget --no-verbose "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        tar -xvzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
        mkdir -p "${SPARK_HOME}/bin" && \
        mkdir -p "${SPARK_HOME}/assembly/target/scala-2.12/jars" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/bin/." "${SPARK_HOME}/bin/" && \
        cp -a "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}/jars/." "${SPARK_HOME}/assembly/target/scala-2.12/jars/" && \
        rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

RUN cd "/usr/lib" && \
	mkdir -p "hadoop" && \
	cd "hadoop" && \
	wget "https://downloads.apache.org/hadoop/common/hadoop-3.2.2/hadoop-3.2.2.tar.gz" && \
	tar xzf "hadoop-3.2.2.tar.gz"
	
ENV HADOOP_HOME /usr/lib/hadoop/hadoop-3.2.2
ENV HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop
	
RUN export SPARK_HOME
ENV PATH=$PATH:${SPARK_HOME}/bin




USER airflow
COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt 
COPY ./dags/crypto_airflow_dag.py /opt/airflow/dags


