From centos:7

MAINTAINER francis francis@francisdu.com

USER root

# Install dependency 
RUN yum update -y && yum install -y \
    wget openssh vim \
    openssh-clients java-1.8.0-openjdk \
    java-1.8.0-openjdk-devel  java-1.8.0-openjdk-headless

# Download QlickSQL package
RUN  wget https://github.com/Qihoo360/Quicksql/releases/download/release-0.6/qsql-0.6.tar.gz &&  \
     tar -xzvf qsql-0.6.tar.gz -C /usr/local

# Download Spark package
RUN wget http://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-2.3.3/spark-2.3.3-bin-hadoop2.7.tgz &&  \
    tar -xzvf spark-2.3.3-bin-hadoop2.7.tgz -C /usr/local

# Setting environments
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.212.b04-0.el7_6.x86_64
ENV QSQL_HOME /usr/local/qsql-0.6
ENV SPARK_HOME /usr/local/spark-2.3.3-bin-hadoop2.7
ENV PATH $JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$QSQL_HOME/bin:$PATH
RUN export PATH QSQL_HOME SPARK_HOME

WORKDIR /usr/local/qsql-0.6

EXPOSE 4040 8080

CMD ["/usr/sbin/init"]