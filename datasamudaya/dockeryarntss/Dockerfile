FROM amd64/openjdk:16-jdk-oracle
ADD "openjdk-15.0.1_linux-x64_bin.tar.gz" /usr/local/java
RUN echo export JAVA_HOME=/usr/local/java/jdk-15.0.1 >> ~/.bashrc
RUN echo export PATH=/usr/local/java/jdk-15.0.1/bin:$PATH >> ~/.bashrc
ADD "MassiveDataCruncher-1.0.0-64bit.tar.gz" /opt/datasamudaya
RUN mkdir -p /opt/datasamudaya/appjar
RUN chmod 777 /opt/datasamudaya/appjar
COPY datasamudayatss.properties /opt/datasamudaya/config/datasamudaya.properties
COPY taskschedulerstream.sh /opt/datasamudaya/bin/taskschedulerstream.sh
RUN cd /opt/datasamudaya/bin
WORKDIR /opt/datasamudaya/bin
ENTRYPOINT ["sh","taskschedulerstream.sh"]