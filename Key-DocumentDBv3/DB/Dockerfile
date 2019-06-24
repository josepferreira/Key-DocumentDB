FROM java:8
# WORKDIR /
# ADD SLAVEbd-serverless-1.0-SNAPSHOT.jar SLAVEbd-serverless-1.0-SNAPSHOT.jar
# COPY ./testeslave.sh /
# RUN chmod +x /testeslave.sh
# ENTRYPOINT /testeslave.sh
# CMD []

COPY . /slave
ENTRYPOINT ["java", "-jar", "/slave/SLAVEbd-serverless-1.0-SNAPSHOT.jar"]