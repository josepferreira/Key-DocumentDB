FROM java:8
# WORKDIR /
# ADD SLAVEbd-serverless-1.0-SNAPSHOT.jar SLAVEbd-serverless-1.0-SNAPSHOT.jar
# COPY ./testeslave.sh /
# RUN chmod +x /testeslave.sh
# ENTRYPOINT /testeslave.sh
# CMD []

COPY ./bd-serverless-1.0-SNAPSHOT.jar .
ENTRYPOINT ["java", "-cp", "bd-serverless-1.0-SNAPSHOT.jar", "nodes.Slave"]