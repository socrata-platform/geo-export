FROM socrata/java8-focal

RUN apt-get -y update && apt-get -y install jq

# Service specifif settings.
ENV SERVER_NAME geo-export

# Some paths/locations we care about
ENV SERVER_ROOT      /srv/${SERVER_NAME}/
ENV SERVER_CONFIG    ${SERVER_NAME}.conf
ENV LOGBACK_CONFIG   ${SERVER_NAME}-logback.xml
ENV SERVER_ARTIFACT  ${SERVER_NAME}-assembly.jar

# Environment Settings.
EXPOSE 7777
ENV JAVA_XMX 1024m
ENV MIN_THREADS 3
ENV MAX_THREADS 20
ENV CHUNK_SIZE 1000
ENV JMX_PORT 1099

# Log Settings.
ENV LOG_LEVEL info
ENV LOG_PATTERN %d %-5p [%t]  [%X{X-Socrata-RequestId}] [%X{X-Socrata-Resource}] %c{36} "%X{X-Socrata-Host}" %m%n

# Add other required resources to the container
ADD ship.d /etc/ship.d
Add ${SERVER_CONFIG}.j2 /etc/
Add ${LOGBACK_CONFIG}.j2 /etc/
ADD ${SERVER_ARTIFACT} ${SERVER_ROOT}

