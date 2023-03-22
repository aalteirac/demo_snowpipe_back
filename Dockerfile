FROM adoptopenjdk:8-jre-hotspot

COPY ./demo_snowpipe.jar /app/demo_snowpipe.jar
COPY ./param_snow.json /app/param_snow.json

WORKDIR /app

ENTRYPOINT ["java", "-jar", "demo_snowpipe.jar"]