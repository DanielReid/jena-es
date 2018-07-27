FROM phusion/baseimage
RUN sed -i 's/main$/main universe/' /etc/apt/sources.list
RUN add-apt-repository ppa:openjdk-r/ppa
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get upgrade -y
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y -q openjdk-8-jre

ENV LC_ALL en_US.UTF-8

RUN mkdir /DB

EXPOSE 3030
ADD JenaEventSourcing-0.0.1-SNAPSHOT.jar JenaEventSourcing-0.0.1-SNAPSHOT.jar
CMD ["java", "-jar", "JenaEventSourcing-0.0.1-SNAPSHOT.jar"]
