FROM ubuntu
MAINTAINER Alexey Kudinkin

COPY build.sbt  /
COPY lib        /lib/
COPY lib        /lib/
COPY src        /src/

EXPOSE  8080
EXPOSE  8081

RUN echo "deb http://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
RUN sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
RUN sudo apt-get update
RUN sudo apt-get install -y sbt

CMD ["sbt", "run"]
