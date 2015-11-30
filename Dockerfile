FROM java:8
MAINTAINER Alexey Kudinkin

#RUN echo "deb http://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
#RUN sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 642AC823
#RUN sudo apt-get update
#RUN sudo apt-get install -y --no-install-recommends sbt
#
#EXPOSE  8080
#EXPOSE  8081
#
#CMD ["sbt", "run"]

ENV SCALA_VERSION 2.11.7
ENV SBT_VERSION 0.13.9

ENV WORKDIR /opt/docker

RUN mkdir -p $WORKDIR/zax

# Install Scala
RUN \
  cd $WORKDIR && \
  curl -o scala-$SCALA_VERSION.tgz http://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz && \
  tar -xf scala-$SCALA_VERSION.tgz && \
  rm scala-$SCALA_VERSION.tgz && \
  touch $WORKDIR/.bashrc && \
  echo 'export PATH=$PATH:$WORKDIR/scala-$SCALA_VERSION/bin:$PATH' >> $WORKDIR/.bashrc

# Install sbt
RUN \
  curl -L -o sbt-$SBT_VERSION.deb https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt

# Define working directory
WORKDIR /opt/docker/zax

COPY build.sbt  .
COPY lib        ./lib/
COPY src        ./src/

RUN sbt compile