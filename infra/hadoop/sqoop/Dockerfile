FROM apache/hadoop:3
USER root
ADD https://archive.apache.org/dist/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz /tmp/sqoop.tgz
RUN tar -xzf /tmp/sqoop.tgz -C /opt \
  && mv /opt/sqoop-1.4.7.bin__hadoop-2.6.0 /opt/sqoop \
  && rm -f /tmp/sqoop.tgz
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar /opt/sqoop/lib/postgresql-jdbc.jar
# Sqoop 1.4.7 expects commons-lang 2.x (org.apache.commons.lang.StringUtils); not bundled with Hadoop 3 image.
ADD https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar /opt/sqoop/lib/commons-lang-2.6.jar
# apache/hadoop:3 ships JRE only; Sqoop codegen requires javac (JDK).
ADD https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u432-b06/OpenJDK8U-jdk_x64_linux_hotspot_8u432b06.tar.gz /tmp/jdk.tgz
RUN mkdir -p /usr/lib/jvm && tar -xzf /tmp/jdk.tgz -C /usr/lib/jvm && rm -f /tmp/jdk.tgz
ENV JAVA_HOME=/usr/lib/jvm/jdk8u432-b06
ENV SQOOP_HOME=/opt/sqoop
ENV PATH="${JAVA_HOME}/bin:${PATH}:/opt/sqoop/bin"
