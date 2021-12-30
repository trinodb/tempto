FROM ghcr.io/trinodb/testing/centos7-oj11:53

RUN yum install -y tar

RUN curl -SL https://repo1.maven.org/maven2/io/trino/trino-server/356/trino-server-356.tar.gz \
      | tar xz \
      && mv $(find -type d -name 'trino-server*') trino-server

RUN mkdir /trino-server/etc

COPY etc /trino-server/etc/

CMD /trino-server/bin/launcher run
