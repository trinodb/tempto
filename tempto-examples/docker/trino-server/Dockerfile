FROM ghcr.io/trinodb/testing/centos7-oj11:53

# TODO: Remove when updating to newer base image
# replace mirrorlist.centos.org and mirror.centos.org with vault.centos.org
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*

RUN yum install -y tar

RUN curl -SL https://repo1.maven.org/maven2/io/trino/trino-server/356/trino-server-356.tar.gz \
      | tar xz \
      && mv $(find -type d -name 'trino-server*') trino-server

RUN mkdir /trino-server/etc

COPY etc /trino-server/etc/

CMD /trino-server/bin/launcher run
