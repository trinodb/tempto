FROM prestodb/centos6-oj8
MAINTAINER Presto community <https://prestosql.io/community.html>

RUN yum install -y tar

RUN curl -SL http://search.maven.org/remotecontent?filepath=io/prestosql/presto-server/302/presto-server-302.tar.gz \
      | tar xz \
      && mv $(find -type d -name 'presto-server*') presto-server

RUN mkdir /presto-server/etc 

COPY etc /presto-server/etc/

CMD /presto-server/bin/launcher run
