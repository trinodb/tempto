FROM ghcr.io/trinodb/testing/centos7-oj11:53

# TODO: Remove when updating to newer base image
# replace mirrorlist.centos.org and mirror.centos.org with vault.centos.org
RUN sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*
RUN sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*

RUN yum install -y openssh-server openssh-clients passwd
RUN echo s3cr37_p@55 | passwd --stdin root

RUN mkdir /var/run/sshd && mkdir /root/.ssh

RUN ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -N '' 

COPY keys/key.pub /root/.ssh/authorized_keys

RUN chmod 400 /root/.ssh/authorized_keys

EXPOSE 22

CMD /usr/sbin/sshd -D
