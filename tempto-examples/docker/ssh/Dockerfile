FROM ghcr.io/trinodb/testing/almalinux9-oj17:126

RUN dnf install -y openssh-server openssh-clients passwd && dnf clean all
RUN echo 'root:s3cr37_p@55' | passwd --stdin root

RUN mkdir /var/run/sshd && mkdir /root/.ssh

RUN ssh-keygen -t ecdsa -f /etc/ssh/ssh_host_ecdsa_key -N ''

RUN update-crypto-policies --set DEFAULT:SHA1

RUN printf 'PubkeyAcceptedAlgorithms +ssh-rsa\n' > /etc/ssh/sshd_config.d/00-tempto.conf

COPY keys/key.pub /root/.ssh/authorized_keys

RUN chmod 400 /root/.ssh/authorized_keys

EXPOSE 22

CMD /usr/sbin/sshd -D