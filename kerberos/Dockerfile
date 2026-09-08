FROM almalinux:9

RUN dnf install -y krb5-server krb5-workstation krb5-libs && dnf clean all

COPY krb5.conf /etc/krb5.conf

RUN mkdir -p /etc/krb5kdc /etc/keytabs

RUN cat > /etc/krb5kdc/kdc.conf <<'EOF'
[kdcdefaults]
    kdc_ports = 88
    kdc_tcp_ports = 88

[realms]
    KAFKA.LOCAL = {
        acl_file = /etc/krb5kdc/kadm5.acl
        dict_file = /usr/share/dict/words
        admin_keytab = /etc/krb5kdc/kadm5.keytab
        supported_enctypes = aes256-cts:normal aes128-cts:normal
        max_life = 24h 0m 0s
        max_renewable_life = 7d 0h 0m 0s
    }
EOF

RUN echo "*/admin@KAFKA.LOCAL *" > /etc/krb5kdc/kadm5.acl

RUN kdb5_util create -s -r KAFKA.LOCAL -P admin

EXPOSE 88 749

CMD ["/bin/sh", "-c", "krb5kdc && kadmind && tail -f /dev/null"]
