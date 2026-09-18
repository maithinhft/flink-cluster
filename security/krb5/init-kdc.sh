#!/bin/sh
set -e

REALM="${KRB5_REALM:-EXAMPLE.COM}"
KDC_HOST="${KRB5_KDC:-kdc}"
SERVER_IP="${SERVER_IP:-localhost}"

mkdir -p /var/lib/secret /etc/krb5kdc

# Install Kerberos packages if not present
if ! command -v krb5kdc > /dev/null 2>&1; then
    echo "Installing krb5-server and krb5..."
    apk add --no-cache krb5-server krb5
fi

# Generate krb5.conf
echo "Writing krb5.conf for realm ${REALM}..."
cat << EOF > /var/lib/secret/krb5.conf
[libdefaults]
    default_realm = ${REALM}
    dns_lookup_realm = false
    dns_lookup_kdc = false
    dns_canonicalize_hostname = false
    rdns = false
    ticket_lifetime = 24h
    renew_lifetime = 7d
    forwardable = true

[realms]
    ${REALM} = {
        kdc = ${KDC_HOST}:88
        admin_server = ${KDC_HOST}:749
    }

[domain_realm]
    .example.com = ${REALM}
    example.com = ${REALM}
EOF

cp /var/lib/secret/krb5.conf /etc/krb5.conf

# Initialize KDC database if not exists
if [ ! -f /var/lib/krb5kdc/principal ]; then
    echo "Creating KDC database for realm ${REALM}..."
    kdb5_util create -s -r "${REALM}" -P masterpassword
fi

# Helper function to add principal safely
add_principal() {
    local princ="$1"
    local pass="$2"
    if ! kadmin.local -q "getprinc ${princ}" 2>&1 | grep -q "Principal: ${princ}"; then
        if [ -n "$pass" ]; then
            kadmin.local -q "addprinc -pw ${pass} ${princ}"
        else
            kadmin.local -q "addprinc -randkey ${princ}"
        fi
    fi
}

echo "Adding Kerberos principals..."
add_principal "kafka/kafka-gssapi@${REALM}"
add_principal "kafka/localhost@${REALM}"
add_principal "kafka/kafka-gssapi.flink-cluster_cluster-network@${REALM}"
if [ "${SERVER_IP}" != "localhost" ] && [ "${SERVER_IP}" != "127.0.0.1" ]; then
    add_principal "kafka/${SERVER_IP}@${REALM}"
fi
add_principal "client@${REALM}" "clientpassword"
add_principal "admin@${REALM}" "adminpassword"

# Generate keytab files
echo "Exporting keytabs to /var/lib/secret/..."
rm -f /var/lib/secret/kafka.keytab /var/lib/secret/client.keytab

kadmin.local -q "ktadd -k /var/lib/secret/kafka.keytab kafka/kafka-gssapi@${REALM}"
kadmin.local -q "ktadd -k /var/lib/secret/kafka.keytab kafka/localhost@${REALM}"
kadmin.local -q "ktadd -k /var/lib/secret/kafka.keytab kafka/kafka-gssapi.flink-cluster_cluster-network@${REALM}"
if [ "${SERVER_IP}" != "localhost" ] && [ "${SERVER_IP}" != "127.0.0.1" ]; then
    kadmin.local -q "ktadd -k /var/lib/secret/kafka.keytab kafka/${SERVER_IP}@${REALM}"
fi

kadmin.local -q "ktadd -k /var/lib/secret/client.keytab client@${REALM}"
kadmin.local -q "ktadd -k /var/lib/secret/client.keytab admin@${REALM}"

chmod 644 /var/lib/secret/*.keytab /var/lib/secret/krb5.conf

echo "Kerberos KDC ready. Starting daemon on port 88/749..."
exec krb5kdc -n

