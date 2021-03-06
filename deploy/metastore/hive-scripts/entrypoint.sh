#!/bin/bash
function importCert() {
PEM_FILE=$1
PASSWORD=$2
KEYSTORE=$3
# number of certs in the PEM file
CERTS=$(grep 'END CERTIFICATE' $PEM_FILE| wc -l)

# For every cert in the PEM file, extract it and import into the JKS keystore
# awk command: step 1, if line is in the desired cert, print the line
#              step 2, increment counter when last line of cert is found
for N in $(seq 0 $(($CERTS - 1))); do
    ALIAS="${PEM_FILE%.*}-$N"
    cat $PEM_FILE |
    awk "n==$N { print }; /END CERTIFICATE/ { n++ }" |
    keytool -noprompt -import -trustcacerts \
            -alias $ALIAS -keystore $KEYSTORE -storepass $PASSWORD
done
}
set -e

# if the s3-compatible ca bundle is mounted in, add to the root Java truststore.
if [ -a /s3-compatible-ca/ca-bundle.crt ]; then
echo "Adding /s3-compatible-ca/ca-bundle.crt to $JAVA_HOME/lib/security/cacerts"
importCert /s3-compatible-ca/ca-bundle.crt changeit $JAVA_HOME/lib/security/cacerts
fi
# always add the openshift service-ca.crt if it exists
if [ -a /var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt ]; then
echo "Adding /var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt to $JAVA_HOME/lib/security/cacerts"
importCert /var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt changeit $JAVA_HOME/lib/security/cacerts
fi

# add UID to /etc/passwd if missing
if ! whoami &> /dev/null; then
    if [ -w /etc/passwd ]; then
        echo "Adding user ${USER_NAME:-hadoop} with current UID $(id -u) to /etc/passwd"
        # Remove existing entry with user first.
        # cannot use sed -i because we do not have permission to write new
        # files into /etc
        sed  "/${USER_NAME:-hadoop}:x/d" /etc/passwd > /tmp/passwd
        # add our user with our current user ID into passwd
        echo "${USER_NAME:-hadoop}:x:$(id -u):0:${USER_NAME:-hadoop} user:${HOME}:/sbin/nologin" >> /tmp/passwd
        # overwrite existing contents with new contents (cannot replace the
        # file due to permissions)
        cat /tmp/passwd > /etc/passwd
        rm /tmp/passwd
    fi
fi

# symlink our configuration files to the correct location
if [ -f /hadoop-config/core-site.xml ]; then
ln -s -f /hadoop-config/core-site.xml /etc/hadoop/core-site.xml
else
echo "/hadoop-config/core-site.xml doesnt exist, skipping symlink"
fi
if [ -f /hadoop-config/hdfs-site.xml ]; then
ln -s -f /hadoop-config/hdfs-site.xml /etc/hadoop/hdfs-site.xml
else
echo "/hadoop-config/hdfs-site.xml doesnt exist, skipping symlink"
fi

ln -s -f /hive-config/hive-site.xml $HIVE_HOME/conf/hive-site.xml
ln -s -f /hive-config/hive-log4j2.properties $HIVE_HOME/conf/hive-log4j2.properties
ln -s -f /hive-config/hive-exec-log4j2.properties $HIVE_HOME/conf/hive-exec-log4j2.properties

export HADOOP_LOG_DIR="${HADOOP_HOME}/logs"
# Set garbage collection settings
export GC_SETTINGS="-XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${HADOOP_LOG_DIR}/heap_dump.bin -XX:+ExitOnOutOfMemoryError -XX:ErrorFile=${HADOOP_LOG_DIR}/java_error%p.log"
export VM_OPTIONS="$VM_OPTIONS -XX:+UseContainerSupport"

if [ -n "$JVM_INITIAL_RAM_PERCENTAGE" ]; then
VM_OPTIONS="$VM_OPTIONS -XX:InitialRAMPercentage=$JVM_INITIAL_RAM_PERCENTAGE"
fi
if [ -n "$JVM_MAX_RAM_PERCENTAGE" ]; then
VM_OPTIONS="$VM_OPTIONS -XX:MaxRAMPercentage=$JVM_MAX_RAM_PERCENTAGE"
fi
if [ -n "$JVM_MIN_RAM_PERCENTAGE" ]; then
VM_OPTIONS="$VM_OPTIONS -XX:MinRAMPercentage=$JVM_MIN_RAM_PERCENTAGE"
fi

# Set JMX options
export JMX_OPTIONS="-javaagent:/opt/jmx_exporter/jmx_exporter.jar=8082:/opt/jmx_exporter/config/config.yml -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.port=8081 -Dcom.sun.management.jmxremote.rmi.port=8081 -Djava.rmi.server.hostname=127.0.0.1"

# Set garbage collection logs
export GC_SETTINGS="${GC_SETTINGS} -verbose:gc"

export HIVE_LOGLEVEL="${HIVE_LOGLEVEL:-INFO}"
export HADOOP_OPTS="${HADOOP_OPTS} ${VM_OPTIONS} ${GC_SETTINGS} ${JMX_OPTIONS}"
export HIVE_METASTORE_HADOOP_OPTS=" -Dhive.log.level=${HIVE_LOGLEVEL} "
export HIVE_OPTS="${HIVE_OPTS} --hiveconf hive.root.logger=${HIVE_LOGLEVEL},console "

set +e
if schematool -dbType postgres -info -verbose; then
    echo "Hive metastore schema verified."
else
    if schematool -dbType postgres -initSchema -verbose; then
        echo "Hive metastore schema created."
    else
        echo "Error creating hive metastore: $?"
    fi
fi
set -e
exec $@
