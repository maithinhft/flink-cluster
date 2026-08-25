export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

cd flink-jobs

mvn clean package

mvn exec:exec -Dexec.executable="java" -Dexec.args="-classpath %classpath flink.ValidationJob" -P local