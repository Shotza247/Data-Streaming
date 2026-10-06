#!/bin/sh
# Probe Maven Central for correct JAR filenames
wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/ -O /tmp/k.html 2>/dev/null
echo "=== kafka connector ==="
grep -oE 'flink-sql-connector-kafka-[^"]+\.jar' /tmp/k.html | grep -v sources | grep -v javadoc | sort -u

wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/ -O /tmp/j.html 2>/dev/null
echo "=== jdbc connector ==="
grep -oE 'flink-connector-jdbc-[^"]+\.jar' /tmp/j.html | grep -v sources | grep -v javadoc | sort -u

wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.18.1/ -O /tmp/s.html 2>/dev/null
echo "=== s3 fs hadoop ==="
grep -oE 'flink-s3-fs-hadoop-[^"]+\.jar' /tmp/s.html | grep -v sources | grep -v javadoc | sort -u

wget -q https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/ -O /tmp/p.html 2>/dev/null
echo "=== postgresql ==="
grep -oE 'postgresql-[^"]+\.jar' /tmp/p.html | grep -v sources | grep -v javadoc | sort -u
