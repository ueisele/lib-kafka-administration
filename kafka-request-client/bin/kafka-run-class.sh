#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ $# -lt 1 ];
then
  echo "USAGE: $0 [-daemon] [-name servicename] [-loggc] classname [opts]"
  exit 1
fi

# CYGINW == 1 if Cygwin is detected, else 0.
if [[ $(uname -a) =~ "CYGWIN" ]]; then
  CYGWIN=1
else
  CYGWIN=0
fi

if [ -z "$INCLUDE_TEST_JARS" ]; then
  INCLUDE_TEST_JARS=false
fi

# Exclude jars not necessary for running commands.
regex="(-(test|src|scaladoc|javadoc)\.jar|jar.asc)$"
should_include_file() {
  if [ "$INCLUDE_TEST_JARS" = true ]; then
    return 0
  fi
  file=$1
  if [ -z "$(echo "$file" | egrep "$regex")" ] ; then
    return 0
  else
    return 1
  fi
}

base_dir=$(dirname $0)/..

if [ -z "$SCALA_VERSION" ]; then
  SCALA_VERSION=2.12.5
fi

if [ -z "$SCALA_BINARY_VERSION" ]; then
  SCALA_BINARY_VERSION=$(echo $SCALA_VERSION | cut -f 1-2 -d '.')
fi

# run ./gradlew copyDependantLibs to get all dependant jars in a local dir
shopt -s nullglob
for dir in "$base_dir"/build/dependant-libs;
do
  CLASSPATH="$CLASSPATH:$dir/*"
done

for file in "$base_dir"/build/libs/*.jar;
do
  if should_include_file "$file"; then
    CLASSPATH="$CLASSPATH":"$file"
  fi
done
shopt -u nullglob

if [ -z "$CLASSPATH" ] ; then
  echo "Classpath is empty. Please build the project first e.g. by running './gradlew build"
  exit 1
fi

# JMX settings
if [ -z "$KAFKA_JMX_OPTS" ]; then
  KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi

# JMX port to use
if [  $JMX_PORT ]; then
  KAFKA_JMX_OPTS="$KAFKA_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

# Log directory to use
if [ "x$LOG_DIR" = "x" ]; then
  LOG_DIR="$base_dir/logs"
fi

# Log4j settings
if [ -z "$KAFKA_LOG4J_OPTS" ]; then
  # Log to console. This is a tool.
  LOG4J_DIR="$base_dir/config/tools-log4j.properties"
  # If Cygwin is detected, LOG4J_DIR is converted to Windows format.
  (( CYGWIN )) && LOG4J_DIR=$(cygpath --path --mixed "${LOG4J_DIR}")
  KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:${LOG4J_DIR}"
else
  # create logs directory
  if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
  fi
fi

# If Cygwin is detected, LOG_DIR is converted to Windows format.
(( CYGWIN )) && LOG_DIR=$(cygpath --path --mixed "${LOG_DIR}")
KAFKA_LOG4J_OPTS="-Dkafka.logs.dir=$LOG_DIR $KAFKA_LOG4J_OPTS"

# Generic jvm settings you want to add
if [ -z "$KAFKA_OPTS" ]; then
  KAFKA_OPTS=""
fi

# Set Debug options if enabled
if [ "x$KAFKA_DEBUG" != "x" ]; then

    # Use default ports
    DEFAULT_JAVA_DEBUG_PORT="5005"

    if [ -z "$JAVA_DEBUG_PORT" ]; then
        JAVA_DEBUG_PORT="$DEFAULT_JAVA_DEBUG_PORT"
    fi

    # Use the defaults if JAVA_DEBUG_OPTS was not set
    DEFAULT_JAVA_DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=${DEBUG_SUSPEND_FLAG:-n},address=$JAVA_DEBUG_PORT"
    if [ -z "$JAVA_DEBUG_OPTS" ]; then
        JAVA_DEBUG_OPTS="$DEFAULT_JAVA_DEBUG_OPTS"
    fi

    echo "Enabling Java debug options: $JAVA_DEBUG_OPTS"
    KAFKA_OPTS="$JAVA_DEBUG_OPTS $KAFKA_OPTS"
fi

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Memory options
if [ -z "$KAFKA_HEAP_OPTS" ]; then
  KAFKA_HEAP_OPTS="-Xmx256M"
fi

# JVM performance options
if [ -z "$KAFKA_JVM_PERFORMANCE_OPTS" ]; then
  KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true"
fi


while [ $# -gt 0 ]; do
  COMMAND=$1
  case $COMMAND in
    -name)
      DAEMON_NAME=$2
      CONSOLE_OUTPUT_FILE=$LOG_DIR/$DAEMON_NAME.out
      shift 2
      ;;
    -loggc)
      if [ -z "$KAFKA_GC_LOG_OPTS" ]; then
        GC_LOG_ENABLED="true"
      fi
      shift
      ;;
    -daemon)
      DAEMON_MODE="true"
      shift
      ;;
    *)
      break
      ;;
  esac
done

# GC options
GC_FILE_SUFFIX='-gc.log'
GC_LOG_FILE_NAME=''
if [ "x$GC_LOG_ENABLED" = "xtrue" ]; then
  GC_LOG_FILE_NAME=$DAEMON_NAME$GC_FILE_SUFFIX
  # the first segment of the version number, which is '1' for releases before Java 9
  # it then becomes '9', '10', ...
  JAVA_MAJOR_VERSION=$($JAVA -version 2>&1 | sed -E -n 's/.* version "([^.-]*).*"/\1/p')
  if [[ "$JAVA_MAJOR_VERSION" -ge "9" ]] ; then
    KAFKA_GC_LOG_OPTS="-Xlog:gc*:file=$LOG_DIR/$GC_LOG_FILE_NAME:time,tags:filecount=10,filesize=102400"
  else
    KAFKA_GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M"
  fi
fi

# Remove a possible colon prefix from the classpath (happens at lines like `CLASSPATH="$CLASSPATH:$file"` when CLASSPATH is blank)
# Syntax used on the right side is native Bash string manipulation; for more details see
# http://tldp.org/LDP/abs/html/string-manipulation.html, specifically the section titled "Substring Removal"
CLASSPATH=${CLASSPATH#:}

# If Cygwin is detected, classpath is converted to Windows format.
(( CYGWIN )) && CLASSPATH=$(cygpath --path --mixed "${CLASSPATH}")

# Launch mode
if [ "x$DAEMON_MODE" = "xtrue" ]; then
  nohup $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS $KAFKA_LOG4J_OPTS -cp $CLASSPATH $KAFKA_OPTS "$@" > "$CONSOLE_OUTPUT_FILE" 2>&1 < /dev/null &
else
  exec $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS $KAFKA_LOG4J_OPTS -cp $CLASSPATH $KAFKA_OPTS "$@"
fi