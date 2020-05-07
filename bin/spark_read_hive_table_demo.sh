#!/usr/bin/env bash

## usage:
##
## sh spark_read_hive_table_demo.sh --auth-user <user name> --auth-id <secure id> --auth-key <secure key> \
##    --hive-metastore-uris <hive metastore address> --hive-db <hive database> --hive-table <--hive-table>

bin=$(dirname "$0")

ARGS=()
while [ $# != 0 ]; do
  case $1 in
  "--auth-id")
    authId=$2
    shift 2
    ;;
  "--auth-key")
    authKey=$2
    shift 2
    ;;
  "--auth-user")
    authUser=$2
    shift 2
    ;;
  *)
    ARGS+=("$1")
    shift
    ;;
  esac
done

## 认证方式: 使用环境变量
export hadoop_security_authentication_tbds_username="$authUser"
export hadoop_security_authentication_tbds_secureid="$authId"
export hadoop_security_authentication_tbds_securekey="$authKey"

/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit \
  --class com.tencent.tbds.demo.spark.ReadHiveTableDemo \
  --master yarn \
  --queue root.default \
  --deploy-mode client \
  --jars /usr/hdp/2.2.0.0-2041/hive/lib/jopt-simple-4.9.jar \
  "${bin}"/../dev-demo-1.0-SNAPSHOT.jar \
  "${ARGS[@]}"