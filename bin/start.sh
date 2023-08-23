#!/bin/bash
# 当前脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 获取工作目录
export WORKPUBLISH_HOME="$SCRIPT_DIR/.."
# 设置ZK的连接接口
#ZK_PATH="10.0.0.201:2181"
ZK_PATH="127.0.0.1:2181"
# 设置DS服务的日志路径
LOG_DIR="$WORKPUBLISH_HOME/dataServer/logs"
# 创建DS服务日志目录
mkdir -p "$LOG_DIR"
# 启动ds数据服务
LOG_FILE_DS0="$LOG_DIR/ds1.log"
LOG_FILE_DS1="$LOG_DIR/ds2.log"
LOG_FILE_DS2="$LOG_DIR/ds3.log"
LOG_FILE_DS3="$LOG_DIR/ds4.log"
cd "$WORKPUBLISH_HOME/dataServer"

echo "four dataServer starting"
# 分别用4个配置文件启动四个ds
nohup java -jar -Dzookeeper.addr="$ZK_PATH" dataServer-1.0.jar --spring.config.location=classpath:/application.yml > "$LOG_FILE_DS0" 2>&1 &
sleep 2
nohup java -jar -Dzookeeper.addr="$ZK_PATH" dataServer-1.0.jar --spring.config.location=classpath:/application-ds2.yml > "$LOG_FILE_DS1" 2>&1 &
sleep 2
nohup java -jar -Dzookeeper.addr="$ZK_PATH" dataServer-1.0.jar --spring.config.location=classpath:/application-ds3.yml > "$LOG_FILE_DS2" 2>&1 &
sleep 2
nohup java -jar -Dzookeeper.addr="$ZK_PATH" dataServer-1.0.jar --spring.config.location=classpath:/application-ds4.yml > "$LOG_FILE_DS3" 2>&1 &
sleep 2

# 记录MS的日志
LOG_DIR="$WORKPUBLISH_HOME/metaServer/logs"
mkdir -p "$LOG_DIR"
LOG_FILE_MS0="$LOG_DIR/ms1.log"
LOG_FILE_MS1="$LOG_DIR/ms2.log"
cd "$WORKPUBLISH_HOME/metaServer"
echo "two metaServer starting"
# 分别用两个配置文件启动两个ms服务
nohup java -Dzookeeper.addr="$ZK_PATH" -jar metaServer-1.0.jar --spring.config.location=classpath:/application.yml > "$LOG_FILE_MS0" 2>&1 &
sleep 2
nohup java -Dzookeeper.addr="$ZK_PATH" -jar metaServer-1.0.jar --spring.config.location=classpath:/application-ms2.yml > "$LOG_FILE_MS1" 2>&1 &
sleep 2
echo "All services are in port."



