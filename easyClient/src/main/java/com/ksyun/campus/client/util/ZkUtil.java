package com.ksyun.campus.client.util;

import org.apache.zookeeper.*;

import javax.annotation.PostConstruct;
import java.io.IOException;

public class ZkUtil {
    // 默认的 ZooKeeper 连接字符串
    private String connectionString = "localhost:2181";
    // 默认的 ZooKeeper 会话超时时间
    private int sessionTimeout = 30000;

    @PostConstruct
    public void postCons() throws Exception {
        // 检查是否设置了启动参数 -Dzookeeper.addr
        String zkAddr = System.getProperty("zookeeper.addr");
        if (zkAddr != null && !zkAddr.isEmpty()) {
            // 如果设置了启动参数 -Dzookeeper.addr，则使用该地址
            connectionString = zkAddr;
        }

        try {
            // 初始化与 ZooKeeper 的连接
            ZooKeeper zk = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // 这里可以处理 ZooKeeper 事件，例如监听路径的变化
                    // 当配置有变化时，随时更新逻辑写在这里
                }
            });

            // 注册监听路径：
            String path = "/config";
            zk.exists(path, true);

            // 等待与 ZooKeeper 建立连接
            while (zk.getState() != ZooKeeper.States.CONNECTED) {
                Thread.sleep(100);
            }

            // 在这里进行其他初始化操作，例如创建节点、获取数据等
            // ...

            // 关闭与 ZooKeeper 的连接
            zk.close();

        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
