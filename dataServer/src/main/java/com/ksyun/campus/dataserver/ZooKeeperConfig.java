package com.ksyun.campus.dataserver;

import org.apache.zookeeper.ZooKeeper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

@Configuration
public class ZooKeeperConfig {

    @Value("${zookeeper.addr}")
    private String zookeeperAddress;

    @Value("${zookeeper.sessionTimeout}")
    private int sessionTimeout;

    @Bean(destroyMethod = "close")
    public ZooKeeper zooKeeper() throws IOException {
        return new ZooKeeper(zookeeperAddress, sessionTimeout, event -> {
            // This is the watcher logic, you can customize it if needed
            System.out.println("ZooKeeper event: " + event);
        });
    }
}


