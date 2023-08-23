package com.ksyun.campus.metaserver.services;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.net.InetAddress;

@Component
public class RegistService implements ApplicationRunner {

    @Value("${server.port}")
    private int serverPort;

    @Value("${zookeeper.addr}")
    private String zookeeperAddress;

    @Value("${meta.id}")
    private String metaId;

    @Value("${meta.role}")
    private String role;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        registToCenter();
    }

    public void registToCenter() {
        try {
            String instanceInfo = "ip:port=" + InetAddress.getLocalHost().getHostAddress() + ":" + serverPort +
                    ",role="+role;

            CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(
                    zookeeperAddress,
                    new ExponentialBackoffRetry(1000, 3)
            );

            curatorFramework.start();

            String nodePath = "/meta/"+metaId;

            if (curatorFramework.checkExists().forPath(nodePath) == null) {
                // Node does not exist, create a persistent node
                curatorFramework.create().creatingParentsIfNeeded().forPath(nodePath, instanceInfo.getBytes());
            } else {
                // Node already exists, update the data of the existing node
                curatorFramework.setData().forPath(nodePath, instanceInfo.getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
