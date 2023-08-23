package com.ksyun.campus.dataserver.services;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
//import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
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

    @Value("${az.rack}")
    private String azRack;

    @Value("${az.zone}")
    private String azZone;

    @Value("${zookeeper.addr}")
    private String zookeeperAddress;

    @Value("${capacity}")
    private int capacity;

    @Value("${fileTotal}")
    private int fileTotal;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        registToCenter();
    }

    public void registToCenter() {
        try {
            String instanceInfo = "ip:port=" + InetAddress.getLocalHost().getHostAddress() + ":" + serverPort +
                    ",capacity=" +capacity+  // Replace with the actual capacity value
                    ",rack=" + azRack +
                    ",zone=" + azZone +
                    ",fileTotal=" + fileTotal;

            CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(
                    zookeeperAddress,
                    new ExponentialBackoffRetry(1000, 3)
            );

            curatorFramework.start();

            String nodePath = "/ds/"+azRack+"-"+azZone;

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
