package com.ksyun.campus.dataserver.services;

import com.ksyun.campus.dataserver.domain.MetaInfo;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

@Service
public class DataService {
//    public DataService(ZooKeeper zk) {
//
//    }
//    @Value("${filePath}")
//    public String fileSetPath;
    private final String basePath = "/dataserver";
    private final ZooKeeper zk;

    @Value("${az.rack}")
    public String azRack;

    @Value("${az.zone}")
    public String azZone;

    @Value("${zookeeper.addr}")
    private String ZOOKEEPER_ADDRESS; // ZooKeeper服务器地址
    private static final int SESSION_TIMEOUT = 3000; // 会话超时时间，单位：毫秒

    public DataService(ZooKeeper zk) {
        this.zk = zk;
        //createReplicasNodeIfNeeded();
    }

    public List<String> write(byte[] data) {
        // Todo: Write data locally and save replicas
        List<String> replicas = getDsAddresses();
        // Todo: Register replicas in ZooKeeper
        // Todo: Choose strategy to distribute replicas among different az and rack
        // Todo: Support retry mechanism
        // Todo: Return three replica locations
        return replicas;
    }

    private List<String> getDsAddresses() {
        try {
            List<String> dsAddresses = new ArrayList<>();
            List<String> children = zk.getChildren(basePath, false);
            for (String child : children) {
                byte[] data = zk.getData(basePath + "/" + child, false, null);
                dsAddresses.add(new String(data));
            }
            return dsAddresses;
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    public byte[] read(String fileSystem,String path, int offset, int length) {
        try {
            byte[] fileData;
            if(fileSystem.equals("no")){
                fileData = Files.readAllBytes(Paths.get(path));
            }else {
                fileData = Files.readAllBytes(Paths.get(fileSystem+azRack+"/"+azZone+path));
            }

            int endIndex = Math.min(offset + length, fileData.length);
            System.out.println("data:"+Arrays.copyOfRange(fileData, offset, endIndex));
            return Arrays.copyOfRange(fileData, offset, endIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String mkdir(String fileSystem, String path) {
        File directory = new File(fileSystem, Paths.get(azRack+"/"+azZone+path).toString());
        if (!directory.exists()) {
            directory.mkdirs();
        }
        return directory.getAbsolutePath();
    }

    public String delete(String fileSystem, String path) throws Exception {
        //File file = new File(fileSystem, azRack + "/" + azZone + path);
        File file = new File("",  Paths.get(path).toString());
        System.out.println("path:"+path);
        String rackZone = getRackZone(path);
        freeCapacity(file.length(),rackZone);
        System.out.println(path);
        if (file.exists()) {
            file.delete();
        }
        return file.getAbsolutePath();
    }

    public void decreaseCapacity(long n) throws Exception {
        // 创建一个CountDownLatch，用于等待Zookeeper连接成功
        CountDownLatch connectedSignal = new CountDownLatch(1);

        // 连接到Zookeeper服务器
        ZooKeeper zk = new ZooKeeper(ZOOKEEPER_ADDRESS, 5000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                // 连接成功，释放CountDownLatch
                connectedSignal.countDown();
            }
        });

        // 等待Zookeeper连接成功
        connectedSignal.await();

        // 获取/ds/rack1-zone1节点的数据
        byte[] data = zk.getData("/ds/"+azRack+"-"+azZone, false, null);
        String dataStr = new String(data);

        // 解析数据，获取capacity的值
        String[] parts = dataStr.split(",");
        long capacity = 0;
        for (String part : parts) {
            if (part.startsWith("capacity=")) {
                capacity = Long.parseLong(part.substring("capacity=".length()));
                break;
            }
        }

        // 减少capacity的值
        capacity -= n;

        // 更新数据字符串中的capacity值
        dataStr = dataStr.replaceFirst("capacity=\\d+", "capacity=" + capacity);

        // 更新Zookeeper中的数据
        zk.setData("/ds/"+azRack+"-"+azZone, dataStr.getBytes(), -1);

        // 关闭Zookeeper连接
        zk.close();
    }

    public String getRackZone(String path){
        int rackIndex = path.indexOf("rack");
        String rackNum = path.substring(rackIndex + "rack".length(), rackIndex + "rack".length() + 1);
        System.out.println("rack number: " + rackNum);

        // 获取zone后面的数字
        int zoneIndex = path.indexOf("zone");
        String zoneNum = path.substring(zoneIndex + "zone".length(), zoneIndex + "zone".length() + 1);
        System.out.println("zone number: " + zoneNum);
        return "rack"+rackNum+"-"+"zone"+zoneNum;
    }

    public void freeCapacity(long n,String rackZone) throws Exception {
        // 创建一个CountDownLatch，用于等待Zookeeper连接成功
        CountDownLatch connectedSignal = new CountDownLatch(1);

        // 连接到Zookeeper服务器
        ZooKeeper zk = new ZooKeeper(ZOOKEEPER_ADDRESS, 5000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                // 连接成功，释放CountDownLatch
                connectedSignal.countDown();
            }
        });

        // 等待Zookeeper连接成功
        connectedSignal.await();

        // 获取/ds/rack1-zone1节点的数据
        byte[] data = zk.getData("/ds/"+rackZone, false, null);
        String dataStr = new String(data);

        // 解析数据，获取capacity的值
        String[] parts = dataStr.split(",");
        long capacity = 0;
        for (String part : parts) {
            if (part.startsWith("capacity=")) {
                capacity = Long.parseLong(part.substring("capacity=".length()));
                break;
            }
        }

        // 增加capacity的值
        capacity += n;

        // 更新数据字符串中的capacity值
        dataStr = dataStr.replaceFirst("capacity=\\d+", "capacity=" + capacity);

        // 更新Zookeeper中的数据
        zk.setData("/ds/"+rackZone, dataStr.getBytes(), -1);

        // 关闭Zookeeper连接
        zk.close();
    }

    @Scheduled(fixedRate = 2000)
    public void checkbeat() throws IOException, InterruptedException, KeeperException {
        System.out.println("load");
        List<MetaInfo> metaInfos = readAllMetaFromZooKeeper("/meta");
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        HttpEntity<String> requestEntity = new HttpEntity<>("", headers);

        for (MetaInfo m : metaInfos) {
            String dsReaderUrl = "http://" + m.getIp() + ":" + m.getPort() + "/checkbeat";
            //System.out.println(dsReaderUrl);
            String result;
            try {
                ResponseEntity<String> response = restTemplate.postForEntity(dsReaderUrl, requestEntity, String.class);
                //System.out.println("success");
            } catch (RestClientException e) {
                System.out.println("error");
                ZooKeeper zk = new ZooKeeper(ZOOKEEPER_ADDRESS, 3000, null);
//
                // 删除名为 "/meta/zone1" 的节点
                System.out.println("/meta/"+ m.getRole());
                zk.delete("/meta/"+ m.getRole(), -1);

                // 关闭连接
                zk.close();
                System.out.println("Removed registration for: " + dsReaderUrl);
            }

        }

    }

    private List<MetaInfo> readAllMetaFromZooKeeper(String path) {
        try {
            List<MetaInfo> metaInfoList = new ArrayList<>();

            // 创建ZooKeeper连接
            ZooKeeper zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // 处理ZooKeeper事件
                }
            });

            // 等待连接建立
            while (zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
                Thread.sleep(100);
            }

            // 获取指定路径下的子节点
            List<String> children = zooKeeper.getChildren(path, false);

            // 遍历子节点并解析每个节点的数据
            for (String child : children) {
                String childPath = path + "/" + child;
                byte[] data = zooKeeper.getData(childPath, false, null);
                String value = new String(data);

                // 解析数据并创建DataServerInfo对象
                MetaInfo metaInfo = parseMetaServerInfo(value);
                if (metaInfo != null) {
                    metaInfoList.add(metaInfo);
                }
            }

            // 关闭ZooKeeper连接
            zooKeeper.close();

            return metaInfoList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private MetaInfo parseMetaServerInfo(String data) {
        // 这里根据实际数据格式进行解析
        // 假设数据格式为：ip:port=192.168.109.1:9000,capacity=capacity_value,rack=rack1,zone=zone1
        String[] fields = data.split(",");
        MetaInfo metaInfo = new MetaInfo();
        for (String field : fields) {
            String[] keyValue = field.split("=");
            String key = keyValue[0].trim();
            String value = keyValue[1].trim();
            if (key.equals("ip:port")) {
                String[] ipPort = value.split(":");
                metaInfo.setIp(ipPort[0].trim());
                metaInfo.setPort(Integer.parseInt(ipPort[1].trim()));
            } else if (key.equals("role")) {
                metaInfo.setRole(value);
            }
        }
        return metaInfo;
    }
}
