package com.ksyun.campus.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.ksyun.campus.client.domain.ClusterInfo;
import com.ksyun.campus.client.domain.StatInfo;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import com.fasterxml.jackson.core.type.TypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.util.UriComponentsBuilder;

public class EFileSystem extends FileSystem{

    private String fileName="default";

    private RestTemplate restTemplate;

    private String ZOOKEEPER_ADDRESS ="127.0.0.1:2181";

    public EFileSystem() {
        String zkAddress = System.getProperty("zookeeper.addr");
        if (zkAddress != null){
            ZOOKEEPER_ADDRESS= zkAddress;
            this.ZOOKEEPER_ADDRESS = zkAddress;
        }
    }

    public EFileSystem(String fileName) {
        this.fileName = fileName;
    }

    public FSInputStream open(String path) throws Exception {
        String ipPort = getIpPortFromZooKeeper();
        System.out.println(ipPort);
        String url = "http://"+ipPort+"/open";
        HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
        httpRequestFactory.setConnectionRequestTimeout(20000); // 设置连接超时时间
        httpRequestFactory.setConnectTimeout(30000); // 设置连接超时时间
        httpRequestFactory.setReadTimeout(50000); // 设置读取超时时间
        RestTemplate restTemplate = new RestTemplate(httpRequestFactory);

        //final RestTemplate restTemplate = new RestTemplate();

        FSInputStream fsInputStream = new FSInputStream(){
            @Override
            public int read(byte[] b){
                List<String> pathString = splitString(path);
                HttpHeaders headers = new HttpHeaders();
                headers.set("fileSystem", pathString.get(0));

// 准备请求参数
                MultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
                params.add("path", pathString.get(1));
                //params.add("path","/data/file1.txt");
                params.add("offset", "0");
                params.add("length", "0");

                System.out.println(params);
                System.out.println(path);
// 准备请求实体（包含请求头和请求参数）
                HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(params, headers);

// 发送POST请求并获取响应
                ResponseEntity<byte[]> response = restTemplate.postForEntity(url, requestEntity, byte[].class);

// 处理响应
                byte[] responseBody = response.getBody();
                System.out.println(response);
                System.out.println(responseBody);
//                return response;
                return 0;
            }
            @Override
            public void close() throws IOException {

            }
        };
        return fsInputStream;
    }
    public FSOutputStream create(String path) throws Exception {
        String ipPort = getIpPortFromZooKeeper();
        System.out.println(ipPort);
        String url = "http://"+ipPort+"/write";
        HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
        httpRequestFactory.setConnectionRequestTimeout(20000); // 设置连接超时时间
        httpRequestFactory.setConnectTimeout(30000); // 设置连接超时时间
        httpRequestFactory.setReadTimeout(500000); // 设置读取超时时间
        RestTemplate restTemplate = new RestTemplate(httpRequestFactory);


        // Create an FSOutputStream and write data to it
        FSOutputStream fsOutputStream = new FSOutputStream() {
            @Override
            public void write(byte[] b) throws IOException {
                // Prepare headers
                HttpHeaders headers = new HttpHeaders();
                List<String> pathString = splitString(path);
                headers.set("fileSystem", pathString.get(0));

                UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url)
                        .queryParam("path", pathString.get(1))
                        .queryParam("offset", 0)
                        .queryParam("length", String.valueOf(b.length));
                HttpEntity<byte[]> requestEntity = new HttpEntity<>(b, headers);
                ResponseEntity<String> response = restTemplate.exchange(builder.toUriString(), HttpMethod.POST, requestEntity, String.class);
                System.out.println("Response Code: " + response.getStatusCodeValue());


            }

            @Override
            public void close() throws IOException {

            }
        };

        return fsOutputStream;
    }


    public boolean mkdir(String path) throws Exception {
        String ipPort = getIpPortFromZooKeeper();
        System.out.println(ipPort);
        String url = "http://"+ipPort+"/mkdir";
        final RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        List<String> pathString = splitString(path);
        headers.set("fileSystem", pathString.get(0));

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url)
                .queryParam("path", pathString.get(1));
        HttpEntity<byte[]> requestEntity = new HttpEntity<>(headers);
        ResponseEntity<String> response = restTemplate.exchange(builder.toUriString(), HttpMethod.POST, requestEntity, String.class);
        System.out.println("Response Code: " + response.getStatusCodeValue());
        return false;
    }
    public boolean delete(String path) throws Exception {
        String ipPort = getIpPortFromZooKeeper();
        System.out.println(ipPort);
        String url = "http://"+ipPort+"/delete";
        final RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        List<String> pathString = splitString(path);
        headers.set("fileSystem", pathString.get(0));

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url)
                .queryParam("path", pathString.get(1));
        HttpEntity<byte[]> requestEntity = new HttpEntity<>(headers);
        ResponseEntity<String> response = restTemplate.exchange(builder.toUriString(), HttpMethod.POST, requestEntity, String.class);
        System.out.println("Response Code: " + response.getStatusCodeValue());
        return false;
    }
    public StatInfo getFileStats(String path) throws Exception {
        String ipPort = getIpPortFromZooKeeper();
        System.out.println(ipPort);
        String url = "http://"+ipPort+"/stats";
        final RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        List<String> pathString = splitString(path);
        headers.set("fileSystem", pathString.get(0));

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url)
                .queryParam("path", pathString.get(1));
        HttpEntity<byte[]> requestEntity = new HttpEntity<>(headers);
        ResponseEntity<String> response = restTemplate.exchange(builder.toUriString(), HttpMethod.POST, requestEntity, String.class);
        System.out.println("Response Code: " + response.getStatusCodeValue()+response.getBody());
        ObjectMapper objectMapper = new ObjectMapper();

        // 将JSON字符串转换为StatInfo对象
        StatInfo statInfo = objectMapper.readValue(response.getBody(), StatInfo.class);

        // 打印StatInfo对象的内容
        System.out.println(statInfo);
        return statInfo;
    }
    public List<StatInfo> listFileStats(String path) throws Exception {
        String ipPort = getIpPortFromZooKeeper();
        System.out.println(ipPort);
        String url = "http://"+ipPort+"/listdir";
        final RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        List<String> pathString = splitString(path);
        headers.set("fileSystem", pathString.get(0));

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(url)
                .queryParam("path", pathString.get(1));
        HttpEntity<byte[]> requestEntity = new HttpEntity<>(headers);
        ResponseEntity<String> response = restTemplate.exchange(builder.toUriString(), HttpMethod.POST, requestEntity, String.class);
        System.out.println("Response Code: " + response.getStatusCodeValue()+response.getBody());
        ObjectMapper objectMapper = new ObjectMapper();

        List<StatInfo> statInfos = objectMapper.readValue(response.getBody(), new TypeReference<List<StatInfo>>() {});

        // 打印StatInfo对象的内容
        System.out.println(statInfos);
        return statInfos;
    }
    public ClusterInfo getClusterInfo() throws Exception {
        ClusterInfo.MetaServerMsg masterMetaServer = new ClusterInfo().new MetaServerMsg();
        List<String> metaInfo = getMetaValue("master");
        System.out.println(metaInfo);
        masterMetaServer.setHost(metaInfo.get(0));
        masterMetaServer.setPort(Integer.valueOf(metaInfo.get(1)));

        ClusterInfo.MetaServerMsg slaveMetaServer = new ClusterInfo().new MetaServerMsg();
        metaInfo = getMetaValue("slave");
        masterMetaServer.setHost(metaInfo.get(0));
        masterMetaServer.setPort(Integer.valueOf(metaInfo.get(1)));

        List<ClusterInfo.DataServerMsg> dataServers = getDataServerMsgs();

        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setMasterMetaServer(masterMetaServer);
        clusterInfo.setSlaveMetaServer(slaveMetaServer);
        clusterInfo.setDataServer(dataServers);

        return clusterInfo;
    }

    public String getIpPortFromZooKeeper() throws Exception {
        // 连接到ZooKeeper服务器
// 连接到ZooKeeper服务器
        ZooKeeper zk = new ZooKeeper(ZOOKEEPER_ADDRESS, 3000, null);

// 获取/meta ZNode下的所有子节点
        List<String> children = zk.getChildren("/meta", false);
        List<String> ipPorts = new ArrayList<>();
// 遍历子节点列表
        for (String child : children) {
            // 获取子节点的完整路径
            String childPath = "/meta/" + child;

            // 获取子节点中的数据
            byte[] data = zk.getData(childPath, false, null);

            // 将字节数组转换为字符串
            String dataStr = new String(data);

            //String dataStr = new String(data);

            // 解析字符串以获取IP和端口
            String[] parts = dataStr.split(",");
            String ipPort = parts[0].split("=")[1];

            ipPorts.add(ipPort);
            // 处理数据（例如打印输出）
            System.out.println(childPath + ": " + dataStr);
        }
        int size = ipPorts.size();
        int index = (int) (Math.random()*size-1);
        return ipPorts.get(index);
    }

    public static List<String> splitString(String input) {
        List<String> result = new ArrayList<>();
        int index = input.indexOf("/");
        if (index != -1) {
            result.add(input.substring(0, index + 1));
            result.add(input.substring(index));
        } else {
            result.add(input);
        }
        return result;
    }

    public List<String> getMetaValue(String value) throws Exception {
        List<String> result = new ArrayList<>();

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

        // 获取/meta/value节点的数据
        byte[] data = zk.getData("/meta/"+value, false, null);
        String dataStr = new String(data);

        // 解析数据，获取ip和port的值
        String[] parts = dataStr.split(",");
        for (String part : parts) {
            if (part.startsWith("ip:port=")) {
                String[] ipPort = part.substring("ip:port=".length()).split(":");
                result.add(ipPort[0]);
                result.add(ipPort[1]);
                break;
            }
        }

        // 关闭Zookeeper连接
        zk.close();

        return result;
    }

    public List<ClusterInfo.DataServerMsg> getDataServerMsgs() throws Exception {
        List<ClusterInfo.DataServerMsg> result = new ArrayList<>();

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

        // 获取/ds节点下的所有子节点
        List<String> children = zk.getChildren("/ds", false);
        for (String child : children) {
            // 获取子节点的数据
            byte[] data = zk.getData("/ds/" + child, false, null);
            String dataStr = new String(data);

            // 解析数据，创建DataServerMsg对象并赋值
            ClusterInfo.DataServerMsg dataServerMsg = new ClusterInfo.DataServerMsg();
            String[] parts = dataStr.split(",");
            for (String part : parts) {
                if (part.startsWith("ip:port=")) {
                    String[] ipPort = part.substring("ip:port=".length()).split(":");
                    dataServerMsg.setHost(ipPort[0]);
                    dataServerMsg.setPort(Integer.parseInt(ipPort[1]));
                } else if (part.startsWith("capacity=")) {
                    dataServerMsg.setUseCapacity(dataServerMsg.getCapacity()-Integer.parseInt(part.substring("capacity=".length())));
                } else if (part.startsWith("fileTotal=")) {
                    dataServerMsg.setFileTotal(Integer.parseInt(part.substring("fileTotal=".length())));
                }
            }

            // 将DataServerMsg对象添加到结果列表中
            result.add(dataServerMsg);
        }

        // 关闭Zookeeper连接
        zk.close();

        return result;
    }
}
