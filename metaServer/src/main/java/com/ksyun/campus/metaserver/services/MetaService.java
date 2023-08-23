package com.ksyun.campus.metaserver.services;

import com.ksyun.campus.metaserver.domain.*;
import okhttp3.MediaType;
import org.apache.zookeeper.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.AsyncRestTemplate;
import java.util.ArrayList;
import java.util.List;
import okhttp3.*;
import org.springframework.web.util.UriComponentsBuilder;

@Service
public class MetaService {
    @Value("${zookeeper.addr}")
    private String ZOOKEEPER_ADDRESS; // ZooKeeper服务器地址
    private static final int SESSION_TIMEOUT = 3000; // 会话超时时间，单位：毫秒

    private List<StatInfo> localStatInfoList = new ArrayList<>();
    RestTemplate restTemplate = new RestTemplate();

    AsyncRestTemplate asyncRestTemplate = new AsyncRestTemplate();

    @Value("${meta.role}")
    public String role;

    public List<DataServerInfo> pickDataServerValues() {
        return readAllDataFromZooKeeper("/ds");
    }

    public Object metaWrite(String fileSystem, String path, int offset, int length, HttpServletRequest request) {
        List<DataServerInfo> allList = readAllDataFromZooKeeper("/ds");

            if (allList.size() < 3) {
                return "节点数小于3，无法写入";
            }

            List<DataServerInfo> dataServerList = new ArrayList<>();

            if (allList.size() == 3) {
                dataServerList.addAll(allList);
            } else {
                // 从allList中随机选取三个节点
                Random random = new Random();
                List<DataServerInfo> tempList = new ArrayList<>(allList);
                for (int i = 0; i < 3; i++) {
                    int randomIndex = random.nextInt(tempList.size());
                    dataServerList.add(tempList.get(randomIndex));
                    tempList.remove(randomIndex);
                }
            }

        // 调用writeData方法执行实际写入操作
        //writeData(dataServerList,fileSystem,path,offset,length,request);
        writeData(dataServerList, "/root/", path, offset, length, request);
        System.out.println(pickDataServerValues());
        return "写入成功";
    }

    private List<DataServerInfo> readAllDataFromZooKeeper(String path) {
        try {
            List<DataServerInfo> dataServerInfoList = new ArrayList<>();

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
                DataServerInfo dataServerInfo = parseDataServerInfo(value);
                if (dataServerInfo != null) {
                    dataServerInfoList.add(dataServerInfo);
                }
            }

            // 关闭ZooKeeper连接
            zooKeeper.close();

            return dataServerInfoList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private DataServerInfo parseDataServerInfo(String data) {
        // 这里根据实际数据格式进行解析
        // 假设数据格式为：ip:port=192.168.109.1:9000,capacity=capacity_value,rack=rack1,zone=zone1
        String[] fields = data.split(",");
        DataServerInfo dataServerInfo = new DataServerInfo();
        for (String field : fields) {
            String[] keyValue = field.split("=");
            String key = keyValue[0].trim();
            String value = keyValue[1].trim();
            if (key.equals("ip:port")) {
                String[] ipPort = value.split(":");
                dataServerInfo.setIp(ipPort[0].trim());
                dataServerInfo.setPort(Integer.parseInt(ipPort[1].trim()));
            } else if (key.equals("capacity")) {
                dataServerInfo.setCapacity(value);
            } else if (key.equals("rack")) {
                dataServerInfo.setRack(value);
            } else if (key.equals("zone")) {
                dataServerInfo.setZone(value);
            } else if (key.equals("fileTotal")){
                dataServerInfo.setFileTotal(Integer.parseInt(value));
            }
        }
        return dataServerInfo;
    }

    public StatInfo getFileStats(String path){
        for (StatInfo s:localStatInfoList
             ) {
            if (compareFileNameAndExtension(s.getPath(),path)){
                return s;
            }
        }
        return null;
    }

    public List<StatInfo> listFileStats(String path){
        List<StatInfo> statInfos = new ArrayList<>();
        path = Paths.get(path).toString();
        System.out.println(path);
        for (StatInfo s:localStatInfoList
             ) {
            if (matchPath(path,s.getPath())){
                statInfos.add(s);
            }
        }
        return deduplicatePath(statInfos);
        //return statInfos;
    }

    private List<StatInfo> writeData(List<DataServerInfo> dataServerInfos, String fileSystem, String path, int offset, int length, HttpServletRequest request) {

        HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
        httpRequestFactory.setConnectionRequestTimeout(20000); // 设置连接超时时间
        httpRequestFactory.setConnectTimeout(30000); // 设置连接超时时间
        httpRequestFactory.setReadTimeout(500000); // 设置读取超时时间
        RestTemplate restTemplate = new RestTemplate(httpRequestFactory);


        List<StatInfo> statInfos = new ArrayList<>();
        WriteDataRequest writeDataRequest = new WriteDataRequest();
        writeDataRequest.setFileSystem("/root"+"/");
        //writeDataRequest.setPath(path);
        writeDataRequest.setPath(Paths.get(path).toString());
        writeDataRequest.setOffset(offset);
        writeDataRequest.setLength(length);
        writeDataRequest.setRequestBody(readRequestBody(request));
        getPathList(Paths.get(path).toString());
        int i =0;
        //三个副本
        StatInfo writeStatInfo1 = new StatInfo();
        StatInfo writeStatInfo2 = new StatInfo();
        StatInfo writeStatInfo3 = new StatInfo();
        List<ReplicaData> replicaDataList1 = new ArrayList<>();
        List<ReplicaData> replicaDataList2 = new ArrayList<>();
        List<ReplicaData> replicaDataList3 = new ArrayList<>();
        for (DataServerInfo d : dataServerInfos) {
            String url = "http://" + d.getIp() + ":" + d.getPort() + "/writebyclass";
            String responseEntity = restTemplate.postForObject(url, writeDataRequest, String.class);
            System.out.println(responseEntity);

            if (i==0){
                writeStatInfo1 = parseStringStatInfo(responseEntity);
            } else if (i==1) {
                writeStatInfo2 = parseStringStatInfo(responseEntity);
            } else if (i==2){
                writeStatInfo3 = parseStringStatInfo(responseEntity);
                i=-1;
            }
            replicaDataList1.add(createReplicaData(responseEntity,"id"+i));
            replicaDataList2.add(createReplicaData(responseEntity,"id"+i));
            replicaDataList3.add(createReplicaData(responseEntity,"id"+i));
            i++;
        }
        writeStatInfo1.setReplicaData(replicaDataList1);
        writeStatInfo2.setReplicaData(replicaDataList2);
        writeStatInfo3.setReplicaData(replicaDataList3);
        System.out.println("replicaDataList1:"+replicaDataList1);
        statInfos.add(writeStatInfo1);
        statInfos.add(writeStatInfo2);
        statInfos.add(writeStatInfo3);
        localStatInfoList.add(writeStatInfo1);
        localStatInfoList.add(writeStatInfo2);
        localStatInfoList.add(writeStatInfo3);
        System.out.println("localStatInfoList"+localStatInfoList.get(0).getMtime());
        System.out.println(localStatInfoList.size());
        localStatInfoList = deduplicate(localStatInfoList);
        System.out.println(localStatInfoList.size());
        pollLocalStatInfoList();
        return statInfos;

    }

    public static StatInfo parseStringStatInfo(String input) {
        StatInfo statInfo = new StatInfo();

        // Regular expression pattern to match the required information
        String pattern = "File '(.+)' fully overwritten successfully at offset (\\d+) with length (\\d+). File path: (.+), End offset: (\\d+)";
        Pattern regex = Pattern.compile(pattern);
        Matcher matcher = regex.matcher(input);

        if (matcher.find()) {
            String fileName = matcher.group(1);
            long fileSize = Long.parseLong(matcher.group(3));
            //2.String filePath = matcher.group(4);
            String filePath = Paths.get(matcher.group(4)).toString();
            statInfo.setType(FileType.File);
            long now = Instant.now().toEpochMilli();
            //statInfo.setMtime(now);
            statInfo.setPath(filePath);
            statInfo.setSize(fileSize);

        }
        long now = Instant.now().toEpochMilli();
        statInfo.setMtime(now);
        return statInfo;
    }

    public void getPathList(String filePath) {

        String[] parts = filePath.split("[/\\\\]");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length - 1; i++) {
            StatInfo statInfo = new StatInfo();
            statInfo.setType(FileType.File);
            long now = Instant.now().toEpochMilli();
            statInfo.setMtime(now);
            sb.append(parts[i]).append("/");
            statInfo.setPath(sb.toString());
            statInfo.setSize(0);
            localStatInfoList.add(statInfo);
        }
        pollLocalStatInfoList();
    }

    public ReplicaData createReplicaData(String input,String id) {
        String[] result = new String[2];
        ReplicaData replicaData = new ReplicaData();
        // 解析第一个字符串
        Pattern firstPattern = Pattern.compile("File path: ([^,]+)");
        Matcher firstMatcher = firstPattern.matcher(input);
        if (firstMatcher.find()) {
            String path = firstMatcher.group(1).trim();
            replicaData.setPath(path);
            result[0] = firstMatcher.group(1).trim();
            int rackIndex = path.indexOf("rack");
            String rackNum = path.substring(rackIndex + "rack".length(), rackIndex + "rack".length() + 1);
            System.out.println("rack number: " + rackNum);

            // 获取zone后面的数字
            int zoneIndex = path.indexOf("zone");
            String zoneNum = path.substring(zoneIndex + "zone".length(), zoneIndex + "zone".length() + 1);
            System.out.println("zone number: " + zoneNum);
            replicaData.setDsNode(getIpPortByRackZoneNum(rackNum,zoneNum));
            replicaData.setId("rack"+rackNum+"-zone"+zoneNum);
        }


 //       replicaData.setId(id);
        System.out.println("replicaData:234"+replicaData);
        return replicaData;
    }

    public String getIpPortByRackZoneNum(String rackNum, String zoneNum){
        List<DataServerInfo> dataServerInfoList = readAllDataFromZooKeeper("/ds");
        String ipPort = null;
        for (DataServerInfo d:dataServerInfoList
        ) {
            if (d.getRack().equals("rack"+rackNum)&&d.getZone().equals("zone"+zoneNum)){
                ipPort=d.getIp()+":"+d.getPort();
            }
        }
        return ipPort;
    }

    private byte[] readRequestBody(HttpServletRequest request) {
        try (InputStream inputStream = request.getInputStream()) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[102400];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            return outputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
            // 处理读取请求内容的异常
            return new byte[0];
        }
    }

    public ResponseEntity<byte[]> metaRead(String fileSystem, String path, HttpServletRequest request){
        //String url = fileSystem+path;
        //System.out.println("url+"+url);
        String localPath=null;
        int size = 0;
        List<DataServerInfo> allList = readAllDataFromZooKeeper("/ds");
        for (StatInfo s:localStatInfoList
             ) {
            if (compareFileNameAndExtension(s.getPath(),path)){
                localPath = s.getPath();
                size = (int) s.getSize();
            }
        }
        String ipPort = allList.get(0).getIp()+":"+allList.get(0).getPort();
        String dsReaderUrl = "http://"+ipPort+"/read";
        System.out.println("dsReaderUrl:"+dsReaderUrl);
        //String url = "http://" + ipPort + "/yourPostEndpoint";
        HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
        httpRequestFactory.setConnectionRequestTimeout(20000); // 设置连接超时时间
        httpRequestFactory.setConnectTimeout(30000); // 设置连接超时时间
        httpRequestFactory.setReadTimeout(500000); // 设置读取超时时间
        RestTemplate restTemplate = new RestTemplate(httpRequestFactory);


// 准备请求头
        HttpHeaders headers = new HttpHeaders();
        headers.set("fileSystem", "no");

// 准备请求参数
        MultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
        //params.add("path", convertBackslashToSlash(localPath));
        params.add("path", localPath);
        //params.add("path","/data/file1.txt");
        params.add("offset", 0);
        params.add("length", size);

        System.out.println(params);
        //System.out.println(convertBackslashToSlash(localPath));
// 准备请求实体（包含请求头和请求参数）
        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(params, headers);

// 发送POST请求并获取响应
        ResponseEntity<byte[]> response = restTemplate.postForEntity(dsReaderUrl, requestEntity, byte[].class);

// 处理响应
        byte[] responseBody = response.getBody();
        System.out.println(response);
        System.out.println(responseBody);
        return response;
    }


    public boolean compareFileNameAndExtension(String path1, String path2) {
        File file1 = new File(path1);
        File file2 = new File(path2);
        return file1.getName().equals(file2.getName());
    }

    public String convertBackslashToSlash(String input) {
        return input.replace("\\", "/");
    }

    public String mkdir(String fileSystem, String path) throws Exception {
        List<DataServerInfo> allList = readAllDataFromZooKeeper("/ds");

        if (allList.size() < 3) {
            return "节点数小于3，无法写入";
        }

        List<DataServerInfo> dataServerList = new ArrayList<>();

        if (allList.size() == 3) {
            dataServerList.addAll(allList);
        } else {
            // 从allList中随机选取三个节点
            Random random = new Random();
            List<DataServerInfo> tempList = new ArrayList<>(allList);
            for (int i = 0; i < 3; i++) {
                int randomIndex = random.nextInt(tempList.size());
                dataServerList.add(tempList.get(randomIndex));
                tempList.remove(randomIndex);
            }
        }

        //String ipPort = getIpPortFromZooKeeper();
        //System.out.println(ipPort);
        getPathList(Paths.get(path).toString());
        int i=0;
        StatInfo statInfo1 = new StatInfo();
        StatInfo statInfo2 = new StatInfo();
        StatInfo statInfo3 = new StatInfo();
        List<ReplicaData> replicaDataList = new ArrayList<>();
        for (DataServerInfo d:dataServerList
             ) {
            String url = "http://"+d.getIp()+":"+d.getPort()+"/mkdir";
            final RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.set("fileSystem", "/root/");

// 准备请求参数
            MultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
            params.add("path", Paths.get(path).toString());

// 准备请求实体（包含请求头和请求参数）
            HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(params, headers);

// 发送POST请求并获取响应
            ResponseEntity<String> response = restTemplate.postForEntity(url, requestEntity, String.class);

// 处理响应
            String responseBody = response.getBody();
            System.out.println(response);
            System.out.println("responseBody"+responseBody);
            ReplicaData replicaData = parsemkdirReturn(responseBody,"id"+i);
            replicaDataList.add(replicaData);
            if (i==0){
                statInfo1.setPath(responseBody);
                statInfo1.setType(FileType.File);
                long now = Instant.now().toEpochMilli();
                statInfo1.setMtime(now);
            } else if (i==1) {
                statInfo2.setPath(responseBody);
                statInfo2.setType(FileType.File);
                long now = Instant.now().toEpochMilli();
                statInfo2.setMtime(now);
            } else if (i==2) {
                statInfo3.setPath(responseBody);
                statInfo3.setType(FileType.File);
                long now = Instant.now().toEpochMilli();
                statInfo3.setMtime(now);
                i=-1;
            }
            i++;
        }
        statInfo1.setReplicaData(replicaDataList);
        statInfo2.setReplicaData(replicaDataList);
        statInfo3.setReplicaData(replicaDataList);
        localStatInfoList.add(statInfo1);
        localStatInfoList.add(statInfo2);
        localStatInfoList.add(statInfo3);
//                return response;
        localStatInfoList = deduplicate(localStatInfoList);
        System.out.println(localStatInfoList.size());
        pollLocalStatInfoList();
        return "创建文件夹成功";
    }

    public String delete(String fileSystem, String path) throws Exception {

        List<String> deletePathList = new ArrayList<>();
        String path1 = Paths.get(path).toString();
        for (StatInfo s:localStatInfoList
             ) {
            String path2 = s.getPath();
            File file1 = new File(path1);
            File file2 = new File(path2);
            if (file1.getName().equals(file2.getName())) {
                deletePathList.add(s.getPath());
                System.out.println("文件名和后缀匹配");
            } else {
                System.out.println("文件名和后缀不匹配");
            }
        }
        String ipPort = getIpPortFromZooKeeper();
        System.out.println(ipPort);
        String url = "http://"+ipPort+"/delete";

        HttpHeaders headers = new HttpHeaders();
        headers.set("fileSystem", "");
        System.out.println("deletePathList:"+deletePathList);
// 准备请求参数
        for (String deletePath:deletePathList
             ) {
            deletePath = removeBeforeSlash(deletePath);
            System.out.println("deletePath:"+deletePath);
            MultiValueMap<String, Object> params = new LinkedMultiValueMap<>();
            params.add("path", deletePath);
            System.out.println("deletePath"+deletePath);
            System.out.println(params);
            System.out.println(path);
// 准备请求实体（包含请求头和请求参数）
            HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(params, headers);

// 发送POST请求并获取响应
            ResponseEntity<String> response = restTemplate.postForEntity(url, requestEntity, String.class);

// 处理响应
            String responseBody = response.getBody();
            System.out.println(response);
            System.out.println(responseBody);
//                return response;

        }
        //在StatInfo里面删除
        System.out.println("localStatInfoList:"+localStatInfoList);
        Iterator<StatInfo> iterator = localStatInfoList.iterator();
        while (iterator.hasNext()) {
            StatInfo s = iterator.next();
            String path2 = s.getPath();
            File file1 = new File(path1);
            File file2 = new File(path2);
            if (file1.getName().equals(file2.getName())) {
                iterator.remove();
            } else {
                System.out.println("文件名和后缀不匹配");
            }
        }
        System.out.println("localStatInfoList:"+localStatInfoList);
        return "删除完成";
    }



    public ReplicaData parsemkdirReturn(String path, String id){
        ReplicaData replicaData = new ReplicaData();
        replicaData.setPath(Paths.get(path).toString());
        String input = path;
        Pattern pattern = Pattern.compile("rack(\\d+).*zone(\\d+)");
        List<DataServerInfo> dataServerInfoList = readAllDataFromZooKeeper("/ds");

        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            String rackNumber = matcher.group(1);
            String zoneNumber = matcher.group(2);
            String ip=null;
            int port=0;
            for (DataServerInfo d:dataServerInfoList
                 ) {
                if (d.getRack().equals("rack"+rackNumber)&&d.getZone().equals("zone"+zoneNumber)){
                    ip=d.getIp();
                    port=d.getPort();
                }
            }
            replicaData.setId("rack"+rackNumber+"-"+"zone"+zoneNumber);
            replicaData.setDsNode(ip+":"+port);
        }
        System.out.println(replicaData);
        return replicaData;

    }
    public String getIpPortFromZooKeeper() throws Exception {
        // 连接到ZooKeeper服务器
// 连接到ZooKeeper服务器
        ZooKeeper zk = new ZooKeeper(ZOOKEEPER_ADDRESS, 3000, null);

// 获取/meta ZNode下的所有子节点
        List<String> children = zk.getChildren("/ds", false);
        List<String> ipPorts = new ArrayList<>();
// 遍历子节点列表
        for (String child : children) {
            // 获取子节点的完整路径
            String childPath = "/ds/" + child;

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

    public static String removeBeforeSlash(String input) {
        int index = input.indexOf("/");
        if (index == -1) {
            index = input.indexOf("\\");
        }
        if (index != -1) {
            return input.substring(index + 1);
        } else {
            return input;
        }
    }

    //对List<StatInfo去重>
    public static List<StatInfo> deduplicate(List<StatInfo> statInfos) {
        Map<String, StatInfo> resultMap = new HashMap<>();

        for (StatInfo statInfo : statInfos) {
            String path = statInfo.getPath();
            if (resultMap.containsKey(path)) {
                StatInfo existingStatInfo = resultMap.get(path);
                if (statInfo.getMtime() > existingStatInfo.getMtime()) {
                    resultMap.put(path, statInfo);
                }
            } else {
                resultMap.put(path, statInfo);
            }
        }

        return new ArrayList<>(resultMap.values());
    }

    public boolean matchPath(String path1, String path2) {
//        path1="/data/test/good/"+path1;
//        path2="/data/test/good/"+path2;
        String[] parts1 = path1.split("[/\\\\]");
        String[] parts2 = path2.split("[/\\\\]");
        if (parts2.length < 2) {
            return false;
        }
        String fileName = Paths.get(path1).getFileName().toString();
        System.out.println(parts1.length+":"+parts2.length);
        return fileName.equals(parts2[parts2.length - 2]);
    }

    public static List<StatInfo> deduplicatePath(List<StatInfo> statInfos) {
        Map<String, StatInfo> map = new HashMap<>();
        for (StatInfo statInfo : statInfos) {
            String fileName = Paths.get(statInfo.getPath()).getFileName().toString();
            if (!map.containsKey(fileName)) {
                map.put(fileName, statInfo);
            }
        }
        return new ArrayList<>(map.values());
    }

    @Scheduled(fixedRate = 2000)
    public void checkbeat() throws IOException, InterruptedException, KeeperException {
        List<DataServerInfo> dataServerInfoList = pickDataServerValues();
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        HttpEntity<String> requestEntity = new HttpEntity<>("", headers);

        for (DataServerInfo d : dataServerInfoList) {
            String dsReaderUrl = "http://" + d.getIp() + ":" + d.getPort() + "/checkbeat";
            //System.out.println(dsReaderUrl);
            String result;
            try {
                ResponseEntity<String> response = restTemplate.postForEntity(dsReaderUrl, requestEntity, String.class);
                //System.out.println("success");
            } catch (RestClientException e) {
                System.out.println("error");
                ZooKeeper zk = new ZooKeeper(ZOOKEEPER_ADDRESS, 3000, null);
//
                    // 删除名为 "/ds/zone1" 的节点
                    System.out.println("/ds/" + d.getRack() + "-" + d.getZone());
                    zk.delete("/ds/" + d.getRack() + "-" + d.getZone(), -1);

                    // 关闭连接
                    zk.close();
                    System.out.println("Removed registration for: " + dsReaderUrl);
            }

        }

    }

    //发请求
    public String pollLocalStatInfoList(){
        String aimRole = null;
        if (role.equals("master")){
            aimRole = "slave";
        } else if (role.equals("slave")) {
            aimRole = "master";
        }
        String ipPort = "null";
        List<MetaInfo> metaInfos = readAllMetaFromZooKeeper("/meta");
        for (MetaInfo m:metaInfos
             ) {
            if (m.getRole().equals(aimRole)){
                ipPort=m.getIp()+":"+m.getPort();
            }
        }
        if (!ipPort.equals("null"))
        {
            RestTemplate restTemplate = new RestTemplate();

            //HttpHeaders headers = new HttpHeaders();


            HttpHeaders headers = new HttpHeaders();
            //headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<List<StatInfo>> requestEntity = new HttpEntity<>(localStatInfoList, headers);
            String dsReaderUrl = "http://"+ipPort+"/asycLocalStatInfoList";
            try {
                ResponseEntity<String> response = restTemplate.postForEntity(dsReaderUrl, requestEntity, String.class);
                System.out.println("async success");
            } catch (RestClientException e) {
                System.out.println("no"+aimRole);
            }
            System.out.println(dsReaderUrl);


            // 准备请求参数
//            MultiValueMap<String, List<StatInfo>> params = new LinkedMultiValueMap<>();
//            params.add("statInfos", localStatInfoList);
//
//            System.out.println(params);
//
//    // 准备请求实体（包含请求头和请求参数）
//            HttpEntity<MultiValueMap<String, List<StatInfo>>> requestEntity = new HttpEntity<>(params, headers);
//            String dsReaderUrl = "http://"+ipPort+"/asycLocalStatInfoList";
//            System.out.println(localStatInfoList.toString());
//            System.out.println(dsReaderUrl);
//            try {
//                ResponseEntity<String> response = restTemplate.postForEntity(dsReaderUrl, requestEntity, String.class);
//                System.out.println("async success");
//            } catch (RestClientException e) {
//                System.out.println("no"+aimRole);
//            }
        }
        return null;
    }
    //收请求并同步
    public String asycLocalStatInfoList(List<StatInfo> statInfos){
        this.localStatInfoList = statInfos;
        System.out.println("async localStatInfoList");
        return "async localStatInfoList";
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

    //实现fsckRecovery
    //适配win和Linux
    @Scheduled(fixedRate = 10000)
    public void fsckRecovery() throws IOException {
        List<StatInfo> newlocalList = localStatInfoList;
        for (StatInfo s:localStatInfoList
             ) {
            if (Files.exists(Paths.get(s.getPath()))){

            }else {
                System.out.println(Paths.get(s.getPath())+"不存在了");
                String fileName = Paths.get(s.getPath()).getFileName().toString();
                System.out.println(fileName+"不存在了");
                for(int i=0;i<3;i++){
                    if (Files.exists(Paths.get(s.getReplicaData().get(i).getPath()))){
                        Files.copy(Paths.get(s.getReplicaData().get(i).getPath()), Paths.get(s.getPath()));
                        System.out.println("复制成功");
                        break;
                    }
                }
//                for (StatInfo s1:newlocalList
//                     ) {
//                    System.out.println(Paths.get(s1.getPath()));
//                    System.out.println(Files.exists(Paths.get(s1.getPath())));
//                    System.out.println(Paths.get(s1.getPath()).getFileName());
//                    System.out.println(Paths.get(s1.getPath()).getFileName().toString().equals(fileName));
//                    if (Files.exists(Paths.get(s1.getPath()))){
//                        if(Paths.get(s1.getPath()).getFileName().equals(fileName))
//                        {
//                            Files.copy(Paths.get(s1.getPath()), Paths.get(s.getPath()));
//                            System.out.println("复制成功");
//                            break;
//                        }
//                    }
//                }
            }
        }
    }
}
