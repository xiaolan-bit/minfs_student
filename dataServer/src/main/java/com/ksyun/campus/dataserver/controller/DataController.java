package com.ksyun.campus.dataserver.controller;

import com.ksyun.campus.dataserver.domain.WriteDataRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.nio.charset.StandardCharsets;
import com.ksyun.campus.dataserver.services.DataService;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.http.HttpStatus;
import java.nio.file.Path;
import java.nio.file.Paths;
@RestController("/")
public class DataController {
    @Autowired
    public DataService dataService;

//    @Value("${filePath}")
//    public String fileSetPath;

    @Value("${az.rack}")
    private String azRack;

    @Value("${az.zone}")
    private String azZone;
    public DataController(ZooKeeper zooKeeper) {
        this.dataService = new DataService(zooKeeper);
    }

    /**
     * 1、读取request content内容并保存在本地磁盘下的文件内
     * 2、同步调用其他ds服务的write，完成另外2副本的写入
     * 3、返回写成功的结果及三副本的位置
     *
     * @param fileSystem
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping("write")
    public ResponseEntity<String> writeFile(@RequestHeader String fileSystem,
                                            @RequestParam String path,
                                            @RequestParam int offset,
                                            @RequestParam int length,
                                            HttpServletRequest request) {
//        String fileSystem =
        // Determine the file name based on the provided path
        String fileName = path.substring(path.lastIndexOf('/') + 1);

        // Determine the file path in the D drive's "data" folder
        String directoryPath = fileSystem+azRack+"/"+azZone;
        Path filePath = Paths.get(directoryPath, path);

        // Create necessary directories if they don't exist
        File directory = filePath.getParent().toFile();
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                return new ResponseEntity<>("Failed to create directory: " + directory, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

        // Read the request content and save it to a byte array
        byte[] data = readRequestBody(request);

        System.out.println(offset+" "+length);
        if (data.length<length){
            length=data.length;
        }
        // Perform full overwrite write to the file at the specified offset
        writeDataToFile(filePath.toString(), data, offset, length);

        // Calculate the end offset of the data after writing
        int endOffset = offset + data.length;

        // Return the result as a response
        String response = "File '" + fileName + "' fully overwritten successfully at offset " + offset +
                " with length " + data.length + ". File path: " + filePath + ", End offset: " + endOffset;
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    @RequestMapping("writebyclass")
    public ResponseEntity<String> writeFileByClass(@RequestBody WriteDataRequest writeDataRequest) throws Exception {
        String fileSystem = writeDataRequest.getFileSystem();
        String path = writeDataRequest.getPath();
        int offset = writeDataRequest.getOffset();
        int length = writeDataRequest.getLength();
        byte[] data = writeDataRequest.getRequestBody();
        // Determine the file name based on the provided path
        String fileName = path.substring(path.lastIndexOf('/') + 1);

        // Determine the file path in the D drive's "data" folder
        String directoryPath = fileSystem+azRack+"/"+azZone;
        Path filePath = Paths.get(Paths.get(directoryPath).toString(), Paths.get(path).toString());

        // Create necessary directories if they don't exist
        File directory = filePath.getParent().toFile();
        if (!directory.exists()) {
            if (!directory.mkdirs()) {
                return new ResponseEntity<>("Failed to create directory: " + directory, HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }

        // Read the request content and save it to a byte array
        //byte[] data = readRequestBody(request);

        System.out.println(offset+" "+length);
//        if (data.length<length){
            length=data.length;
        //}
        // Perform full overwrite write to the file at the specified offset
        writeDataToFile(filePath.toString(), data, offset, length);

        // Calculate the end offset of the data after writing
        int endOffset = offset + data.length;
        dataService.decreaseCapacity(length);
        // Return the result as a response
        String response = "File '" + fileName + "' fully overwritten successfully at offset " + offset +
                " with length " + data.length + ". File path: " + filePath + ", End offset: " + endOffset;
        return new ResponseEntity<>(response, HttpStatus.OK);
    }

    private void writeDataToFile(String filePath, byte[] data, int offset, int length) {
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            System.out.println("Data数组大小：" + data.length);
            System.out.println("偏移量：" + offset + "，长度：" + length);

            // 验证偏移量和长度
            if (offset >= 0 && offset < data.length && length > 0 && offset + length <= data.length) {
                fos.write(data, offset, length);
                fos.flush();
            } else {
                System.err.println("给定data数组的偏移量或长度无效。");
            }
        } catch (IOException e) {
            e.printStackTrace();
            // 处理异常
        }
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

    private void saveDataToLocalDisk(byte[] data) {
        String directoryPath = "D:/data";
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            if (!directory.mkdir()) {
                System.err.println("Failed to create directory: " + directoryPath);
                return;
            }
        }

        String filePath = directoryPath + "/data.txt"; // Custom local save path in the data folder under D drive
        try (FileOutputStream fos = new FileOutputStream(new File(filePath))) {
            fos.write(data);
            fos.flush();
        } catch (IOException e) {
            e.printStackTrace();
            // Handle the exception for saving to local disk
        }
    }

    /**
     * 在指定本地磁盘路径下，读取指定大小的内容后返回
     *
     * @param fileSystem
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping("read")
    public ResponseEntity<byte[]> readFile(@RequestHeader String fileSystem, @RequestParam String path, @RequestParam int offset, @RequestParam int length) {
        // 根据path读取指定大小的内容
        byte[] data = dataService.read(fileSystem,path, offset, length);
        if (data != null) {
            return new ResponseEntity<>(data, HttpStatus.OK);
        } else {
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }

    @RequestMapping("mkdir")
    public ResponseEntity<String> mkdir(@RequestHeader String fileSystem, @RequestParam String path){
        return new ResponseEntity<>(dataService.mkdir(fileSystem,path),HttpStatus.OK);
    }

    @RequestMapping("delete")
    public ResponseEntity<String> delete(@RequestHeader String fileSystem, @RequestParam String path) throws Exception {
        return new ResponseEntity<>(dataService.delete(fileSystem,path),HttpStatus.OK);
    }

    //检测ds心跳接口
    @RequestMapping("checkbeat")
    public String checkbeat(){
        return azRack+"-"+azZone+"heartbeat";
    }
    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer() {
        System.exit(-1);
    }
}
