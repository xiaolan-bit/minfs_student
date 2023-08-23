package com.ksyun.campus.metaserver.controller;

import com.ksyun.campus.metaserver.domain.StatInfo;
import com.ksyun.campus.metaserver.services.MetaService;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.List;

@RestController("/")
public class MetaController {

    @Autowired
    MetaService metaService;

    @Value("${meta.role}")
    public String role;
    @RequestMapping("stats")
    public ResponseEntity stats(@RequestHeader String fileSystem,@RequestParam String path){
        return new ResponseEntity(metaService.getFileStats(path),HttpStatus.OK);
    }
    @RequestMapping("create")
    public ResponseEntity createFile(@RequestHeader String fileSystem, @RequestParam String path){
        return new ResponseEntity(HttpStatus.OK);
    }
//    @RequestMapping("mkdir")
//    public ResponseEntity mkdir(@RequestHeader String fileSystem, @RequestParam String path){
//        return new ResponseEntity(HttpStatus.OK);
//    }
    @RequestMapping("listdir")
    public ResponseEntity listdir(@RequestHeader String fileSystem,@RequestParam String path){
        return new ResponseEntity(metaService.listFileStats(path),HttpStatus.OK);
    }
//    @RequestMapping("delete")
//    public ResponseEntity delete(@RequestHeader String fileSystem, @RequestParam String path){
//        return new ResponseEntity(HttpStatus.OK);
//    }

    /**
     * 保存文件写入成功后的元数据信息，包括文件path、size、三副本信息等
     * @param fileSystem
     * @param path
     * @param offset
     * @param length
     * @return
     */
    @RequestMapping("write")
    public ResponseEntity commitWrite(@RequestHeader String fileSystem, @RequestParam String path, @RequestParam int offset, @RequestParam int length, HttpServletRequest request){
        //metaService.metaWrite(fileSystem,path,offset,length,request);
        System.out.println("1111");
        return new ResponseEntity(metaService.metaWrite(fileSystem,path,offset,length,request),HttpStatus.OK);
    }

    /**
     * 根据文件path查询三副本的位置，返回客户端具体ds、文件分块信息
     * @param fileSystem
     * @param path
     * @return
     */
    @RequestMapping("open")
    public ResponseEntity<byte[]> open(@RequestHeader String fileSystem,@RequestParam String path,HttpServletRequest request){
        //return
        //System.out.println("read success");
        return new ResponseEntity(metaService.metaRead(fileSystem,path,request),HttpStatus.OK);
    }

    @RequestMapping("mkdir")
    public ResponseEntity<String> mkdir(@RequestHeader String fileSystem, @RequestParam String path) throws Exception {
        return new ResponseEntity<>(metaService.mkdir(fileSystem,path),HttpStatus.OK);
    }

    @RequestMapping("delete")
    public ResponseEntity<String> delete(@RequestHeader String fileSystem, @RequestParam String path) throws Exception {
        return new ResponseEntity<>(metaService.delete(fileSystem,path),HttpStatus.OK);
    }

    @RequestMapping("checkbeat")
    public String checkbeat() throws IOException, InterruptedException, KeeperException {
        return "heartbeat";
    }

    @RequestMapping("asycLocalStatInfoList")
    public String asycLocalStatInfoList(@RequestBody List<StatInfo> statInfos){
        metaService.asycLocalStatInfoList(statInfos);
        return "async success";
    }
    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer(){
        System.exit(-1);
    }

}
