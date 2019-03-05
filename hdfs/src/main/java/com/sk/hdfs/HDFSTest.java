//package com.sk.hdfs;
//
//import lombok.extern.slf4j.Slf4j;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.*;
//import org.apache.hadoop.io.IOUtils;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.*;
//import java.util.HashMap;
//
//@Slf4j
//public class HDFSTest {
//    Configuration conf;
//    FileSystem fs;
//
//    @Before
//    public void connection() throws IOException {
//        conf = new Configuration(true);
//        fs = FileSystem.get(conf);
//    }
//
//    @Test
//    public void mkdir() throws IOException {
////      创建文件夹
//        Path file = new Path("/dir");
//        if (fs.exists(file)) {
//            fs.delete(file, true);
//        }
//        fs.mkdirs(file);
//
//    }
//
//    @Test
//    public void upload() throws IOException {
////      上传文件
//        FSDataOutputStream output = fs.create(new Path("/dir/hello.txt"));
//        InputStream input = new BufferedInputStream(new FileInputStream(new File("C:\\aow_drv.log")));
//        IOUtils.copyBytes(input, output, conf, true);
//    }
//
//    @Test
//    public void download() throws IOException {
////      下载文件
//        FSDataInputStream input = fs.open(new Path("/dir/hello.txt"));
//        OutputStream output = new BufferedOutputStream(new FileOutputStream(new File("D:\\hello.txt")));
//        IOUtils.copyBytes(input, output, conf, true);
//    }
//
//    @Test
//    public void block() throws IOException {
////        读取分区
//        FileStatus status = fs.getFileLinkStatus(new Path("/dir/hello.txt"));
//        BlockLocation[] blocks = fs.getFileBlockLocations(status, 0, status.getLen());
//        for (BlockLocation block : blocks) {
////            log.info(block.toString());
//            System.out.println(block);
//        }
//        FSDataInputStream input = fs.open(new Path("/dir/hello.txt"));
//        System.out.println((char) input.readByte());
//        System.out.println((char) input.readByte());
//        System.out.println((char) input.readByte());
//        System.out.println((char) input.readByte());
//        System.out.println((char) input.readByte());
//        System.out.println((char) input.readByte());
//        System.out.println((char) input.readByte());
//        System.out.println((char) input.readByte());
//        System.out.println("=================================");
//        input.seek(1000);
//        System.out.println((char) input.readByte());
//        System.out.println((char) input.readByte());
//        System.out.println((char) input.readByte());
//        System.out.println((char) input.readByte());
//    }
//
//    @After
//    public void close() throws IOException {
//        fs.close();
//    }
//}
