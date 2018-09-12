package com.xian.liankloud.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 在该类中完成对hdfs的操作
 */
public class HDFSTest {

    //HDFS的操作入口类 FileSystem
    private FileSystem fs;
    private URI uri;//hdfs的rpc通信地址
    private Configuration conf;//hadoop配置环境的抽象表示

    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");

    @Before
    public void setUp() throws Exception {
        uri = new URI("hdfs://lky01:9000");
        conf = new Configuration();
        fs = FileSystem.get(uri, conf);
//        System.out.println(fs);
    }

    /**
        列出目录的内容 listStatus
     *  hdfs dfs -ls /
     *  -rw-r--r--   1 root supergroup         29 2017-07-10 10:09 /hello
     */
    @Test
    public void testList() throws IOException {
        Path path = new Path("/");
        FileStatus[] fStatus = fs.listStatus(path);//数组形式
        for(FileStatus f : fStatus) {
            String prefix = "-";
            if(f.isDirectory()) {
                prefix = "d";
            }
            //权限
            FsPermission permission = f.getPermission();
            String up = permission.getUserAction().SYMBOL;
            String gp = permission.getGroupAction().SYMBOL;
            String op = permission.getOtherAction().SYMBOL;
            String p = up + gp + op;
            //备份
            short rep = f.getReplication();
            //文件所属用户
            String ower = f.getOwner();
            //文件所属用户所在组
            String group = f.getGroup();
            //文件大小 单位字节
            long size = f.getLen();
            //文件修改日期
            String midifyStr = df.format(new Date(f.getModificationTime()));
            //文件路径
            String filePath = f.getPath().toString();
            System.out.println(prefix + p + "\t" + rep + "  " + ower + "  " + group + "\t" +
                    size + "\t" + midifyStr + "  " + filePath);
        }
    }

    /**
        读取文件 open
     *  hdfs dfs -text/cat/tail 一个文件
     */
    @Test
    public void testOpen() throws IOException {
        Path path = new Path("/mm/hello");
        FSDataInputStream fis = fs.open(path);
       /**
        *  方式一：通过io流基本操作来获取网络中的数据
        byte[] bytes = new byte[1024];
        int len = -1;
        while((len = fis.read(bytes)) != -1) {
            System.out.println(new String(bytes, 0, len));
        }
        fis.close();
        */
       //方式二：通过hdfs自身api快速搞定
        IOUtils.copyBytes(fis, System.out, 1024);
    }

    /**
        创建目 mkdirs
     *  hdfs dfs -mkdir -p hdfs_path
     */
    @Test
    public void testMkdirs() throws IOException {
        boolean ret = fs.mkdirs(new Path("/input/hadoop/mr"));
        /**
         * 第一个参数expected：预期的结果值
         * 第二个参数actual：真实的结果值
         */
        Assert.assertEquals(true, ret);
    }
//    创建文件 create
    @Test
    public void testCreate() throws IOException {
        FSDataOutputStream fos = fs.create(new Path("/input/hadoop/mr/heihei.txt"));

        Path path = new Path("/hello");
        FSDataInputStream fis = fs.open(path);

        IOUtils.copyBytes(fis, fos, 1024);

        fos.close();
        fis.close();

    }

//    显示文件存储位置getFileBlockLocations
    @Test
    public void testBlockLocation() throws IOException {
        Path p = new Path("/input/hadoop/hdfs/hadoop-2.6.4.tar.gz");
        FileStatus f = fs.getFileStatus(p);

        BlockLocation[] bls = fs.getFileBlockLocations(f, 0, f.getLen());
        for (BlockLocation bl : bls) {
            System.out.println(bl);
        }

    }
//    删除文件或目录 delete

    @After
    public void cleanUp() throws IOException {
        if(fs != null) {
            fs.close();
        }
    }
}
