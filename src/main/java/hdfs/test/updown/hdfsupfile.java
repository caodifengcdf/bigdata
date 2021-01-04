package hdfs.test.updown;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class hdfsupfile {
    public static void main(String[] args) {
        //源文件
        String source = args[0];
        //上传到HDFS目的地
        String dest = args[1];
        //参数：  C:\Users\caodi\Desktop\1.txt     hdfs://192.168.61.101:9000/test/test.txt
        //根据源文件，创建输入流
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(source));
            //创建输出流
            //创建文件系统对象
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dest), conf);
            //fs创建输出流
            OutputStream out = fs.create(new Path(dest));
            //流的拷贝及关闭流
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
