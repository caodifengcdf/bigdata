package hdfs.test.updown;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class hdfsdownfile {
    public static void main(String[] args) {
        try {
            //源文件
            String srcFile = "hdfs://192.168.61.101:9000/test/test.txt";

            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(URI.create(srcFile),conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(srcFile));

            //本地文件
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream("C:\\Users\\caodi\\Desktop\\1.txt"));

            IOUtils.copyBytes(hdfsInStream, outputStream, 4096, true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}