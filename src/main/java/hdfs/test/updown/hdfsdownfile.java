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
            //源文件地址
            String source = args[0]; //"hdfs://192.168.61.101:9000/hadoop_test/file_up_down.txt"
            //目标地址
            String dest = args[1]; //"/home/kkb/test_file/2.txt" IOUtils.copyBytes没有默认文件名，需要自己指定

            //使用默认的参数
            Configuration conf = new Configuration();

            FileSystem fs = FileSystem.get(URI.create(source),conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(source));

            //本地文件
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(dest));

            IOUtils.copyBytes(hdfsInStream, outputStream, 4096, true);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}