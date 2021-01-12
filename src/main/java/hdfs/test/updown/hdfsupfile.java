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
        //String source = args[0]; //使用调用参数
        //上传到HDFS目的地
        //String dest = args[1]; //使用调用参数
        //参数：  C:\Users\caodi\Desktop\file_up_down.txt     hdfs://192.168.61.101:9000/test/test.txt
        //根据源文件，创建输入流

        //源文件地址
        String source = "G:\\sougou\\sogou.50w.utf8";
        //需要上传的hdfs地址
        String dest = "hdfs://192.168.61.101:9000/hadoop_test/sougou_data_source/sougou50w";

        try {
            InputStream in = new BufferedInputStream(new FileInputStream(source));//创建输入流，类似python中的open with

            //创建文件系统对象 Java抽象类org.apache.hadoop.fs.FileSystem定义了hadoop的一个文件系统接口。该类是一个抽象类，
            // 通过以下两种静态工厂方法可以过去FileSystem实例：
            //public static FileSystem.get(Configuration conf) throws IOException
            //public static FileSystem.get(URI uri, Configuration conf) throws IOException
            Configuration conf = new Configuration();//使用默认的参数配置
            FileSystem fs = FileSystem.get(URI.create(dest), conf);
            //fs创建输出流
            OutputStream out = fs.create(new Path(dest));
            //流的拷贝及关闭流
            IOUtils.copyBytes(in, out, 4096, true);//3、4参数：一次传输拷贝的字节，传输完成后是否关闭流
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
