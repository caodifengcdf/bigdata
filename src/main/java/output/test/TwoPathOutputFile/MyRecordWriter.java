package output.test.TwoPathOutputFile;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream baidu_out;
    FSDataOutputStream other_out;

    public MyRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
        //1.获取文件系统
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());

        //2.创建2个分类的输出流
        //"hdfs://192.168.61.101:9000/hadoop_test/baidu" "hdfs://192.168.61.101:9000/hadoop_test/other"

        baidu_out = fileSystem.create(new Path("hdfs://192.168.61.101:9000/hadoop_test/sougou_2_output/baidu"));
        other_out = fileSystem.create(new Path("hdfs://192.168.61.101:9000/hadoop_test/sougou_2_output/other"));
        //

    }

    //写入数据,根据分类要求，输出至不同目录
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        if (text.toString().contains("baidu")){
            baidu_out.write(text.toString().getBytes());
            baidu_out.write("\r\n".getBytes());//换行，否则写在一行中
        }else {
            other_out.write(text.toString().getBytes());
            baidu_out.write("\r\n".getBytes());
        }
    }
    //关闭流
    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(baidu_out);
        IOUtils.closeStream(other_out);
    }
}
