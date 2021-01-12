package input.test.CombineFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

public class NoLineRecordReader extends RecordReader {
    private FileSplit split;
    private Configuration configuration;
    private boolean precessed = false;
    private BytesWritable value = new BytesWritable();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //当前所读取的分片初始化设置
        this.split = (FileSplit)inputSplit;
        this.configuration= taskAttemptContext.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!precessed){
            //获取分片长度，一次性读取，tip：多行读取时采用NlineInputFormat
            long length = split.getLength();
            //定义一个字节数组，
            byte[] file_contens = new byte[(int)length];
            //获得所属分片的文件路劲，作为Key
            Path file_path = split.getPath();
            //生成文件对象,获得分片信息
            FileSystem fileSystem = file_path.getFileSystem(configuration);
            FSDataInputStream in = fileSystem.open(file_path);

            //将分片信息写入file_contens,之后关闭流
            IOUtils.readFully(in,file_contens,0,(int)length);
            IOUtils.closeStream(in);
            //将所读取的分片内容转为可序列化的BytesWritable
            value.set(file_contens,0,(int)length);
            precessed = true; //作用是只执行一次nextKeyValue，个人理解，不确定。查资料说是每个文件有没有读取过的flag，表示不理解

            return true;
        }

        return false;
    }

    @Override
    public Object getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public Object getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    //获取读取进程
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return precessed ? 1.0f :0.0f; }

    @Override
    public void close() throws IOException {

    }
}
