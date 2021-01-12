package input.test.CombineFile;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CombineFileInput extends FileInputFormat {

    //大于128M的小文件，也不分片

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        NoLineRecordReader noLineRecordReader= new NoLineRecordReader ();
        //初始化,将分片与相关配置传入
        noLineRecordReader.initialize(inputSplit,taskAttemptContext);
        return noLineRecordReader;
    }
}
