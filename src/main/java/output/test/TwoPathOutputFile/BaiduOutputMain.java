package output.test.TwoPathOutputFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//---windows本地运行报错 Wrong FS .........expected: file:///---
//---原因应该时无法从HDFS上读取文件，需要打包jar包至节点后运行
public class BaiduOutputMain {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration,"BaiduOutput");

        //2.设置输入输入输出路径,调用的类名；map、reduce输出KV的格式
        Path path1 = new Path(args[0]);// "hdfs://192.168.61.101:9000/hadoop_test/sougou_data_clean/part-m-00000"
        Path path2 = new Path(args[1]);//输出success文件的地址"hdfs://192.168.61.101:9000/hadoop_test/sougou_2_output"


        job.setInputFormatClass(TextInputFormat.class);//使用默认的
        job.setOutputFormatClass(TwoPathOutputFile.class);//使用自定义的

        TextInputFormat.setInputPaths(job,path1);
        TwoPathOutputFile.setOutputPath(job,path2);//输出success文件

        job.setMapOutputKeyClass(Text.class);//与reduce（最终输出）类一致，可以省略
        job.setMapOutputValueClass(NullWritable.class);//与reduce（最终输出）类一致，可以省略

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //3.设置jar包类名，map、reduce类名
        job.setJarByClass(BaiduOutputMain.class);

        job.setMapperClass(MyMapper.class);
        //job.setReducerClass(MyReducer.class);//使用默认也没啥问题，自定义解决重复数据

        //4.设置reudcer个数
        job.setNumReduceTasks(1);//默认为1，可以省略

        //运行
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
