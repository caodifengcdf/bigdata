package sort.test.secondsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import output.test.TwoPathOutputFile.TwoPathOutputFile;

import java.io.IOException;

public class SecondSortMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        ToolRunner.run(configuration,new SecondSortMain(),args);

    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration,"SecondSort");

        //2.设置输入输入输出路径,调用的类名；map、reduce输出KV的格式
        Path path1 = new Path(args[0]);
        Path path2 = new Path(args[1]);


        job.setInputFormatClass(TextInputFormat.class);//使用默认的
        job.setOutputFormatClass(TextOutputFormat.class);//使用默认的

        TextInputFormat.setInputPaths(job,path1);
        TextOutputFormat.setOutputPath(job,path2);

        job.setMapOutputKeyClass(Person.class);//与reduce（最终输出）类一致，可以省略
        job.setMapOutputValueClass(NullWritable.class);//与reduce（最终输出）类一致，可以省略

        job.setOutputKeyClass(Person.class);
        job.setOutputValueClass(NullWritable.class);

        //3.设置jar包类名，map、reduce类名
        job.setJarByClass(SecondSortMain.class);

        job.setMapperClass(SecondSortMapper.class);
        job.setReducerClass(SecondSortReducer.class);//使用默认也没啥问题，自定义解决重复数据

        //4.设置reudcer个数
        job.setNumReduceTasks(1);//默认为1，可以省略

        //运行
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

        return 0;
    }

    public static class SecondSortMapper extends Mapper<LongWritable, Text ,Person, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Person person = new Person();
        //提取name、age、salary赋值给person对象
        String[] datas = value.toString().split("\t");
        person.setName(datas[0]);
        person.setAge(Integer.parseInt(datas[1]));
        person.setSalary(Integer.parseInt(datas[2]));
        context.write(person,NullWritable.get());
        }
    }

    public static class SecondSortReducer extends Reducer<Person, NullWritable,Person, NullWritable>{
        @Override
        protected void reduce(Person key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
        }
    }
}
