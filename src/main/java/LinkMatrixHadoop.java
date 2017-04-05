import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Created by zuston on 17/4/4.
 */
public class LinkMatrixHadoop {
    public static final Logger log = LoggerFactory.getLogger(LinkMatrixHadoop.class);

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable count = new IntWritable(1);
        private Text word = new Text();
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizerArticle = new StringTokenizer(value.toString(), "\n");
            while(tokenizerArticle.hasMoreTokens()){
                StringTokenizer rightArr = new StringTokenizer(tokenizerArticle.nextToken(),"++");
                rightArr.nextToken();
                if (rightArr.countTokens()<=1){
                    continue;
                }
                StringTokenizer tokenizer = new StringTokenizer(rightArr.nextToken(),"--");
                ArrayList<String> list = new ArrayList<String>();
                while (tokenizer.hasMoreTokens()){
                    list.add(tokenizer.nextToken());
                }
                for (int i=0;i<list.size();i++){
                    for (int j=i+1;j<list.size();j++){
                        if (list.get(i).compareTo(list.get(j))>0){
                            word.set(list.get(i)+" "+list.get(j));
                        }else{
                            word.set(list.get(j)+" "+list.get(i));
                        }
                        context.write(word,count);
                    }
                }

            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LinkMatrix");
        job.setJarByClass(LinkMatrixHadoop.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        boolean isSuccess = job.waitForCompletion(true);

        System.exit(isSuccess ? 0 : 1);
    }
}
