import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexJobBigrams {
  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, Text>{
    private Text id = new Text(); 
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String fullDoc = value.toString();
      String[] idTextArr = fullDoc.split("\t",2);
      String docId = idTextArr[0];
      String docText = idTextArr[1];
      docText = docText.replaceAll("[^a-zA-Z]"," ").toLowerCase();
      id.set(docId);
      StringTokenizer itr = new StringTokenizer(docText);
      String firstToken = "";
      if(itr.hasMoreTokens()) {
        firstToken = itr.nextToken();
      }
      while (itr.hasMoreTokens()) {
        String secondToken = "";
        if(itr.hasMoreTokens()){
          secondToken = itr.nextToken();
        }
        if(firstToken != "" && secondToken != ""){
          word.set(firstToken + " " + secondToken);
          context.write(word, id);
        }
        firstToken = secondToken;
      }
    }
  }
  public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Iterator<Text> itr = values.iterator();
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      while (itr.hasNext()) {
        String curr = itr.next().toString();
        map.put(curr, map.getOrDefault(curr, 0) + 1);
      }
      StringBuilder sb = new StringBuilder();
      for (String s: map.keySet()) {
        sb.append(s + ":" + map.get(s) + "\t");
      }
      Text result = new Text(sb.toString());
      context.write(key, result);
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(InvertedIndexJobBigrams.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}