import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class TripleJoinTC {
   static final Logger sLogger = Logger.getLogger(TripleJoinTC.class);
   static final Level level = Level.DEBUG;
   static final String log = "hadoop.log";

   public static class JoinMapper extends
         Mapper<LongWritable, Text, Text, Text> {
      Text outKey = new Text(), outValue = new Text();
      String[] keyString;
      int gridDim;

      @Override
      public void setup(Context context) throws IOException,
            InterruptedException {
         super.setup(context);
         Configuration conf = context.getConfiguration();
         gridDim = conf.getInt("gridDim", 0);
      }

      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         keyString = value.toString().split(",");
         for (int i = 0; i < gridDim; i++) {
            outKey.set(String.format("left,%s,%d", keyString[1], i));
            outValue.set(keyString[0]);
            context.write(outKey, outValue);
            outKey.set(String.format("right,%s,%d", keyString[0], i));
            outValue.set(keyString[1]);
            context.write(outKey, outValue);
         }
         outKey.set(String.format("center,%s,%s", keyString[0], keyString[1]));
         outValue.set("");
         context.write(outKey, outValue);
      }
   }

   public static class JoinPart extends Partitioner<Text, Text> implements
         Configurable {
      int gridDim;

      @Override
      public int getPartition(Text key, Text value, int numPartitions) {
         int row, col;
         String[] s = key.toString().split(",");
         if(s[0].equals("left")) {
            row = (s[1].hashCode() & Integer.MAX_VALUE) % gridDim;
            col = Integer.parseInt(s[2]);
         }
         else if(s[0].equals("center")) {
            row = (s[1].hashCode() & Integer.MAX_VALUE) % gridDim;
            col = (s[2].hashCode() & Integer.MAX_VALUE) % gridDim;
         }
         else {
            row = Integer.parseInt(s[2]);
            col = (s[1].hashCode() & Integer.MAX_VALUE) % gridDim;
         }
         sLogger.debug("Partition: " + Integer.toString(row*gridDim + col));
         return row*gridDim + col;
      }
      @Override
      public void setConf(Configuration conf) {
         gridDim = conf.getInt("gridDim", 0);
      }
      @Override
      public Configuration getConf() {
         return null;
      }
   }

   public static class JoinReducer extends
         Reducer<Text, Text, Text, NullWritable> {
      Multimap<String, String> hashMap = HashMultimap.create();
      Text outKey = new Text();
      NullWritable outValue = NullWritable.get();

      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
         String[] k = key.toString().split(",");
         Collection<String> list = new ArrayList<String>();
         String edge="%s,%s", hashKey=String.format(edge, k[0],k[1]);
         
         if (k[0].equals("left") && hashMap.containsKey(hashKey)) {
            list = hashMap.removeAll(hashKey);
            for(Text v: values){
               for (String s : list) {
                  hashKey = "right," + s;
                  hashMap.put(hashKey, v.toString());
                  outKey.set(String.format(edge, v.toString(), s));
                  context.write(outKey, outValue);
               }
            }
         }
         if (k[0].equals("center")){
            hashKey = String.format(edge, "left", k[1]);
            hashMap.put(hashKey, k[2]);
            outKey.set(String.format(edge, k[1], k[2]));
            context.write(outKey, outValue);
         }
         if (k[0].equals("right") && hashMap.containsKey(hashKey)) {
            list = hashMap.removeAll(hashKey);
            for(Text v: values){
               for (String s : list) {
                  outKey.set(String.format(edge, s, v.toString()));
                  context.write(outKey, outValue);
               }
            }
         }
      }
   }

   public static void main(String[] args) throws Exception {
      // TripleJoinTC input output gridDim
      Job job = null;
      Configuration conf = new Configuration();
      String input, output;
      int gridDim;
      if (args.length < 3) {
         System.out.println("usage: TripleJoinTC input output gridDim");
         System.exit(1);
      }
      input = args[0];
      output = args[1];
      gridDim = Integer.parseInt(args[2]);
      conf.setInt("gridDim", gridDim);
      job = new Job(conf);
      job.setNumReduceTasks((int) Math.pow(gridDim, 2));
      job.setJarByClass(TripleJoinTC.class);
      job.setMapperClass(JoinMapper.class);
      job.setReducerClass(JoinReducer.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setPartitionerClass(JoinPart.class);
      FileInputFormat.setInputPaths(job, new Path(input));
      FileOutputFormat.setOutputPath(job, new Path(output));
      sLogger.setLevel(level);
      sLogger.setAdditivity(false);
      sLogger.addAppender(new FileAppender(new PatternLayout(), log));
      job.waitForCompletion(true);
   }
}
