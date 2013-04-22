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
         Mapper<LongWritable, Text, Text, NullWritable> {
      Text outKey = new Text();
      String outKeyString;
      NullWritable outValue = NullWritable.get();
      int gridSize;

      @Override
      public void setup(Context context) throws IOException,
            InterruptedException {
         super.setup(context);
         Configuration conf = context.getConfiguration();
         gridSize = conf.getInt("gridSize", 0);
      }

      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         outKeyString = value.toString();
         for (int i = 0; i < gridSize; i++) {
            outKey.set(String.format("left,%s,%d", outKeyString, i));
            context.write(outKey, outValue);
            outKey.set(String.format("right,%s,%d", outKeyString, i));
            context.write(outKey, outValue);
         }
         outKey.set(String.format("center,%s,$", outKeyString));
         context.write(outKey, outValue);
      }
   }

   public static class JoinPart extends Partitioner<Text, NullWritable> implements
         Configurable {
      int gridSize;

      @Override
      public int getPartition(Text key, NullWritable value, int numPartitions) {
         int row, col;
         String[] s = key.toString().split(",");
         if (s[0].equals("left")) {
            row = (s[2].hashCode() & Integer.MAX_VALUE) % gridSize;
            col = Integer.parseInt(s[3]);
         }
         else if (s[0].equals("center")) {
            row = (s[1].hashCode() & Integer.MAX_VALUE) % gridSize;
            col = (s[2].hashCode() & Integer.MAX_VALUE) % gridSize;
         }
         else {
            row = Integer.parseInt(s[3]);
            col = (s[1].hashCode() & Integer.MAX_VALUE) % gridSize;
         }
         sLogger.debug("Partition: " + Integer.toString(row*gridSize + col));
         return row*gridSize + col;
      }
      @Override
      public void setConf(Configuration conf) {
         gridSize = conf.getInt("gridSize", 0);
      }
      @Override
      public Configuration getConf() {
         return null;
      }
   }

   public static class JoinReducer extends
         Reducer<Text, NullWritable, Text, NullWritable> {
      Multimap<String, String> hashMap = HashMultimap.create();
      Text outKey = new Text();
      NullWritable outValue = NullWritable.get();

      @Override
      public void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
         String[] k = key.toString().split(",");
         Collection<String> list = new ArrayList<String>();
         String hashKey, edge="%s,%s";

         if (k[0].equals("left") && hashMap.containsKey(hashKey="left,"+k[2])) {
            list = hashMap.removeAll(hashKey);
            for (String s : list) {
               hashKey = String.format(edge, "right", s);
               hashMap.put(hashKey, k[1]);
               outKey.set(String.format(edge, k[1],s));
               context.write(outKey, outValue);
            }
         }
         if (k[0].equals("center")){
            hashKey = String.format(edge, "left", k[1]);
            hashMap.put(hashKey, k[2]);
            outKey.set(String.format(edge, k[1], k[2]));
            context.write(outKey, outValue);
         }
         if (k[0].equals("right") && hashMap.containsKey(hashKey="right,"+ k[1])) {
            list = hashMap.removeAll(hashKey);
            for (String s : list) {
               outKey.set(String.format(edge, s,k[2]));
               context.write(outKey, outValue);
            }
         }
      }
   }

   public static void main(String[] args) throws Exception {
      // TripleJoinTC input output gridSize
      Job job = null;
      Configuration conf = new Configuration();
      String input, output;
      int gridSize;
      if (args.length < 3) {
         System.out.println("usage: TripleJoinTC input output gridSize");
         System.exit(1);
      }
      input = args[0];
      output = args[1];
      gridSize = Integer.parseInt(args[2]);
      conf.setInt("gridSize", gridSize);
      job = new Job(conf);
      job.setNumReduceTasks((int) Math.pow(gridSize, 2));
      job.setJarByClass(TripleJoinTC.class);
      job.setMapperClass(JoinMapper.class);
      job.setReducerClass(JoinReducer.class);
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
