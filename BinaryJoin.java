import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * 
 * @author Jade Koskela 
 * 
 * This class implements a pair of steps in a transitive closure.
 * Input should be edges of the format (u,v), one per line. 
 * 
 */
public class BinaryJoin extends Configured implements Tool{
   static final Logger sLogger = Logger.getLogger(BinaryJoin.class);
   static final Level level = Level.DEBUG;
   static final String log = "hadoop.log";
   static final String tempDir = "doublejointc_temp";

   public static class JoinMapper extends
         Mapper<LongWritable, Text, Text, Text> {
      Text outKey = new Text();
      Text outValue = new Text();
      String[] tuple;
      
      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         tuple = value.toString().split(",");
         outKey.set("left," + tuple[1]);
         outValue.set(tuple[0]);
         context.write(outKey, outValue);
         outKey.set("right," + tuple[0]);
         outValue.set(tuple[1]);
         context.write(outKey, outValue);
      }
   }

   // Partition on join key.
   public static class JoinPart extends Partitioner<Text, Text>{ 
      @Override
      public int getPartition(Text key, Text value, int numPartitions) {
         return (key.toString().split(",")[1].hashCode() & Integer.MAX_VALUE) % numPartitions;
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
         String valueString;
         
         if(k[0].equals("left")){
            // Pass through all existing tuples.
            // Hash all left side tuples. These appear first due to sort order.
            for(Text t : values){
               valueString = t.toString();
               outKey.set(valueString + ',' + k[1]);
               context.write(outKey, outValue);
               hashMap.put(k[1], valueString);
            }
         }
         else if(k[0].equals("right")){  
            // Perform join.
            list = hashMap.get(k[1]);
            for(Text t : values){
               valueString = t.toString();
               for(String s: list){
                  outKey.set(s + ',' + valueString);
                  context.write(outKey, outValue);
               }
            }
         }
         else
            sLogger.debug("Reducer: Bad Key");
      }
   }
   
   public int run(String[] args) throws Exception {
      boolean result = false;
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      String input, output;
      int numReducer;
      if (args.length != 3) {
         System.err.println("usage: DoubleJoinTC input output numReducers");
         ToolRunner.printGenericCommandUsage(System.err);
         return -1;
      }
      input = args[0];
      output = args[1];
      numReducer = Integer.parseInt(args[2]);
      fs.delete(new Path(tempDir), true);
      sLogger.setLevel(level);
      sLogger.setAdditivity(false);
      sLogger.addAppender(new FileAppender(new PatternLayout(), log));
      
      result = joinStep(input, tempDir, numReducer);
      if(!result) return -1;
      result = joinStep(tempDir, output, numReducer);
      if(!result) return -1;
      return 0;
   }
   
   public boolean joinStep(String input, String output, int numReducer) throws Exception{
      Job job = null;
      Configuration conf = new Configuration();
      job = new Job(conf);
      job.setNumReduceTasks(numReducer);
      job.setJarByClass(BinaryJoin.class);
      job.setMapperClass(JoinMapper.class);
      job.setMapOutputValueClass(Text.class);
      job.setReducerClass(JoinReducer.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setPartitionerClass(JoinPart.class);
      FileInputFormat.setInputPaths(job, new Path(input));
      FileOutputFormat.setOutputPath(job, new Path(output));
      return job.waitForCompletion(true);
   }
   
   public static void main(String[] args) throws Exception {
      int exitCode = ToolRunner.run(new BinaryJoin(), args);
      System.exit(exitCode);
   }
}
