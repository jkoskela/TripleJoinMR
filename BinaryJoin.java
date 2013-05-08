import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * 
 * @author Jade Koskela 
 * 
 * This class implements a triple equi-join as two binary joins.
 * Input should be edges of the format (u,v), one per line. 
 * 
 */
public class BinaryJoin extends Configured implements Tool{
   static final String tempDir = "binaryJoin_temp";

   public static class JoinMapper extends
         Mapper<LongWritable, Text, Text, Text> {
      Text outKey = new Text();
      Text outValue = new Text();
      byte relationPosition = 0;
      String[] tuple;
      
      @Override
      public void setup(Context context) throws IOException,
            InterruptedException {
         super.setup(context);
         String relation;
         Configuration conf = context.getConfiguration();
         FileSplit fs = (FileSplit)context.getInputSplit();
         relation = fs.getPath().getName();
         if(relation.contains("part-r"))
            relation = fs.getPath().getParent().getName();
         if(conf.get("left").equals(relation))
            relationPosition |= 1;
         if(conf.get("right").equals(relation))
            relationPosition |= 2;
      } 
      
      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         tuple = value.toString().split(",");
         if((relationPosition & 1) > 0){
            outKey.set(String.format("left,%s", tuple[1]));
            outValue.set(tuple[0]);
            context.write(outKey, outValue);
         }
         if((relationPosition & 2) > 0){
            outKey.set(String.format("right,%s", tuple[0]));
            outValue.set(tuple[1]);
            context.write(outKey, outValue);
         }
      }
   }

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
            for(Text t : values){
               valueString = t.toString();
               hashMap.put(k[1], valueString);
            }
         }
         else if(k[0].equals("right")){  
            list = hashMap.get(k[1]);
            for(Text t : values){
               valueString = t.toString();
               for(String s: list){
                  outKey.set(s + ',' + valueString);
                  context.write(outKey, outValue);
               }
            }
         }
      }
   }
   
   public int run(String[] args) throws Exception {
      boolean result = false;
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      String r1, r2, r3, output;
      int numReducer;
      if (args.length != 5) {
         System.err.println("usage: BinaryJoin relation1 relation2 relation3 output numReducers");
         ToolRunner.printGenericCommandUsage(System.err);
         return -1;
      }
      r1 = args[0];
      r2 = args[1];
      r3 = args[2];
      output = args[3];
      numReducer = Integer.parseInt(args[4]);
      fs.delete(new Path(tempDir), true);
      
      result = joinStep(r1, r2, tempDir, numReducer);
      if(!result) return -1;
      result = joinStep(tempDir, r3, output, numReducer);
      if(!result) return -1;
      return 0;
   }
   
   public boolean joinStep(String left, String right, String output, int numReducer) throws Exception{
      Job job = null;
      Configuration conf = new Configuration();
      conf.set("left", left);
      conf.set("right", right);
      job = new Job(conf);
      job.setNumReduceTasks(numReducer);
      job.setJarByClass(BinaryJoin.class);
      job.setMapperClass(JoinMapper.class);
      job.setMapOutputValueClass(Text.class);
      job.setReducerClass(JoinReducer.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setPartitionerClass(JoinPart.class);
      FileInputFormat.setInputPaths(job, new Path(left), new Path(right));
      FileOutputFormat.setOutputPath(job, new Path(output));
      return job.waitForCompletion(true);
   }
   
   public static void main(String[] args) throws Exception {
      int exitCode = ToolRunner.run(new BinaryJoin(), args);
      System.exit(exitCode);
   }
}
