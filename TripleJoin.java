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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/*
 * @author Jade Koskela
 * This class implements a triple join between 3 input relations.
 * For simplification, relations only have 2 columns.
 * Using relations R(A,B) S(B,C) T(C,D), the SQL equivalent:
 *    select r.a, t.d from r,s,t where r.b=s.b and s.c=t.c 
 */
public class TripleJoin {

   public static class JoinMapper extends
         Mapper<LongWritable, Text, Text, Text> {
      Text outKey = new Text(), outValue = new Text();
      String[] keyString;
      String relation;
      int gridDimX, gridDimY; 
      byte relationPosition = 0;

      @Override
      public void setup(Context context) throws IOException,
            InterruptedException {
         super.setup(context);
         Configuration conf = context.getConfiguration();
         FileSplit fs = (FileSplit)context.getInputSplit();
         gridDimX = conf.getInt("gridDimX", 0);
         gridDimY = conf.getInt("gridDimY", 0);
         relation = fs.getPath().getName();
         if(conf.get("left").equals(relation))
            relationPosition |= 1;
         if(conf.get("center").equals(relation))
            relationPosition |= 2;
         if(conf.get("right").equals(relation))
            relationPosition |= 4;
      }

      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         keyString = value.toString().split(",");
         for (int i = 0; i < gridDimY; i++) 
            if((relationPosition & 1) > 0){
               outKey.set(String.format("left,%s,%d", keyString[1], i));
               outValue.set(keyString[0]);
               context.write(outKey, outValue);
            }
         for (int i = 0; i < gridDimX; i++) 
            if((relationPosition & 4) > 0){
               outKey.set(String.format("right,%s,%d", keyString[0], i));
               outValue.set(keyString[1]);
               context.write(outKey, outValue);
            }
         
         if((relationPosition & 2) > 0){
            outKey.set(String.format("center,%s,%s", keyString[0], keyString[1]));
            outValue.set("");
            context.write(outKey, outValue);
         }
      }
   }

   public static class JoinPart extends Partitioner<Text, Text> implements
         Configurable {
      int gridDimX, gridDimY;

      @Override
      public int getPartition(Text key, Text value, int numPartitions) {
         int x, y;
         String[] s = key.toString().split(",");
         if(s[0].equals("left")) {
            x = (s[1].hashCode() & Integer.MAX_VALUE) % gridDimX;
            y = Integer.parseInt(s[2]);
         }
         else if(s[0].equals("center")) {
            x = (s[1].hashCode() & Integer.MAX_VALUE) % gridDimX;
            y = (s[2].hashCode() & Integer.MAX_VALUE) % gridDimY;
         }
         else {
            x = Integer.parseInt(s[2]);
            y = (s[1].hashCode() & Integer.MAX_VALUE) % gridDimY;
         }
         System.out.printf("PARTITION %d: %s %s\n", x*gridDimY+y, key.toString(), value.toString());
         return x*gridDimY + y;
      }
      @Override
      public void setConf(Configuration conf) {
         gridDimX = conf.getInt("gridDimX", 0);
         gridDimY = conf.getInt("gridDimY", 0);
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
         Collection<String> list;
         String edge="%s,%s", hashKey=String.format(edge, k[0],k[1]);
         
         if (k[0].equals("left") && hashMap.containsKey(hashKey)) {
            list = hashMap.removeAll(hashKey);
            for(Text v: values){
               for (String s : list) {
                  hashKey = "right," + s;
                  hashMap.put(hashKey, v.toString());
               }
            }
         }
         if (k[0].equals("center")){
            hashKey = String.format(edge, "left", k[1]);
            hashMap.put(hashKey, k[2]);
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
      Job job = null;
      Configuration conf = new Configuration();
      String left, center, right, output;
      int gridDimX,gridDimY;
      if (args.length != 6) {
         System.out.println("usage: TripleJoinTC relation1 relation2 relation3 Output gridDimX gridDimY");
         System.exit(1);
      }
      left = args[0];
      center = args[1];
      right = args[2];
      output = args[3];
      gridDimX = Integer.parseInt(args[4]);
      gridDimY = Integer.parseInt(args[5]);
      conf.setInt("gridDimX", gridDimX);
      conf.setInt("gridDimY", gridDimY);
      conf.set("left", left);
      conf.set("center", center);
      conf.set("right", right);
      job = new Job(conf);
      job.setNumReduceTasks(gridDimX*gridDimY);
      job.setJarByClass(TripleJoin.class);
      job.setMapperClass(JoinMapper.class);
      job.setReducerClass(JoinReducer.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setPartitionerClass(JoinPart.class);
      FileInputFormat.setInputPaths(job, new Path(left), new Path(center), new Path(right));
      FileOutputFormat.setOutputPath(job, new Path(output));
      job.waitForCompletion(true);
   }
}
