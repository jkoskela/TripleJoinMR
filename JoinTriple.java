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

public class JoinTriple {
   static final Logger sLogger = Logger.getLogger(JoinTriple.class);
   static final Level level = Level.DEBUG;
   static final String log = "hadoop.log";
   static final int _aShare = 5;
   static final int _bShare = 5;
   static final int _numReducer = 25;
   static final String _r1Path = "country.csv";
   static final String _r2Path = "ismember.csv";
   static final String _r3Path = "organization.csv";
   static final String _outPath = "joinTriple";

   public static class JoinMapper extends
         Mapper<LongWritable, Text, Text, Text> {
      String relationName, r1, r2, r3;
      Text outKey = new Text(), outValue = new Text();
      int aShare; // Share of buckets for 1st set of join keys.
      int bShare; // Share of buckets for 2nd set of join keys.

      @Override
      public void setup(Context context) throws IOException,
            InterruptedException {
         super.setup(context);
         Configuration conf = context.getConfiguration();
         FileSplit f = (FileSplit) context.getInputSplit();
         r1 = conf.get("r1Path");
         r2 = conf.get("r2Path");
         r3 = conf.get("r3Path");
         aShare = conf.getInt("aShare", 0);
         bShare = conf.getInt("bShare", 0);
         relationName = f.getPath().getName().toString();
      }

      @Override
      public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
         String[] columns = value.toString().split(",");
         if (relationName.equals(r1)) {
            outValue.set(columns[0]);
            for (int i = 0; i < bShare; i++) {
               outKey.set(String.format("r1,%s,%d", columns[1], i));
               context.write(outKey, outValue);
            }
         } else if (relationName.equals(r2)) {
            outKey.set(String.format("R2,%s,%s", columns[0], columns[1]));
            outValue.set("");
            context.write(outKey, outValue);
         } else {
            outValue.set(columns[1]);
            for (int i = 0; i < aShare; i++) {
               outKey.set(String.format("r3,%d,%s", i, columns[0]));
               context.write(outKey, outValue);
            }
         }
      }
   }

   public static class JoinPart extends Partitioner<Text, Text> implements
         Configurable {
      int aShare; // Share of buckets for 1st set of join keys.
      int bShare; // Share of buckets for 2nd set of join keys.

      @Override
      public int getPartition(Text key, Text value, int numPartitions) {
         int row, col;
         System.out.println("aShare " + aShare);
         System.out.println("bShare " + bShare);
         String[] s = key.toString().split(",");
         if (s[0].equals("r1")) {
            row = (s[1].hashCode() & Integer.MAX_VALUE) % aShare;
            col = Integer.parseInt(s[2]);
         } else if (s[0].equals("R2")) {
            row = (s[1].hashCode() & Integer.MAX_VALUE) % aShare;
            col = (s[2].hashCode() & Integer.MAX_VALUE) % bShare;
         } else {
            row = Integer.parseInt(s[1]);
            col = (s[2].hashCode() & Integer.MAX_VALUE) % bShare;
         }
         sLogger.debug("Partition: " + Integer.toString(row * bShare + col));
         return row * bShare + col;
      }

      @Override
      public void setConf(Configuration conf) {
         aShare = conf.getInt("aShare", 0);
         bShare = conf.getInt("bShare", 0);
      }

      @Override
      public Configuration getConf() {
         return null;
      }
   }

   public static class JoinReducer extends
         Reducer<Text, Text, Text, NullWritable> {
      Multimap<String, StringPair> hashMap = HashMultimap.create();
      Text outKey = new Text();
      Collection<StringPair> list = new ArrayList<>();

      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
         String k = key.toString();
         String[] s = k.split(",");
         String hashKey = String.format("%s,", s[0]);

         if (sLogger.isDebugEnabled()) {
            sLogger.debug(String.format("REDUCE KEY: %s\n", k));
            int i = 0;
            for (Text t : values)
               sLogger
                     .debug(String.format("\t VALUE %d: %s", i++, t.toString()));
         }
         if (s[0].equals("r1") && hashMap.containsKey(hashKey + s[1])) {
            list = hashMap.removeAll(hashKey + s[1]);
            for (Text t : values) {
               // (r1, code, *) (columns) -> (r3,abbv) (code r1columns,r2columns)
               for (StringPair hashValue : list) {
                  hashKey = String.format("%s,%s", "r3", hashValue.s1);
                  hashValue.s1 = s[1];
                  hashValue.s2 = String.format("%s,%s", t.toString(),
                        hashValue.s2);
                  hashMap.put(hashKey, hashValue);
               }
            }
         }
         if (s[0].equals("R2"))
            for (Text t : values) {
               // (R2, code, abbv) (columns) -> (r1,code) (abbv r2columns)
               hashKey = String.format("%s,%s", "r1", s[1]);
               StringPair hashValue = new StringPair(s[2], t.toString());
               hashMap.put(hashKey, hashValue);
            }
         if (s[0].equals("r3") && hashMap.containsKey(hashKey + s[2])) {
            list = hashMap.removeAll(hashKey + s[2]);
            for (Text t : values) {
               // (r3, *, abbv) -> (code, abbv, r1columns, r2columns, r3columns)
               for (StringPair hashValue : list) {
                  k = String.format("%s,%s,%s,%s", hashValue.s1, s[2],
                        hashValue.s2, t.toString());
                  outKey.set(k);
                  context.write(outKey, NullWritable.get());
               }
            }
         }
      }
   }

   public static class StringPair {
      public String s1;
      public String s2;

      public StringPair(String s1, String s2) {
         this.s1 = s1;
         this.s2 = s2;
      }
   }

   public static void main(String[] args) throws Exception {
      Job job = null;
      Configuration conf = new Configuration();
      conf.set("r1Path", _r1Path);
      conf.set("r2Path", _r2Path);
      conf.set("r3Path", _r3Path);
      conf.setInt("aShare", _aShare);
      conf.setInt("bShare", _bShare);
      job = new Job(conf);
      job.setNumReduceTasks(_numReducer);
      job.setJarByClass(JoinTriple.class);
      job.setMapperClass(JoinMapper.class);
      job.setReducerClass(JoinReducer.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputValueClass(NullWritable.class);
      job.setOutputKeyClass(Text.class);
      job.setPartitionerClass(JoinPart.class);
      FileInputFormat.setInputPaths(job, new Path(_r1Path), new Path(_r2Path),
            new Path(_r3Path));
      FileOutputFormat.setOutputPath(job, new Path(_outPath));
      sLogger.setLevel(level);
      sLogger.setAdditivity(false);
      sLogger.addAppender(new FileAppender(new PatternLayout(), log));
      job.waitForCompletion(true);
   }
}
