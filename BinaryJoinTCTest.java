import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


public class BinaryJoinTCTest {
   static final int gridSize = 5;
  
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, NullWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, Text, Text, NullWritable> mapReduceDriver;

  @Before
  public void setUp() {
     BinaryJoinTC.JoinMapper mapper = new BinaryJoinTC.JoinMapper();
     BinaryJoinTC.JoinReducer reducer = new BinaryJoinTC.JoinReducer();
     mapDriver = MapDriver.newMapDriver(mapper);
     reduceDriver = ReduceDriver.newReduceDriver(reducer);
     //mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() throws IOException {
     mapDriver.withInput(new LongWritable(), new Text("A,B"));
     mapDriver.addOutput(new Text("left,B"), new Text("A"));
     mapDriver.addOutput(new Text("right,A"), new Text("B"));
     mapDriver.runTest(false);
  }

  @Test
  public void testReducer() throws IOException {
     ArrayList<Text> list = new ArrayList<Text>();
     list.add(new Text("A"));
     reduceDriver.addInput(new Text("left,B"), list);
     list.clear();
     list.add(new Text("C"));
     reduceDriver.addInput(new Text("right,B"), list);  
     reduceDriver.addOutput(new Text("A,B"), NullWritable.get()); 
     reduceDriver.addOutput(new Text("B,C"), NullWritable.get()); 
     reduceDriver.addOutput(new Text("A,C"), NullWritable.get());  
     reduceDriver.runTest(false);
  }
  
  @Test
  public void testMR() throws IOException{
  }
}