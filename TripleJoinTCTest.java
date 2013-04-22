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


public class TripleJoinTCTest {
   static final int gridSize = 5;
  
  MapDriver<LongWritable, Text, Text, NullWritable> mapDriver;
  ReduceDriver<Text, NullWritable, Text, NullWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, NullWritable, Text, NullWritable> mapReduceDriver;

  @Before
  public void setUp() {
     TripleJoinTC.JoinMapper mapper = new TripleJoinTC.JoinMapper();
     TripleJoinTC.JoinReducer reducer = new TripleJoinTC.JoinReducer();
     mapDriver = MapDriver.newMapDriver(mapper);
     reduceDriver = ReduceDriver.newReduceDriver(reducer);
     mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
     Configuration conf = mapDriver.getConfiguration();
     conf.setInt("gridSize", gridSize);
     conf = mapReduceDriver.getConfiguration();
     conf.setInt("gridSize", gridSize);
  }

  @Test
  public void testMapper() throws IOException {
     mapDriver.withInput(new LongWritable(), new Text("A,B"));
     for(int i=0; i<gridSize; i++)
        mapDriver.addOutput(new Text("left,A,B,"+i), NullWritable.get());
     mapDriver.addOutput(new Text("center,A,B,$"), NullWritable.get());
     for(int i=0; i<gridSize; i++)
        mapDriver.addOutput(new Text("right,A,B,"+i), NullWritable.get());
     mapDriver.runTest(false);
  }

  @Test
  public void testReducer() throws IOException {
     ArrayList<NullWritable> list = new ArrayList<NullWritable>();
     list.add(NullWritable.get());
     reduceDriver.addInput(new Text("center,B,C,1"), list);
     reduceDriver.addInput(new Text("left,A,B,1"), list);
     reduceDriver.addInput(new Text("right,C,D,1"), list);  
     reduceDriver.addOutput(new Text("B,C"), NullWritable.get()); //Only re-emit center.
     reduceDriver.addOutput(new Text("A,C"), NullWritable.get()); 
     reduceDriver.addOutput(new Text("A,D"), NullWritable.get());  //Don't emit join center-right.
     reduceDriver.runTest(false);
  }
  
  @Test
  public void testMR() throws IOException{
  }
}