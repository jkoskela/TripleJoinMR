import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TripleJoinTest {
   static final int gridDim = 5;
  
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, NullWritable> reduceDriver;
  MapReduceDriver<LongWritable, Text, Text, NullWritable, Text, NullWritable> mapReduceDriver;

  @Before
  public void setUp() {
     TripleJoin.JoinMapper mapper = new TripleJoin.JoinMapper();
     TripleJoin.JoinReducer reducer = new TripleJoin.JoinReducer();
     mapDriver = MapDriver.newMapDriver(mapper);
     reduceDriver = ReduceDriver.newReduceDriver(reducer);
     Configuration conf = mapDriver.getConfiguration();
     conf.setInt("gridDim", gridDim);
     conf.set("left","leftRelation");
     conf.set("right","rightRelation");
     conf.set("center","centerRelation");
  }

  @Test
  public void testMapperLeft() throws IOException {
     mapDriver.setMapInputPath(new Path("leftRelation"));
     mapDriver.withInput(new LongWritable(), new Text("A,B"));
     for(int i=0; i<gridDim; i++)
        mapDriver.addOutput(new Text("left,B,"+i), new Text("A"));
     mapDriver.runTest(false);
  }
  @Test
  public void testMapperRight() throws IOException {
     mapDriver.setMapInputPath(new Path("rightRelation"));
     mapDriver.withInput(new LongWritable(), new Text("A,B"));
     for(int i=0; i<gridDim; i++)
        mapDriver.addOutput(new Text("right,A,"+i), new Text("B"));
     mapDriver.runTest(false);
  }
  @Test
  public void testMapperCenter() throws IOException {
     mapDriver.setMapInputPath(new Path("centerRelation"));
     mapDriver.withInput(new LongWritable(), new Text("A,B"));
     mapDriver.addOutput(new Text("center,A,B"), new Text(""));
     mapDriver.runTest(false);
  }

  @Test
  public void testReducer() throws IOException {
     ArrayList<Text> list = new ArrayList<Text>();
     list.add(new Text(""));
     reduceDriver.addInput(new Text("center,2032,2511"), new ArrayList<Text>(list));
     list.clear();
     list.add(new Text("2511"));
     reduceDriver.addInput(new Text("left,2021,1"), new ArrayList<Text>(list));
     list.clear();
     list.add(new Text("136"));
     list.add(new Text("1955"));
     list.add(new Text("2498"));
     reduceDriver.addInput(new Text("left,2032,1"), new ArrayList<Text>(list));
     list.clear();
     list.add(new Text("2021"));
     list.add(new Text("2598"));
     reduceDriver.addInput(new Text("right,2511,1"), new ArrayList<Text>(list));
     reduceDriver.addOutput(new Text("136,2021"), NullWritable.get());
     reduceDriver.addOutput(new Text("136,2598"), NullWritable.get());
     reduceDriver.addOutput(new Text("1955,2021"), NullWritable.get());
     reduceDriver.addOutput(new Text("1955,2598"), NullWritable.get());
     reduceDriver.addOutput(new Text("2498,2021"), NullWritable.get());
     reduceDriver.addOutput(new Text("2498,2598"), NullWritable.get());
     reduceDriver.runTest(false);
  }
  
  @Test
  public void testMR() throws IOException{
  }
}