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


public class JoinTripleMondialTest {
  static final int _aShare     = 5;
  static final int _bShare     = 5;
  static final int _numReducer = 25;
  static final String _r1Path  = "country.csv";
  static final String _r2Path  = "ismember.csv";
  static final String _r3Path  = "organization.csv";
  static final String _outPath = "joinTriple";
  
  MapDriver<LongWritable, Text, Text, Text> mapDriver;
  ReduceDriver<Text, Text, Text, NullWritable> reduceDriver;
  //MapReduceDriver<LongWritable, Text, MatrixMult.IntTriple, IntWritable, Text, Text> mapReduceDriver;

  @Before
  public void setUp() {
     JoinTripleMondial.JoinMapper mapper = new JoinTripleMondial.JoinMapper();
     JoinTripleMondial.JoinReducer reducer = new JoinTripleMondial.JoinReducer();
     mapDriver = MapDriver.newMapDriver(mapper);
     mapDriver.setMapInputPath(new Path("country.csv"));
     Configuration conf = mapDriver.getConfiguration();
     conf.set("r1Path", _r1Path);
     conf.set("r2Path", _r2Path);
     conf.set("r3Path", _r3Path);
     conf.setInt("aShare", _aShare);
     conf.setInt("bShare", _bShare);
     reduceDriver = ReduceDriver.newReduceDriver(reducer);
  }

  @Test
  public void testMapper() throws IOException {
     mapDriver.withInput(new LongWritable(), new Text("Albania,AL,Tirane,Albania,28750,3249136"));
     for(int i=0; i<5; i++)
        mapDriver.addOutput(new Text("r1,AL," + i), new Text("Albania"));
     mapDriver.runTest();
  }

  @Test
  public void testReducer() throws IOException {
     ArrayList<Text> list = new ArrayList<Text>();
     list.add(new Text(""));
     reduceDriver.addInput(new Text("R2,US,N"), new ArrayList<Text>(list));
     reduceDriver.addInput(new Text("R2,CA,N"), new ArrayList<Text>(list));
     reduceDriver.addInput(new Text("R2,US,UN"), new ArrayList<Text>(list));
     list.clear();
     list.add(new Text("United States"));
     reduceDriver.addInput(new Text("r1,US,1"), new ArrayList<Text>(list));
     list.clear();
     list.add(new Text("Canada"));
     reduceDriver.addInput(new Text("r1,CA,1"), new ArrayList<Text>(list));
     list.clear();
     list.add(new Text("Nato"));
     reduceDriver.addInput(new Text("r3,1,N"), new ArrayList<Text>(list));
     list.clear();
     list.add(new Text("United Nations"));
     reduceDriver.addInput(new Text("r3,1,UN"), new ArrayList<Text>(list));
     reduceDriver.addOutput(new Text("US,N,United States,,Nato"), NullWritable.get());
     reduceDriver.addOutput(new Text("CA,N,Canada,,Nato"), NullWritable.get());
     reduceDriver.addOutput(new Text("US,UN,United States,,United Nations"), NullWritable.get());
     reduceDriver.runTest(false);
  }
  
  @Test
  public void testMR() {
  }
}