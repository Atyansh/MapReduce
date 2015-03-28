import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;


public class NoRating extends MapReduceBase
                      implements Mapper<LongWritable, Text, IntWritable, IntWritable>,
                                 Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

  private static Pattern userRatingDate = Pattern.compile("^([^\\t]+)\t(\\d+)\t(\\d+)\t(\\d+)\t(\\d+)\t(\\d+)$");

  public void map(LongWritable key, Text values,
      OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
    
    String line = ((Text)values).toString();
    Matcher userRating = userRatingDate.matcher(line);

    IntWritable count = new IntWritable(1);
    IntWritable star1 = new IntWritable(1);
    IntWritable star5 = new IntWritable(5);
    
    if (userRating.matches()) {
      int star1Count = Integer.parseInt(userRating.group(2));
      int star5Count = Integer.parseInt(userRating.group(6));

      if(star1Count == 0) {
        output.collect(star1, count);
      }

      if(star5Count == 0) {
        output.collect(star5, count);
      }
    }
    else {
      throw new RuntimeException("REGEX FAIL");
    }
    
  }

  public void reduce(IntWritable key, Iterator<IntWritable> values, 
      OutputCollector<IntWritable, IntWritable>  output, Reporter reporter) throws IOException {
      
      int count = 0;
      
      while(values.hasNext()) {
        count += values.next().get();
      }
      
      output.collect(key, new IntWritable(count));
  }

}
