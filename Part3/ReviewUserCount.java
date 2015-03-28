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


public class ReviewUserCount extends MapReduceBase
                         implements Mapper<LongWritable, Text, IntWritable, IntWritable>,
                                    Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
  
  private static Pattern userReview = Pattern.compile("^(\\d+)\t(\\d+)$");
  
  public void map(LongWritable key, Text values,
      OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
    
    String line = ((Text)values).toString();
    Matcher userRating = userReview.matcher(line);

    IntWritable reviewCount = new IntWritable();
    IntWritable count = new IntWritable(1);
    
    if (userRating.matches()) {
      reviewCount.set(Integer.parseInt(userRating.group(2)));
      output.collect(reviewCount, count);
    }
    else {
      System.err.println("\n\nSHOULD NOT HAVE HAPPENED. PANIC!\n\n");
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
