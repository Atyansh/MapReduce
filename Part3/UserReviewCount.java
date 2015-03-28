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


public class UserReviewCount extends MapReduceBase
                         implements Mapper<LongWritable, Text, IntWritable, IntWritable>,
                                    Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
  
  private static Pattern userRatingDate = Pattern.compile("^(\\d+),(\\d+),(\\d{4})-(\\d{2})-(\\d{2})$");
  
  public void map(LongWritable key, Text values,
      OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
    
    String line = ((Text)values).toString();
    Matcher userRating = userRatingDate.matcher(line);

    IntWritable userID = new IntWritable();
    IntWritable count = new IntWritable(1);
    
    if(line.matches("^\\d+:$")) {
    }
    else if (userRating.matches()) {
      userID.set(Integer.parseInt(userRating.group(1)));
      output.collect(userID, count);
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
