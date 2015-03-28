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


public class RatingsDate extends MapReduceBase
                         implements Mapper<LongWritable, Text, Text, IntWritable>,
                                    Reducer<Text, IntWritable, Text, IntWritable> {
  
  private static Pattern userRatingDate = Pattern.compile("^(\\d+),(\\d+),(\\d{4})-(\\d{2})-(\\d{2})$");
  
  public void map(LongWritable key, Text values,
      OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    
    String line = ((Text)values).toString();
    Matcher userRating = userRatingDate.matcher(line);

    Text date = new Text();

    IntWritable dateCount = new IntWritable(1);
    
    if(line.matches("^\\d+:$")) {
      /* This is the Movie ID line. Ignore it */
    }
    else if (userRating.matches()) {
      String year = userRating.group(3);
      String month = userRating.group(4);
      String day = userRating.group(5);

      date.set(year + "-" + month + "-" + day);

      output.collect(date, dateCount);
      
    }
    
  }

  public void reduce(Text key, Iterator<IntWritable> values, 
      OutputCollector<Text, IntWritable>  output, Reporter reporter) throws IOException {

      int dateCount = 0;
      
      while(values.hasNext()) {
        dateCount += values.next().get();
      }
      
      output.collect(key, new IntWritable(dateCount));
  }

}
