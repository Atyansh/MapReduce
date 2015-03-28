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


public class MovieRating extends MapReduceBase
                         implements Mapper<LongWritable, Text, Text, Ratings>,
                                    Reducer<Text, Ratings, Text, Ratings> {

  private String filename;
  
  private static Pattern userRatingDate = Pattern.compile("^(\\d+),(\\d+),(\\d{4})-(\\d{2})-(\\d{2})$");

  public void configure(JobConf job) {
    filename = job.get("map.input.file");
  }
  
  public void map(LongWritable key, Text values,
      OutputCollector<Text, Ratings> output, Reporter reporter) throws IOException {
    
    String line = ((Text)values).toString();
    Matcher userRating = userRatingDate.matcher(line);

    if(filename == null) {
      throw new RuntimeException("PANIC");
    }

    Text movieID = new Text(filename);
    Ratings ratings = new Ratings();
    
    if(line.matches("^\\d+:$")) {
    }
    else if (userRating.matches()) {
      ratings.incr(Integer.parseInt(userRating.group(2)));
      output.collect(movieID, ratings);
    }
    
  }

  public void reduce(Text key, Iterator<Ratings> values, 
      OutputCollector<Text, Ratings>  output, Reporter reporter) throws IOException {
      
      Ratings ratings = new Ratings();
      
      while(values.hasNext()) {
        ratings.incr(values.next());
      }
      
      output.collect(key, ratings);
  }

}
