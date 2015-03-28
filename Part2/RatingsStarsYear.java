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

public class RatingsStarsYear extends MapReduceBase
implements Mapper<LongWritable,Text,YearRating,IntWritable>,
Reducer<YearRating,IntWritable,YearRating,IntWritable> {
	
  private static Pattern userRatingDate = Pattern.compile("^(\\d+),(\\d+),(\\d{4})-(\\d{2})-(\\d{2})$");
	
	public void map(LongWritable key, Text values,
			OutputCollector<YearRating, IntWritable> output, Reporter reporter) throws IOException {
		
		String line = ((Text)values).toString();
		Matcher userRating = userRatingDate.matcher(line);

		YearRating yearRating = new YearRating();
		IntWritable count = new IntWritable(1);
		
		if(line.matches("^\\d+:$")) {
		}
    else if (userRating.matches()) {
			int year = Integer.parseInt(userRating.group(3));
			int rating = Integer.parseInt(userRating.group(2));

      yearRating.set(year,rating);
			
			output.collect(yearRating, count);
		}
		
	}

	public void reduce(YearRating key, Iterator<IntWritable> values, 
			OutputCollector<YearRating, IntWritable> output, Reporter reporter) throws IOException {
      
      int count = 0;

      while(values.hasNext()) {
        count += values.next().get();
      }

      output.collect(key, new IntWritable(count));
	}

}
