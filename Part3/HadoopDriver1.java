import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

public class HadoopDriver1 {

	public static void main(String[] args) {
		if(args.length != 1 && args.length != 2) {
			System.err.println("Error! Usage: \n" +
					"HadoopDriver1 <input dir> <output dir>\n" +
					"HadoopDriver1 <job.xml>");
			System.exit(1);
		}
		
		JobClient client = new JobClient();
		JobConf conf = null;
		
		if(args.length == 2) {
			conf = new JobConf(HadoopDriver1.class);
			
			conf.setMapOutputKeyClass(IntWritable.class);
			conf.setMapOutputValueClass(IntWritable.class);
			
			conf.setOutputKeyClass(IntWritable.class);
			conf.setOutputValueClass(IntWritable.class);
	
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);
			
			FileInputFormat.setInputPaths(conf, new Path(args[0]));
			FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
			conf.setMapperClass(UserReviewCount.class);
			conf.setCombinerClass(UserReviewCount.class);
			conf.setReducerClass(UserReviewCount.class);
			
			conf.set("mapred.child.java.opts", "-Xmx2048m");
		} else {
			conf = new JobConf(args[0]);
		}
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

