package popularmovie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;



public class PopularMovie {

	public static void main(String[] args) throws Exception {
		// Get input argument and setup configuration
		Configuration config = new Configuration();
		String[] files = new GenericOptionsParser(config, args)
				.getRemainingArgs();

		/*-----Job 1------*/
		// setup mapreduce job
		Job job1 = new Job(config, "Popular Movies");
		job1.setJarByClass(PopularMovie.class);
		
		// Retrieve input/output paths from the command line arguments
		Path inputReview = new Path(files[0]);
		Path inputMovie = new Path(files[1]);
		Path intermediatePath = new Path("temp1/");
		Path output = new Path(files[2]);

		//Set up mappers (inputformat class, input path and mapper class)
		MultipleInputs.addInputPath(job1, inputReview, TextInputFormat.class,
				ReviewCountByMovieIdMapper.class);
		MultipleInputs.addInputPath(job1, inputMovie, TextInputFormat.class,
				MovieTitleByMovieIdMapper.class);
		
		// set intermediate output key value class
		job1.setMapOutputKeyClass(IntWritable.class);
		job1.setMapOutputValueClass(Text.class);

		//Setup reducer
		job1.setReducerClass(MovieTitleByReviewCountReducer.class);
		
		//Set Reducer output key and value class
		job1.setOutputValueClass(Text.class);
		job1.setOutputKeyClass(IntWritable.class);

		//set output path
		FileOutputFormat.setOutputPath(job1, intermediatePath);

		// task completion
		job1.waitForCompletion(true);
		/*-----Job 1 ends------*/

		/*-----Job 2------*/
		Job job2 = new Job(config, "Sort Movies");
		job2.setJarByClass(PopularMovie.class);

		//Set up mapper
		job2.setMapperClass(SortByReviewCountDescMapper.class);
		
		//set input format class 
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//set input path
		TextInputFormat.addInputPath(job2, intermediatePath);
		
		// set intermediate output key value class
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		//set SortComaparator class for descending review count
		job2.setSortComparatorClass(DescendingIntComparator.class);
		
		//set output path
		TextOutputFormat.setOutputPath(job2, output);
		
		//task completion
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		/*-----Job 2 ends------*/

	}

}
