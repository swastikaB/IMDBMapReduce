package topratedmovie;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortByRatingMapper extends
		Mapper<LongWritable, Text, FloatWritable, Text> {
	public void map(LongWritable key, Text txtMovieReviewDetails, Context con)
			throws IOException, InterruptedException {

		String[] arrMovieReviewDetails=txtMovieReviewDetails.toString().split("\t");
		FloatWritable avgRating=new FloatWritable(Float.parseFloat(arrMovieReviewDetails[0]));
		String movieTitle=arrMovieReviewDetails[1].trim();
		String reviewCount=arrMovieReviewDetails[2].trim();
		String record=movieTitle+"\t"+avgRating+"\t"+reviewCount;
		con.write(avgRating,new Text(record));
	}
}
