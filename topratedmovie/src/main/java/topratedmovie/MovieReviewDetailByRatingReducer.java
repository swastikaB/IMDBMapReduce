package topratedmovie;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReviewDetailByRatingReducer extends
		Reducer<IntWritable, Text, FloatWritable, Text> {
	public void reduce(IntWritable movieId, Iterable<Text> values, Context con)
			throws IOException, InterruptedException {
		int reviewCount = 0;
		float ratingSum=0.0f;
		String movieName = "";
		for (Text value : values) {
			String[] recordDetails = value.toString().split("\t");
			if (recordDetails[0].equals("review")) {
				reviewCount++;
				ratingSum+=Float.parseFloat(recordDetails[1].toString().trim());
			} else {
				movieName = recordDetails[1];
			}
		}
		float avgRating=0.0f;
		if(reviewCount>10)
			avgRating=ratingSum/reviewCount;
		if(avgRating > 4)
			con.write(new FloatWritable(avgRating), new Text(movieName + '\t' +reviewCount));
	}
}