package popularmovie;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieTitleByReviewCountReducer extends
		Reducer<IntWritable, Text, IntWritable, Text> {
	public void reduce(IntWritable movieId, Iterable<Text> values, Context con)
			throws IOException, InterruptedException {
		int reviewCount = 0;
		String movieName = "";
		for (Text value : values) {
			String[] recordDetails = value.toString().split("\t");
			if (recordDetails[0].equals("review")) {
				reviewCount++;
			} else {
				movieName = recordDetails[1];
			}
		}
		if(reviewCount>0)
			con.write(new IntWritable(reviewCount), new Text(movieName));
	}
}