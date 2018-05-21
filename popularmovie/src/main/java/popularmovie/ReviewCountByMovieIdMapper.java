package popularmovie;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewCountByMovieIdMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	public void map(LongWritable key, Text value, Context con)
			throws IOException, InterruptedException {
		if (key.get() == 0 && value.toString().contains("userId"))
			return;
		String line = value.toString();
		String[] reviewDetails = line.split(",");
		if (reviewDetails != null && reviewDetails.length == 4) {
			Text outputReviewCount = new Text("review" + "\t" + 1);
			IntWritable movieId = new IntWritable(
					Integer.parseInt(reviewDetails[1].trim()));
			con.write(movieId, outputReviewCount);
		}
	}
}
