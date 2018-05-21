package topratedmovie;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortByRatingReducer extends
		Reducer<FloatWritable, Text, Text, Text> {
	public void reduce(FloatWritable avgRating, Iterable<Text> values, Context con)
			throws IOException, InterruptedException {
		for (Text value : values) {
			con.write(value, null);
		}
	}
}
