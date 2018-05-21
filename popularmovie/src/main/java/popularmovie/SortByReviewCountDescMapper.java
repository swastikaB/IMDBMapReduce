package popularmovie;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortByReviewCountDescMapper extends
		Mapper<Text, Text, IntWritable, Text> {
	public void map(Text count, Text movieTitle, Context con)
			throws IOException, InterruptedException {

		con.write(new IntWritable(Integer.parseInt(count.toString())),
				movieTitle);
	}
}
