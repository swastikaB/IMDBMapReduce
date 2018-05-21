package popularmovie;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieTitleByMovieIdMapper extends
		Mapper<LongWritable, Text, IntWritable, Text> {
	public void map(LongWritable key, Text value, Context con)
			throws IOException, InterruptedException {
		if (key.get() == 0 && value.toString().contains("movieId"))
			return;
		String line = value.toString();
		String[] movieDetails = line.split(",");
		if (movieDetails != null && movieDetails.length > 2) {
			String strMovieId = movieDetails[0];
			String genres = movieDetails[movieDetails.length - 1];
			String movieTitle = "";
			if (movieDetails.length > 3) {
				// If the movie title contains "," then get the movieTitle by taking the substring from the entire line
				//This movieTitle will be enclosed in "" so removing the quotes also while taking the substring.
				movieTitle = line.substring(strMovieId.length() + 2,
						line.length() - genres.length() - 2);
			} else {
				// If movie title does not contain ","
				movieTitle = movieDetails[1];
			}

			Text outputMovieTitle = new Text("movie" + "\t" + movieTitle.trim());
			IntWritable movieId = new IntWritable(Integer.parseInt(strMovieId
					.trim()));
			con.write(movieId, outputMovieTitle);
		}
	}
}
