package topratedmovie;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MovieReviewDetail implements Writable{
	
	Text movieTitle;
	IntWritable reviewCount;
	public MovieReviewDetail(String movieTitle,int reviewCount)
	{
		this.reviewCount=new IntWritable(reviewCount);
		this.movieTitle=new Text(movieTitle);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		movieTitle.readFields(in);
		reviewCount.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		movieTitle.write(out);
		reviewCount.write(out);
		
	}
	

}
