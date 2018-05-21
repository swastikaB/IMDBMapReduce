package popularmovie;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingIntComparator extends WritableComparator{
	//comparator for sorting in descending order of integer
	public DescendingIntComparator()
	{
		super(IntWritable.class,true);
	}
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		IntWritable iw1=(IntWritable) w1;
		IntWritable iw2=(IntWritable) w2;
		return -1 * iw1.compareTo(iw2);
	}

}
