package topratedmovie;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DescendingFloatComparator extends WritableComparator {
	//comparator for sorting in descending order of float
	public DescendingFloatComparator() {
		super(FloatWritable.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		FloatWritable iw1 = (FloatWritable) w1;
		FloatWritable iw2 = (FloatWritable) w2;
		return -1 * iw1.compareTo(iw2);
	}
}
