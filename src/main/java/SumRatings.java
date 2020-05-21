import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumRatings implements WritableComparable<SumRatings> {

    IntWritable count = new IntWritable(0);
    DoubleWritable sum = new DoubleWritable(0);

    public SumRatings() {
    }

    public SumRatings(double sum, int count) {
        set(new DoubleWritable(sum), new IntWritable(count));
    }

    public void set(DoubleWritable sum, IntWritable count) {
        this.sum = sum;
        this.count = count;
    }

    public void addSumRating(SumRatings sumRating) {
        set(new DoubleWritable(this.sum.get() + sumRating.sum.get()),
                new IntWritable(this.count.get() + sumRating.count.get()));
    }

    @Override
    public String toString() {
        return sum.get()+", "+count.get();
    }

    @Override
    public int compareTo(SumRatings sumRatings) {
        int comparison = this.sum.compareTo(sumRatings.sum);
        if(comparison != 0)
            return comparison;
        return this.count.compareTo(sumRatings.count);
    }

    @Override
    public boolean equals(Object o) {
        if(this == o)
            return true;
        if(o == null || o.getClass() != this.getClass())
            return false;
        SumRatings sumRatings = (SumRatings) o;
        return count.equals(sumRatings.count) && sum.equals(sumRatings.sum);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        sum.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        sum.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        int sumHash = this.sum.hashCode();
        int countHash = this.count.hashCode();
        return 31 * sumHash + countHash;
    }
}
