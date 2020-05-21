import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SumRatingReducer extends Reducer<IntWritable, SumRatings, IntWritable, SumRatings> {

    @Override
    public void reduce(IntWritable key, Iterable<SumRatings> values,
                       Context context) throws IOException, InterruptedException {
        final SumRatings result = new SumRatings();

        for (SumRatings value : values) {
            result.addSumRating(value);
        }

        context.write(key, result);
    }
}
