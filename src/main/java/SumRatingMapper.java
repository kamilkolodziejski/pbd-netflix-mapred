import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class SumRatingMapper extends Mapper<LongWritable, Text, IntWritable, SumRatings> {

    private static final Logger LOG = Logger.getLogger(SumRatingMapper.class);

    public void map(LongWritable key, Text value, Context context) {
        final IntWritable id = new IntWritable();
        final DoubleWritable rate = new DoubleWritable();
        final SumRatings result = new SumRatings();

        try {
            if (key.get() != 0) {
                String line = value.toString();

                int i = 0;

                for (String field : line
                        .split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
                    if (i == 1) {
                        id.set(Integer.parseInt(field));
                    }
                    if (i == 3) {
                        rate.set(Integer.parseInt(field));
                    }
                    i++;
                }
                result.set(rate, new IntWritable(1));
                context.write(id, result);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
