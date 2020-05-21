import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class SumRatingMovies extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(SumRatingMovies.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new SumRatingMovies(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SumRatingMovies");
        job.setJarByClass(SumRatingMovies.class);

        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SumRatingMapper.class);
        job.setCombinerClass(SumRatingReducer.class);
        job.setReducerClass(SumRatingReducer.class);

//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(SumRatings.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(SumRatings.class);

        job.setOutputFormatClass(OrcStruct);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
