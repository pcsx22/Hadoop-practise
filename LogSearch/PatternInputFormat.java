import java.io.*;
import java.util.regex.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class PatternInputFormat
        extends FileInputFormat<LongWritable,Text>{
 
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split,
            TaskAttemptContext context)
                           throws IOException,
                      InterruptedException {
 
        return new PatternRecordReader();
    }
 
}
