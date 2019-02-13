package net.thumbtack.analyzer.mapreduce;

import net.thumbtack.analyzer.data.WordOccurrencesCountRepository;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.StringTokenizer;

public class SearchQueriesQuantityMapReduceJob {

    public static abstract class LogContentMapper extends Mapper<Object, Text, Text, IntWritable> {

        protected IntWritable one = new IntWritable(1);

        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (containsSearchEngineQuery(value)) {
                Optional<String> queryLineOptional = Arrays.stream(StringUtils.split(value.toString(), '&'))
                        .filter(this::containsQueryLineBeginning)
                        .findFirst();
                if (queryLineOptional.isPresent()) {
                    String queryLine = queryLineOptional.get().substring(queryLineOptional.get().indexOf('=') + 1);
                    StringTokenizer itr = new StringTokenizer(queryLine, getQueryParamsDelimiter());
                    while (itr.hasMoreTokens()) {
                        word.set(itr.nextToken());
                        context.write(word, one);
                    }
                }
            }
        }

        protected abstract String getQueryParamsDelimiter();

        protected abstract boolean containsQueryLineBeginning(String line);

        protected abstract boolean containsSearchEngineQuery(Text value);
    }

    public static abstract class LogContentReducer extends TableReducer<Text,IntWritable,Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Put put = new Put(key.copyBytes());
            put.addColumn(WordOccurrencesCountRepository.FAMILY_NAME_BYTES, Bytes.toBytes(getColumnName()),
                    Bytes.toBytes(sum));
            context.write(null, put);
        }

        protected abstract String getColumnName();
    }
}
