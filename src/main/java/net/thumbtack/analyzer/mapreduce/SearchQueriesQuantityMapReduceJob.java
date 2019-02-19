package net.thumbtack.analyzer.mapreduce;

import net.thumbtack.analyzer.data.SearchEngineParameters;
import net.thumbtack.analyzer.data.WordOccurrencesCountRepository;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

public class SearchQueriesQuantityMapReduceJob {

    public static class LogContentMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();

        private Text searchEngineName = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Optional<LogSegmentParseResult> parseResultOptional = Arrays.stream(SearchEngineParameters.values())
                    .map(searchEngineParameters -> {
                        LogSegmentParseResult result = new LogSegmentParseResult();
                        boolean foundSearchQuery = searchEngineParameters.getSearchEngineSiteSignatures().stream()
                                .anyMatch(signature -> value.find(signature) > -1);
                        result.setFoundSearchQuery(foundSearchQuery);
                        if (foundSearchQuery) {
                            result.setSearchEngineParameters(searchEngineParameters);

                            String logSegment = Arrays.stream(StringUtils.split(value.toString(), '&'))
                                    .filter(querySegment ->
                                            querySegment.contains(searchEngineParameters.getQueryLineBeginning()))
                                    .findFirst()
                                    .orElse("");
                            result.setLogSegment(logSegment);
                        }
                        return result;})
                    .filter(LogSegmentParseResult::getFoundSearchQuery)
                    .findFirst();
            if (parseResultOptional.isPresent()) {
                LogSegmentParseResult result = parseResultOptional.get();
                SearchEngineParameters parameters = result.getSearchEngineParameters();

                String queryLine = result.getLogSegment();
                String keyWords = queryLine.substring(queryLine.indexOf('=') + 1);

                StrTokenizer tokenizer = new StrTokenizer(keyWords, parameters.getQueryParamsDelimiter());
                for (String keyWord : tokenizer.getTokenArray()) {
                    word.set(keyWord);
                    searchEngineName.set(parameters.getSearchEngineName());
                    context.write(word, searchEngineName);
                }
            }
        }
    }

    public static class LogContentReducer extends TableReducer<Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {

            Put put = new Put(key.copyBytes());
            Map<String, AtomicInteger> searchEngineNameToOccurrencesMap = new HashMap<>();
            StreamSupport.stream(values.spliterator(), false)
                    .map(Text::toString)
                    .forEach(value -> {
                        AtomicInteger sum = searchEngineNameToOccurrencesMap.get(value);
                        if (sum == null) {
                            searchEngineNameToOccurrencesMap.put(value, new AtomicInteger(1));
                        } else {
                            sum.incrementAndGet();
                        }
            });

            searchEngineNameToOccurrencesMap.keySet().forEach(value ->
                    put.addColumn(WordOccurrencesCountRepository.FAMILY_NAME_BYTES,
                            Bytes.toBytes(value),
                            Bytes.toBytes(searchEngineNameToOccurrencesMap.get(value).get())));

            context.write(key, put);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        CommandLine cmd = parseArgs(otherArgs);
        String table = cmd.getOptionValue("t");
        String input = cmd.getOptionValue("i");
        Job job = Job.getInstance(conf, "countSearchQueriesJob");
        job.setJarByClass(SearchQueriesQuantityMapReduceJob.class);
        job.setMapperClass(SearchQueriesQuantityMapReduceJob.LogContentMapper.class);
        job.setCombinerClass(SearchQueriesQuantityMapReduceJob.LogContentReducer.class);
        job.setReducerClass(SearchQueriesQuantityMapReduceJob.LogContentReducer.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        FileInputFormat.addInputPath(job, new Path(input));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static CommandLine parseArgs(String[] args) {
        Options options = new Options();
        Option o = new Option("t", "table", true,
                "table to import into (must exist)");
        o.setArgName("table-name");
        o.setRequired(true);
        options.addOption(o);
        o = new Option("i", "input", true,
                "the directory or file to read from");
        o.setArgName("path-in-HDFS");
        o.setRequired(true);
        options.addOption(o);
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            System.exit(-1);
        }
        return cmd;
    }
}
