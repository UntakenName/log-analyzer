package net.thumbtack.analyzer.mapreduce;

import net.thumbtack.analyzer.common.SearchEngineParameters;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;
import java.util.*;

public class SearchQueriesMapReduceJob {

    public static abstract class LogContentMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Optional<LogSegmentParseResult> parseResultOptional = parseLogSegment(value);
            if (parseResultOptional.isPresent()) {
                LogSegmentParseResult result = parseResultOptional.get();
                SearchEngineParameters parameters = result.getSearchEngineParameters();
                String queryLine = result.getLogSegment();

                String keyWords = queryLine.substring(queryLine.indexOf('=') + 1);
                String[] parsedKeyWords = new StrTokenizer(keyWords, parameters.getQueryParamsDelimiter())
                        .getTokenArray();

                process(key, parsedKeyWords, parameters, context);
            }
        }

        protected Optional<LogSegmentParseResult> parseLogSegment(Text value) {
            return Arrays.stream(SearchEngineParameters.values())
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
        }

        protected abstract void process(Object key, String[] parsedKeyWords, SearchEngineParameters parameters,
                                        Context context) throws IOException, InterruptedException ;
    }

    public static abstract class LogContentReducer extends TableReducer<Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            Put put = new Put(key.copyBytes());
            reduceValuesToTablePut(put, values);

            context.write(key, put);
        }

        protected abstract void reduceValuesToTablePut(Put put, Iterable<Text> values);
    }
}
