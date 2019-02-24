package net.thumbtack.analyzer.occurrences;

import net.thumbtack.analyzer.common.SearchEngineParameters;
import net.thumbtack.analyzer.mapreduce.SearchQueriesMapReduceJob;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class OccurrencesMapper extends SearchQueriesMapReduceJob.LogContentMapper {

    @Override
    protected void process(Object key, String[] parsedKeyWords, SearchEngineParameters parameters, Context context)
            throws IOException, InterruptedException {
        Text word = new Text();
        Text mapResult = new Text();
        for (String keyWord : parsedKeyWords) {
            word.set(keyWord);
            mapResult.set(parameters.getSearchEngineName());
            context.write(word, mapResult);
        }
    }
}
