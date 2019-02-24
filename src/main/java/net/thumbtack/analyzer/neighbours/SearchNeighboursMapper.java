package net.thumbtack.analyzer.neighbours;

import net.thumbtack.analyzer.common.SearchEngineParameters;
import net.thumbtack.analyzer.mapreduce.SearchQueriesMapReduceJob;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class SearchNeighboursMapper extends SearchQueriesMapReduceJob.LogContentMapper {

    @Override
    protected void process(Object key, String[] parsedKeyWords, SearchEngineParameters parameters, Context context)
            throws IOException, InterruptedException {

        Text word = new Text();
        Text wordNeighbour = new Text();

        for (String keyWord : parsedKeyWords) {
            word.set(keyWord);
            for (String neighbour : parsedKeyWords) {
                if (keyWord.equals(neighbour)) continue;
                wordNeighbour.set(neighbour);
                context.write(word, wordNeighbour);
            }
        }
    }
}
