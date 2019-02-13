package net.thumbtack.analyzer.mapreduce;

import org.apache.hadoop.io.Text;

public class GoogleSearchQueriesQuantityMapReduceJob extends SearchQueriesQuantityMapReduceJob {

    public static class GoogleQueriesMapper extends LogContentMapper {

        protected boolean containsSearchEngineQuery(Text value) {
            return value.find("google.com/search?") > 0 || value.find("google.ru/search?") > 0;
        }

        protected boolean containsQueryLineBeginning(String line) {
            return line.contains("q=");
        }

        @Override
        protected String getQueryParamsDelimiter() {
            return "+";
        }
    }

    public static class GoogleQueriesReducer extends LogContentReducer {

        @Override
        protected String getColumnName() {
            return "google";
        }
    }
}
