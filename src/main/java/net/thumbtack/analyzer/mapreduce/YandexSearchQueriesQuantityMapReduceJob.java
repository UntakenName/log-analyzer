package net.thumbtack.analyzer.mapreduce;

import org.apache.hadoop.io.Text;

public class YandexSearchQueriesQuantityMapReduceJob extends SearchQueriesQuantityMapReduceJob {

    public static class YandexQueriesMapper extends LogContentMapper {

        protected boolean containsSearchEngineQuery(Text value) {
            return value.find("yandex.ru/search/?") > 0;
        }

        protected boolean containsQueryLineBeginning(String line) {
            return line.contains("text=");
        }

        @Override
        protected String getQueryParamsDelimiter() {
            return "%20";
        }
    }

    public static class YandexQueriesReducer extends LogContentReducer {

        @Override
        protected String getColumnName() {
            return "yandex";
        }
    }
}
