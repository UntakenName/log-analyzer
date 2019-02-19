package net.thumbtack.analyzer.data;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum SearchEngineParameters {

    YANDEX("yandex", "text=", "%20", "yandex.ru/search/?"),
    GOOGLE("google", "q=", "+",	"google.com/search?", "google.ru/search?");

    SearchEngineParameters(String searchEngineName, String queryLineBeginning, String queryParamsDelimiter,
            String... searchEngineSiteSignatures) {
        this.searchEngineName = searchEngineName;
        this.searchEngineSiteSignatures = Collections.unmodifiableList(Arrays.asList(searchEngineSiteSignatures));
        this.queryLineBeginning = queryLineBeginning;
        this.queryParamsDelimiter = queryParamsDelimiter;
    }

    private String searchEngineName;

    private List<String> searchEngineSiteSignatures;

    private String queryLineBeginning;

    private String queryParamsDelimiter;

    public String getSearchEngineName() {
        return searchEngineName;
    }

    public String getQueryParamsDelimiter() {
        return queryParamsDelimiter;
    }

    public List<String> getSearchEngineSiteSignatures() {
        return searchEngineSiteSignatures;
    }

    public String getQueryLineBeginning() {
        return queryLineBeginning;
    }
}
