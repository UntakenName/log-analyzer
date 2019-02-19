package net.thumbtack.analyzer.mapreduce;

import net.thumbtack.analyzer.data.SearchEngineParameters;

public class LogSegmentParseResult {

    private Boolean foundSearchQuery;

    private String logSegment;

    private SearchEngineParameters searchEngineParameters;

    public Boolean getFoundSearchQuery() {
        return foundSearchQuery;
    }

    public void setFoundSearchQuery(Boolean foundSearchQuery) {
        this.foundSearchQuery = foundSearchQuery;
    }

    public String getLogSegment() {
        return logSegment;
    }

    public void setLogSegment(String logSegment) {
        this.logSegment = logSegment;
    }

    public SearchEngineParameters getSearchEngineParameters() {
        return searchEngineParameters;
    }

    public void setSearchEngineParameters(SearchEngineParameters searchEngineParameters) {
        this.searchEngineParameters = searchEngineParameters;
    }
}
