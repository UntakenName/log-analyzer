package net.thumbtack.analyzer.occurrences;

import net.thumbtack.analyzer.common.WordStatistics;

import java.util.Map;

public class Occurrences extends WordStatistics {

	private Map<String, Integer> searchEngineByOccurrencesCountMap;

	public Occurrences() {
	}

	public Occurrences(String word, Map<String, Integer> occurrencesCount) {
		super(word);
		this.searchEngineByOccurrencesCountMap = occurrencesCount;
	}

	public Map<String, Integer> getSearchEngineByOccurrencesCountMap() {
		return searchEngineByOccurrencesCountMap;
	}

	public void setSearchEngineByOccurrencesCountMap(Map<String, Integer> searchEngineByOccurrencesCountMap) {
		this.searchEngineByOccurrencesCountMap = searchEngineByOccurrencesCountMap;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder(getWord() == null ? "" : super.toString());
		if (searchEngineByOccurrencesCountMap != null && !searchEngineByOccurrencesCountMap.isEmpty()) {
			result.append(" : ").append(searchEngineByOccurrencesCountMap.toString());
		}
		return result.toString();
	}
}
