package net.thumbtack.analyzer.data;

import java.util.Map;

public class WordOccurrencesCount {

	private String word;
	private Map<String, Integer> searchEngineByOccurrencesCountMap;

	public WordOccurrencesCount(String word, Map<String, Integer> occurrences) {
		super();
		this.word = word;
		this.searchEngineByOccurrencesCountMap = occurrences;
	}

	public String getWord() {
		return word;
	}

	public Map<String, Integer> getSearchEngineByOccurrencesCountMap() {
		return searchEngineByOccurrencesCountMap;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder(word);
		if (searchEngineByOccurrencesCountMap != null && !searchEngineByOccurrencesCountMap.isEmpty()) {
			result.append(" : ").append(searchEngineByOccurrencesCountMap.toString());
		}
		return result.toString();
	}
	
	
}
