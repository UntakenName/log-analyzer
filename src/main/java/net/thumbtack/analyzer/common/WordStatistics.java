package net.thumbtack.analyzer.common;

public class WordStatistics {

	private String word;

	public WordStatistics() {
	}

	public WordStatistics(String word) {
		this.word = word;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	@Override
	public String toString() {
		return word;
	}
}
