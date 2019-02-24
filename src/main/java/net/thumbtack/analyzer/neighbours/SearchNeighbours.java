package net.thumbtack.analyzer.neighbours;

import net.thumbtack.analyzer.common.WordStatistics;

import java.util.List;

public class SearchNeighbours extends WordStatistics {

	private List<String> neighbours;

	public SearchNeighbours() {
	}

	public SearchNeighbours(String word, List<String> neighbours) {
		super(word);
		this.neighbours = neighbours;
	}

	public List<String> getNeighbours() {
		return neighbours;
	}

	public void setNeighbours(List<String> neighbours) {
		this.neighbours = neighbours;
	}

	@Override
	public String toString() {
		StringBuilder result = new StringBuilder(super.toString());
		if (neighbours != null && !neighbours.isEmpty()) {
			result.append(" ").append(neighbours);
		}
		return result.toString();
	}
}
