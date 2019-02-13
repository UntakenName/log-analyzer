package net.thumbtack.analyzer.data;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class WordOccurrencesCountRepository {

	@Autowired
	private HbaseTemplate hbaseTemplate;

	@Value("#{'${spring.main.searchEnginesNames}'.split(',')}")
	private List<String> searchEnginesNames;

	public static final String TABLE_NAME = "word";
	public static final String FAMILY_NAME = "occurrence";
	public static byte[] FAMILY_NAME_BYTES = Bytes.toBytes(FAMILY_NAME);

	public WordOccurrencesCount find(final String word) {
		return hbaseTemplate.get(TABLE_NAME, word, FAMILY_NAME, new WordOccurrencesCountRowMapper(searchEnginesNames));

	}

	public List<WordOccurrencesCount> findAll() {
		return hbaseTemplate.find(TABLE_NAME, FAMILY_NAME, new WordOccurrencesCountRowMapper(searchEnginesNames));

	}

	protected static class WordOccurrencesCountRowMapper implements RowMapper<WordOccurrencesCount> {

		public WordOccurrencesCountRowMapper(List<String> searchEnginesNames) {
			this.searchEnginesNames = searchEnginesNames;
		}

		private List<String> searchEnginesNames;

		@Override
		public WordOccurrencesCount mapRow(Result result, int rowNum) {
			Map<String, Integer> occurrencesMap = new HashMap<>();
			searchEnginesNames.forEach(searchEnginesName -> {
				byte[] occurrencesCount = result.getValue(FAMILY_NAME_BYTES, Bytes.toBytes(searchEnginesName));
				if (occurrencesCount != null) {
					occurrencesMap.put(searchEnginesName, Bytes.toInt(occurrencesCount));
				}
			});

			return new WordOccurrencesCount(Bytes.toString(result.getRow()), occurrencesMap);
		}
	}
}
