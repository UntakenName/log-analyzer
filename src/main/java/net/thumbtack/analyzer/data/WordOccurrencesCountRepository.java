package net.thumbtack.analyzer.data;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Repository
public class WordOccurrencesCountRepository extends HbaseRepository<WordOccurrencesCount> {

	public static final String TABLE_NAME = "word";
	public static final TableName TABLE_NAME_NATIVE_REPRESENTATION = TableName.valueOf(TABLE_NAME);
	public static final String FAMILY_NAME = "occurrence";
	public static byte[] FAMILY_NAME_BYTES = Bytes.toBytes(FAMILY_NAME);

	@Override
	protected String getFamilyName() {
		return FAMILY_NAME;
	}

	@Override
	protected String getTableName() {
		return TABLE_NAME;
	}

	@Override
	protected RowMapper<WordOccurrencesCount> getRowMapper() {
		return (result, rowNum) -> {
			if (result.rawCells().length == 0) {
				return null;
			} else {
				Map<String, Integer> occurrencesMap = new HashMap<>();
				Arrays.stream(SearchEngineParameters.values()).map(SearchEngineParameters::getSearchEngineName)
						.forEach(searchEnginesName -> {
							byte[] occurrencesCount = result.getValue(FAMILY_NAME_BYTES,
									Bytes.toBytes(searchEnginesName));
							if (occurrencesCount != null) {
								occurrencesMap.put(searchEnginesName, Bytes.toInt(occurrencesCount));
							}
						});

				return new WordOccurrencesCount(Bytes.toString(result.getRow()), occurrencesMap);
			}
		};
	}
}
