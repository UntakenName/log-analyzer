package net.thumbtack.analyzer.occurrences;

import net.thumbtack.analyzer.common.HbaseRepository;
import net.thumbtack.analyzer.common.SearchEngineParameters;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Repository
public class OccurrencesRepository extends HbaseRepository<Occurrences> {

	public static final String TABLE_NAME = "word";
	public static final TableName TABLE_NAME_NATIVE_REPRESENTATION = TableName.valueOf(TABLE_NAME);

	public static final String OCCURRENCE_FAMILY_NAME = "occurrence";
	public static byte[] OCCURRENCE_FAMILY_NAME_BYTES = Bytes.toBytes(OCCURRENCE_FAMILY_NAME);

	@Override
	protected String getFamilyName() {
		return OCCURRENCE_FAMILY_NAME;
	}

	@Override
	protected String getTableName() {
		return TABLE_NAME;
	}

	@Override
	protected Occurrences mapResults(Result result, int rowNum) {
		Map<String, Integer> occurrencesCountMap = new HashMap<>();
		Arrays.stream(SearchEngineParameters.values()).map(SearchEngineParameters::getSearchEngineName)
				.forEach(searchEnginesName -> {
					byte[] resultData = result.getValue(OCCURRENCE_FAMILY_NAME_BYTES,
							Bytes.toBytes(searchEnginesName));
					if (resultData != null) {
						occurrencesCountMap.put(searchEnginesName, Bytes.toInt(resultData));
					}
				});

		return new Occurrences(Bytes.toString(result.getRow()),	occurrencesCountMap);
	}
}
