package net.thumbtack.analyzer.occurrences;

import net.thumbtack.analyzer.common.HbaseTableUtils;
import org.apache.hadoop.hbase.TableName;
import org.springframework.stereotype.Component;
import java.util.Collections;
import java.util.List;

@Component
public class OccurrencesUtils extends HbaseTableUtils {

	@Override
	protected TableName getTableName() {
		return OccurrencesRepository.TABLE_NAME_NATIVE_REPRESENTATION;
	}

	@Override
	protected List<byte[]> getTableFamilies() {
		return Collections.singletonList(OccurrencesRepository.OCCURRENCE_FAMILY_NAME_BYTES);
	}
}