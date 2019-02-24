package net.thumbtack.analyzer.neighbours;

import net.thumbtack.analyzer.common.HbaseTableUtils;
import org.apache.hadoop.hbase.TableName;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class SearchNeighboursUtils extends HbaseTableUtils {

	@Override
	protected TableName getTableName() {
		return SearchNeighboursRepository.TABLE_NAME_NATIVE_REPRESENTATION;
	}

	@Override
	protected List<byte[]> getTableFamilies() {
		return Collections.singletonList(SearchNeighboursRepository.NEIGHBOUR_FAMILY_NAME_BYTES);
	}
}