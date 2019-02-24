package net.thumbtack.analyzer.neighbours;

import net.thumbtack.analyzer.common.HbaseRepository;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Repository;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Repository
public class SearchNeighboursRepository extends HbaseRepository<SearchNeighbours> {

	public static final String TABLE_NAME = "neighbours";
	public static final TableName TABLE_NAME_NATIVE_REPRESENTATION = TableName.valueOf(TABLE_NAME);

	public static final String NEIGHBOUR_FAMILY_NAME = "neighbour";
	public static byte[] NEIGHBOUR_FAMILY_NAME_BYTES = Bytes.toBytes(NEIGHBOUR_FAMILY_NAME);

	@Override
	protected String getFamilyName() {
		return NEIGHBOUR_FAMILY_NAME;
	}

	@Override
	protected String getTableName() {
		return TABLE_NAME;
	}

	@Override
	protected SearchNeighbours mapResults(Result result, int rowNum) {
		List<String> neighbours = Arrays.stream(result.rawCells())
				.map(CellUtil::cloneQualifier)
				.map(Bytes::toString)
				.collect(Collectors.toList());
		return new SearchNeighbours(Bytes.toString(result.getRow()), neighbours);
	}
}
