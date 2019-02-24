package net.thumbtack.analyzer.neighbours;

import net.thumbtack.analyzer.mapreduce.SearchQueriesMapReduceJob;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.StreamSupport;

public class SearchNeighboursReducer extends SearchQueriesMapReduceJob.LogContentReducer {

    @Override
    protected void reduceValuesToTablePut(Put put, Iterable<Text> values) {
        Set<String> neighbours = new HashSet<>();
        StreamSupport.stream(values.spliterator(), false)
                .map(Text::toString)
                .forEach(neighbours::add);

        neighbours.forEach(neighbour ->
                put.addColumn(SearchNeighboursRepository.NEIGHBOUR_FAMILY_NAME_BYTES,
                        Bytes.toBytes(neighbour),
                        Bytes.toBytes(""))

        );
    }
}
