package net.thumbtack.analyzer.occurrences;

import net.thumbtack.analyzer.mapreduce.SearchQueriesMapReduceJob;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

public class OccurrencesReducer extends SearchQueriesMapReduceJob.LogContentReducer {

    @Override
    protected void reduceValuesToTablePut(Put put, Iterable<Text> values) {
        Map<String, AtomicInteger> searchEngineNameToOccurrencesMap = new HashMap<>();
        StreamSupport.stream(values.spliterator(), false)
                .map(Text::toString)
                .forEach(value -> {
                    AtomicInteger sum = searchEngineNameToOccurrencesMap.get(value);
                    if (sum == null) {
                        searchEngineNameToOccurrencesMap.put(value, new AtomicInteger(1));
                    } else {
                        sum.incrementAndGet();
                    }
                });

        searchEngineNameToOccurrencesMap.keySet().forEach(value ->
                put.addColumn(OccurrencesRepository.OCCURRENCE_FAMILY_NAME_BYTES,
                        Bytes.toBytes(value),
                        Bytes.toBytes(searchEngineNameToOccurrencesMap.get(value).get())));
    }
}
