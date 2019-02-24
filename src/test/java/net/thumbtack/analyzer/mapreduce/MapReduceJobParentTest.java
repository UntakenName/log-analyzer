package net.thumbtack.analyzer.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import java.util.List;
import java.util.stream.Collectors;

public class MapReduceJobParentTest {

    protected MapDriver<Object, Text, Text, Text> prepareMapDriver(
            Class<? extends SearchQueriesMapReduceJob.LogContentMapper> mapperClass, String logSegment,
            List<Pair<String, String>> outputs) throws Exception {
        MapDriver<Object, Text, Text, Text> mapDriver = MapDriver.newMapDriver(mapperClass.newInstance());
        mapDriver.withInput(new Text("1"), new Text(logSegment));
        outputs.forEach(outputPair ->
                mapDriver.withOutput(new Text(outputPair.getFirst()), new Text(outputPair.getSecond())));
        return mapDriver;
    }

    protected ReduceDriver<Text,Text,Text, Mutation> prepareReduceDriver(
            Class<? extends SearchQueriesMapReduceJob.LogContentReducer> reducerClass,
            Pair<String, List<String>> input) throws Exception {
        ReduceDriver<Text,Text,Text, Mutation> reduceDriver = ReduceDriver.newReduceDriver(reducerClass.newInstance());
        Configuration conf = reduceDriver.getConfiguration();
        conf.set("io.serializations","org.apache.hadoop.hbase.mapreduce.MutationSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");

        reduceDriver.withInput(new Text(input.getFirst()), input.getSecond()
                .stream()
                .map(Text::new)
                .collect(Collectors.toList()));

        return reduceDriver;
    }
}
