package net.thumbtack.analyzer.mapreduce;

import net.thumbtack.analyzer.data.WordOccurrencesCountRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SearchQueriesQuantityMapReduceJobTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testMapper() throws IOException {
        Mapper<Object, Text, Text, Text> mapper = new SearchQueriesQuantityMapReduceJob.LogContentMapper();
        MapDriver<Object, Text, Text, Text> mapDriver = MapDriver.newMapDriver(mapper);
        mapDriver.withInput(new Text("1"), new Text("https://yandex.ru/search/?text=test%20test"));
        mapDriver.withOutput(new Text("test"), new Text("yandex"));
        mapDriver.withOutput(new Text("test"), new Text("yandex"));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        Reducer<Text,Text,Text, Mutation> reducer = new SearchQueriesQuantityMapReduceJob.LogContentReducer();
        ReduceDriver<Text,Text,Text, Mutation> reduceDriver = ReduceDriver.newReduceDriver(reducer);
        Configuration conf = reduceDriver.getConfiguration();
        conf.set("io.serializations","org.apache.hadoop.hbase.mapreduce.MutationSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");

        Text inputKey = new Text("testKey");
        Text inputValue = new Text("testValue");
        reduceDriver.withInput(inputKey, Arrays.asList(inputValue, inputValue));
        List<Pair<Text, Mutation>> result = reduceDriver.run();

        Text outputKey = result.get(0).getFirst();
        assertEquals("Input and output keys doesn't match", inputKey, outputKey);

        Put outputValue = (Put) result.get(0).getSecond();
        List<Cell> outputSumList = outputValue.get(WordOccurrencesCountRepository.FAMILY_NAME_BYTES, inputValue.copyBytes());
        assertEquals("Reducer returned unexpected number of outputs", outputSumList.size(), 1);

        int sum = Bytes.toInt(outputSumList.get(0).getValue());
        assertEquals("Reducer miscalculated an output sum", sum, 2);
    }
}
