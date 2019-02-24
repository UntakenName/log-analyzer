package net.thumbtack.analyzer.neighbours;

import net.thumbtack.analyzer.mapreduce.MapReduceJobParentTest;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FindSearchNeighboursJobTest extends MapReduceJobParentTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testMapper() throws Exception {
        MapDriver<Object, Text, Text, Text> mapDriver = prepareMapDriver(SearchNeighboursMapper.class,
                "https://yandex.ru/search/?text=test1%20test2",
                Arrays.asList(new Pair<>("test1", "test2"), new Pair<>("test2", "test1")));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        String inputKey = "testVale";
        String inputValue = "testValue";
        ReduceDriver<Text,Text,Text, Mutation> reduceDriver = prepareReduceDriver(SearchNeighboursReducer.class,
                new Pair<>(inputKey, Arrays.asList(inputValue, inputValue)));
        List<Pair<Text, Mutation>> result = reduceDriver.run();

        Text outputKey = result.get(0).getFirst();
        assertEquals("Input and output keys doesn't match", inputKey, outputKey.toString());

        Put outputValue = (Put) result.get(0).getSecond();
        List<Cell> outputSumList = outputValue.get(SearchNeighboursRepository.NEIGHBOUR_FAMILY_NAME_BYTES, Bytes.toBytes(inputValue));
        assertEquals("Reducer returned unexpected number of outputs", 1, outputSumList.size());
    }
}
