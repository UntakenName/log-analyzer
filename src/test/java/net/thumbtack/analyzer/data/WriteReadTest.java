package net.thumbtack.analyzer.data;

import net.thumbtack.analyzer.LogAnalyzer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(locations = LogAnalyzer.CONTEXT_DESCRIPTION_PATH)
@IntegrationTest
public class WriteReadTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Autowired
    private Configuration configuration;

    @Autowired
    private JobRunner countSearchQueriesJobRunner;

    @Autowired
    private HbaseRepository<WordOccurrencesCount> repository;

    @Autowired
    private WordOccurrencesCountUtils utils;

    private FileSystem hdfs;
    private String randomKey;
    private Path fileNamePath;

    @Before
    public void initializeInputFileAndOutputTable() throws Exception {
        hdfs = FileSystem.get(configuration);
        randomKey = UUID.randomUUID().toString();
        fileNamePath = new Path("/user/root/input/"+ randomKey + ".txt");
        createInputFile();
        utils.initializeTable();
    }

    @After
    public void deleteInputFileAndOutputTable() throws Exception {
        deleteInputFile();
        utils.deleteTableIfExists();
        hdfs.close();
    }

    @Test
    public void doWriteReadTest() throws Exception {
        countSearchQueriesJobRunner.call();
        WordOccurrencesCount occurrence = repository.find(randomKey);
        assertNotNull("Failed to find a persisted object", occurrence);
        assertEquals(occurrence.getWord(), randomKey);
        Map<String, Integer> searchEngineByOccurrencesCountMap = occurrence.getSearchEngineByOccurrencesCountMap();
        Arrays.stream(SearchEngineParameters.values())
                .forEach(searchEngineParameters -> {
                    String searchEngineName = searchEngineParameters.getSearchEngineName();
                    assertTrue("Response doesn't contain value for key: " + searchEngineName,
                            searchEngineByOccurrencesCountMap.containsKey(searchEngineName));
                    assertEquals("Response contains wrong number of occurrences for key: " + searchEngineName,
                            searchEngineParameters.getSearchEngineSiteSignatures().size(),
                            (int) searchEngineByOccurrencesCountMap.get(searchEngineName));
                }
        );
    }

    private void createInputFile() throws Exception {
        if (hdfs.exists(fileNamePath)) {
            deleteInputFile();
        }
        OutputStream os = hdfs.create(fileNamePath, null);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8));
        StringBuilder fileContent = new StringBuilder();
        Arrays.stream(SearchEngineParameters.values()).forEach(searchEngineParameters ->
                searchEngineParameters.getSearchEngineSiteSignatures().forEach(siteSignature -> {
                    fileContent.append(siteSignature)
                            .append(searchEngineParameters.getQueryLineBeginning())
                            .append(randomKey)
                            .append("\n");
                })
        );
        br.write(fileContent.toString());
        br.close();
    }

    private void deleteInputFile() throws Exception {
        hdfs.delete(fileNamePath, true);
    }
}
