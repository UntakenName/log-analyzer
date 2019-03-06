package net.thumbtack.analyzer.common;

import net.thumbtack.analyzer.neighbours.SearchNeighbours;
import net.thumbtack.analyzer.occurrences.Occurrences;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.Assert.*;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class WriteReadIntegrationTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Value("${spring.hadoop.mapReduceInput}")
    private String inputFolderPath;

    @Value("${spring.data.rest.launchJobs}")
    private String launchJobsUri;

    @Value("${spring.data.rest.neighbours}")
    private String neighboursUri;

    @Value("${spring.data.rest.words}")
    private String wordsUri;

    @Autowired
    private Configuration configuration;

    @Autowired
    private HbaseRepository<Occurrences> occurrencesRepository;

    @Autowired
    private HbaseRepository<SearchNeighbours> searchNeighboursRepositoryRepository;

    @Autowired
    private List<HbaseTableUtils> utilsList;

    @Autowired
    private TestRestTemplate restTemplate;

    private FileSystem hdfs;
    private String randomKey;
    private Path fileNamePath;
    private final String testString = "test";

    @Before
    public void initializeInputFileAndOutputTable() throws Exception {
        hdfs = FileSystem.get(configuration);
        randomKey = UUID.randomUUID().toString();
        fileNamePath = new Path(inputFolderPath + "/"+ randomKey + ".txt");
        createInputFile();

        for (HbaseTableUtils utils : utilsList) {
            if (utils.ifTableExists()) {
                utils.deleteTable();
            }
            utils.createTable();
        }
    }

    @After
    public void deleteInputFileAndOutputTable() throws Exception {
        deleteInputFile();
        for (HbaseTableUtils utils : utilsList) {
            utils.deleteTable();
        }
        hdfs.close();
    }

    @Test
    public void doWriteReadTest() {
        restTemplate.getForEntity(launchJobsUri, ResponseEntity.class);

        SearchNeighbours neighboursStatistics = restTemplate.getForEntity(neighboursUri + "?word={word}",
                SearchNeighbours.class, randomKey).getBody();
        assertNotNull("Failed to find a persisted search neighbour", neighboursStatistics);
        List<String> neighbours = neighboursStatistics.getNeighbours();
        assertNotNull("No response from the neighbors endpoint", neighbours);
        assertTrue("Wrong response", neighbours.size() == 1 && testString.equals(neighbours.get(0)));

        Occurrences occurrence = occurrencesRepository.find(randomKey);
        assertNotNull("Failed to find a persisted occurrence", occurrence);
        assertEquals(randomKey, occurrence.getWord());
        Map<String, Integer> searchEngineByOccurrencesCountMap = occurrence.getSearchEngineByOccurrencesCountMap();
        Arrays.stream(SearchEngineParameters.values())
                .forEach(searchEngineParameters -> {
                    String searchEngineName = searchEngineParameters.getSearchEngineName();
                    assertTrue("Response doesn't contain value for key: " + searchEngineName,
                            searchEngineByOccurrencesCountMap.containsKey(searchEngineName));
                    assertEquals("Response contains wrong number of occurrences for key: " + searchEngineName,
                            (int) searchEngineByOccurrencesCountMap.get(searchEngineName),
                            searchEngineParameters.getSearchEngineSiteSignatures().size());
                }
        );

        List occurrencesList = restTemplate.getForObject(wordsUri, List.class);
        assertNotNull("Failed to find persisted word occurrences", occurrencesList);
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
                            .append(searchEngineParameters.getQueryParamsDelimiter())
                            .append(testString)
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
