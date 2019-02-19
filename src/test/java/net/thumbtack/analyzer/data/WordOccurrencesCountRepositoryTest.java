package net.thumbtack.analyzer.data;

import net.thumbtack.analyzer.LogAnalyzer;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertNull;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(locations = LogAnalyzer.CONTEXT_DESCRIPTION_PATH)
public class WordOccurrencesCountRepositoryTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Autowired
    private HbaseRepository<WordOccurrencesCount> repository;

    @Autowired
    private WordOccurrencesCountUtils utils;

    @Before
    public void initializeInputFileAndOutputTable() throws Exception {
        utils.initializeTable();
    }

    @After
    public void deleteInputFileAndOutputTable() throws Exception {
        utils.deleteTableIfExists();
    }

    @Test
    public void checkFindByAbsentKeyReturn() {
        WordOccurrencesCount occurrence = repository.find("absentKey");
        assertNull("Non-null object returned on an absent key:", occurrence);
    }

    @Test
    public void checkFindByNullKeyReturn() {
        WordOccurrencesCount occurrence = repository.find(null);
        assertNull("Non-null object returned on a null key:", occurrence);
    }

    @Test
    public void checkFindAll() {
        repository.findAll();
    }
}
