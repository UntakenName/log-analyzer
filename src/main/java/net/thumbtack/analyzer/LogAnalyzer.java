package net.thumbtack.analyzer;

import net.thumbtack.analyzer.data.WordOccurrencesCount;
import net.thumbtack.analyzer.data.WordOccurrencesCountRepository;
import net.thumbtack.analyzer.data.WordOccurrencesCountUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.hadoop.mapreduce.JobRunner;

import java.util.List;

@SpringBootApplication
public class LogAnalyzer {

    public static final String CONTEXT_DESCRIPTION_PATH = "/META-INF/spring/application-context.xml";

    private static final Log log = LogFactory.getLog(LogAnalyzer.class);

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(CONTEXT_DESCRIPTION_PATH, args);
        context.registerShutdownHook();
        JobRunner jobRunner = (JobRunner) context.getBean("countSearchQueriesJobRunner");
        WordOccurrencesCountUtils utils = context.getBean(WordOccurrencesCountUtils.class);
        WordOccurrencesCountRepository repository = context.getBean(WordOccurrencesCountRepository.class);
        try {
            utils.initializeTable();
            jobRunner.call();
        } catch (Exception e) {
            log.error(e);
        }
    }
}
