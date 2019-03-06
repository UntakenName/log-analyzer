package net.thumbtack.analyzer;

import net.thumbtack.analyzer.common.HbaseTableUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.util.Map;

@SpringBootApplication
@ImportResource({"classpath*:/META-INF/spring/application-context.xml"})
public class Application {

    private static final Log log = LogFactory.getLog(Application.class);

    @Autowired
    private Configuration hbaseConfiguration;

    @Bean
    public HbaseTemplate hbaseTemplate() {
        return new HbaseTemplate(hbaseConfiguration);
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
        context.registerShutdownHook();
        Map<String, HbaseTableUtils> utilsBeansMap = context.getBeansOfType(HbaseTableUtils.class);
        try {
            for (String key : utilsBeansMap.keySet()) {
                HbaseTableUtils utils = utilsBeansMap.get(key);
                if (!utils.ifTableExists()) {
                    utils.createTable();
                }
            }
        } catch (Exception e) {
            log.error(e);
        }
    }
}
