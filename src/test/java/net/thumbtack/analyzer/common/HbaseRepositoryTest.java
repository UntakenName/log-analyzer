package net.thumbtack.analyzer.common;

import org.apache.hadoop.hbase.client.Result;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

public class HbaseRepositoryTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private HbaseTemplate mockedTemlate = mock(HbaseTemplate.class);

    private HbaseRepository repository;

    private static List<HbaseRepository> beansClasses = new ArrayList<>();

    public HbaseRepositoryTest() throws Exception {
        repository = new HbaseRepository() {

            @Override
            protected String getFamilyName() {
                return null;
            }

            @Override
            protected String getTableName() {
                return null;
            }

            @Override
            protected Object mapResults(Result result, int rowNum) {
                return null;
            }
        };

        Field hbaseTemplateFiled = HbaseRepository.class.getDeclaredField("hbaseTemplate");
        hbaseTemplateFiled.setAccessible(true);
        hbaseTemplateFiled.set(repository, mockedTemlate);

        for (BeanDefinition beanDefinition : InfrastructureTestUtils
                .getBeansClassesNamesForSuperClass(HbaseRepository.class)) {
            HbaseRepository repository = (HbaseRepository) Class.forName(beanDefinition.getBeanClassName()).newInstance();
            beansClasses.add(repository);
        }
    }

    @Test
    public void checkFindByNullKeyReturn() {
        repository.find(null);
        verify(mockedTemlate, never()).get(any(String.class),
                any(String.class), any(String.class), any(String.class), any(RowMapper.class));
    }

    @Test
    public void checkImplementations() {
        beansClasses.forEach(bean -> {
            assertNotNull(bean.getTableName());
            assertNotNull(bean.getFamilyName());
        });
    }
}
