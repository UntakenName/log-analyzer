package net.thumbtack.analyzer.common;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.beans.factory.config.BeanDefinition;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class HbaseTableUtilsTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private Admin mockedAdmin = mock(Admin.class);

    private ArgumentCaptor<HTableDescriptor> tableDescriptorCaptor = ArgumentCaptor.forClass(HTableDescriptor.class);

    private HbaseTableUtils utils;

    private TableName tableTestName = TableName.valueOf("test");

    private List<byte[]> familiesTestArray = Collections.singletonList(Bytes.toBytes("test"));

    private static List<HbaseTableUtils> beansClasses = new ArrayList<>();

    public HbaseTableUtilsTest() throws Exception {
        utils = new HbaseTableUtils() {

            @Override
            protected TableName getTableName() {
                return tableTestName;
            }

            @Override
            protected List<byte[]> getTableFamilies() {
                return familiesTestArray;
            }
        };

        Field adminFiled = HbaseTableUtils.class.getDeclaredField("admin");
        adminFiled.setAccessible(true);
        adminFiled.set(utils, mockedAdmin);

        when(mockedAdmin.isTableDisabled(any(TableName.class))).thenReturn(false);

        for (BeanDefinition beanDefinition : InfrastructureTestUtils
                .getBeansClassesNamesForSuperClass(HbaseTableUtils.class)) {
            HbaseTableUtils utils = (HbaseTableUtils) Class.forName(beanDefinition.getBeanClassName()).newInstance();
            beansClasses.add(utils);
        }
    }

    @Test
    public void checkDeleteTableInvocationOrder() throws Exception {
        utils.deleteTable();
        InOrder inOrder = inOrder(mockedAdmin);

        inOrder.verify(mockedAdmin).isTableDisabled(any(TableName.class));
        inOrder.verify(mockedAdmin).disableTable(any(TableName.class));
        inOrder.verify(mockedAdmin).deleteTable(any(TableName.class));
    }

    @Test
    public void checkTableCreationInputArgument() throws Exception {
        utils.createTable();

        verify(mockedAdmin).createTable(tableDescriptorCaptor.capture());
        HTableDescriptor argument = tableDescriptorCaptor.getValue();

        assertNotNull(argument);
        assertEquals(tableTestName, argument.getTableName());

        assertEquals(familiesTestArray, argument.getFamilies()
                .stream()
                .map(HColumnDescriptor::getName)
                .collect(Collectors.toList()));
    }

    @Test
    public void checkImplementations() {
        beansClasses.forEach(bean -> {
            assertNotNull(bean.getTableName());
            assertNotNull(bean.getTableFamilies());
        });
    }
}
