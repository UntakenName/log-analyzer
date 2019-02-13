package net.thumbtack.analyzer.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;

@Component
public class WordOccurrencesCountUtils implements InitializingBean {

	@Resource(name = "hbaseConfiguration")
	private Configuration config;

	private Admin admin;

	public void initializeTable() throws IOException {

		TableName name = TableName.valueOf(WordOccurrencesCountRepository.TABLE_NAME);
		if (admin.tableExists(name)) {
			if (!admin.isTableDisabled(name)) {
				admin.disableTable(name);
			}
			admin.deleteTable(name);
		}

		HTableDescriptor tableDescriptor = new HTableDescriptor(name);
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(WordOccurrencesCountRepository.FAMILY_NAME_BYTES);
		tableDescriptor.addFamily(columnDescriptor);

		admin.createTable(tableDescriptor);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		admin = new HBaseAdmin(config);
	}
}