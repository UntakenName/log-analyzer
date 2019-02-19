package net.thumbtack.analyzer.data;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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
		deleteTableIfExists();

		HTableDescriptor tableDescriptor = new HTableDescriptor(WordOccurrencesCountRepository.TABLE_NAME_NATIVE_REPRESENTATION);
		HColumnDescriptor columnDescriptor = new HColumnDescriptor(WordOccurrencesCountRepository.FAMILY_NAME_BYTES);
		tableDescriptor.addFamily(columnDescriptor);

		admin.createTable(tableDescriptor);
	}

	public void deleteTableIfExists() throws IOException {
		if (admin.tableExists(WordOccurrencesCountRepository.TABLE_NAME_NATIVE_REPRESENTATION)) {
			if (!admin.isTableDisabled(WordOccurrencesCountRepository.TABLE_NAME_NATIVE_REPRESENTATION)) {
				admin.disableTable(WordOccurrencesCountRepository.TABLE_NAME_NATIVE_REPRESENTATION);
			}
			admin.deleteTable(WordOccurrencesCountRepository.TABLE_NAME_NATIVE_REPRESENTATION);
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		admin = new HBaseAdmin(config);
	}
}