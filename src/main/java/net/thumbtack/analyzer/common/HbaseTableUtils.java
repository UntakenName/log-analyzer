package net.thumbtack.analyzer.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.beans.factory.InitializingBean;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;


public abstract class HbaseTableUtils implements InitializingBean {

	@Resource(name = "hbaseConfiguration")
	private Configuration config;

	private Admin admin;

	public void createTable() throws IOException {
		HTableDescriptor tableDescriptor =
				new HTableDescriptor(getTableName());

		getTableFamilies().forEach(
				familyName -> {
					HColumnDescriptor columnDescriptor = new HColumnDescriptor(familyName);
					tableDescriptor.addFamily(columnDescriptor);
				}
		);

		admin.createTable(tableDescriptor);
	}

	public boolean ifTableExists() throws IOException {
		return admin.tableExists(getTableName());
	}

	public void deleteTable() throws IOException {
		TableName tableName = getTableName();
		if (!admin.isTableDisabled(tableName)) {
			admin.disableTable(tableName);
		}
		admin.deleteTable(tableName);
	}

	protected abstract TableName getTableName();

	protected abstract List<byte[]> getTableFamilies();

	@Override
	public void afterPropertiesSet() throws Exception {
		admin = new HBaseAdmin(config);
	}
}