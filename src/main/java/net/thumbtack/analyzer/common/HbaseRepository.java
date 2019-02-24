package net.thumbtack.analyzer.common;

import org.apache.hadoop.hbase.client.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;

import java.util.List;

public abstract class HbaseRepository<OBJECT_TYPE> {

    @Autowired
    protected HbaseTemplate hbaseTemplate;

    public OBJECT_TYPE find(final String word) {
        if (word == null) {
            return null;
        } else {
            return hbaseTemplate.get(getTableName(), word, getFamilyName(), getRowMapper());
        }
    }

    public List<OBJECT_TYPE> findAll() {
        return hbaseTemplate.find(getTableName(), getFamilyName(), getRowMapper());
    }

    protected abstract String getFamilyName();

    protected abstract String getTableName();

    private RowMapper<OBJECT_TYPE> getRowMapper() {
        return (result, rowNum) -> {
            if (result.rawCells().length == 0) {
                return null;
            } else {
                return mapResults(result, rowNum);
            }
        };
    }

    protected abstract OBJECT_TYPE mapResults(Result result, int rowNum);
}
