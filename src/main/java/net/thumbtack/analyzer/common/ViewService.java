package net.thumbtack.analyzer.common;

import net.thumbtack.analyzer.neighbours.SearchNeighbours;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class ViewService {

    @Autowired
    private JobRunner searchQueriesJobRunner;

    @Autowired
    private HbaseRepository<SearchNeighbours> repository;

    public ResponseEntity launchSearchQueriesJob() {
        try {
            searchQueriesJobRunner.call();
            return new ResponseEntity(HttpStatus.NO_CONTENT);
        } catch (Exception ex) {
            return new ResponseEntity<>(ex, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public SearchNeighbours getWordNeighbours(String word) {
        return repository.find(word);
    }
}
