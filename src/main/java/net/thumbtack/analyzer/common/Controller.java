package net.thumbtack.analyzer.common;

import net.thumbtack.analyzer.neighbours.SearchNeighbours;
import net.thumbtack.analyzer.occurrences.Occurrences;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
public class Controller {

    @Autowired
    private ViewService service;

    @GetMapping("${spring.data.rest.launchJobs}")
    @ResponseBody
    public ResponseEntity launchSearchQueriesJob() {
        return service.launchSearchQueriesJob();
    }

    @GetMapping("${spring.data.rest.neighbours}")
    @ResponseBody
    public SearchNeighbours getWordNeighbours(@RequestParam("word") String word) {
        return service.getWordNeighbours(word);
    }

    @GetMapping("${spring.data.rest.words}")
    @ResponseBody
    public List<Occurrences> getWordsOccurrences() {
        return service.getWordsOccurrences();
    }
}
