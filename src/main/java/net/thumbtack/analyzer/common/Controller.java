package net.thumbtack.analyzer.common;

import net.thumbtack.analyzer.neighbours.SearchNeighbours;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class Controller {

    @Autowired
    private ViewService service;

    @RequestMapping(method = RequestMethod.GET,value = "${spring.data.rest.launchJobs}")
    @ResponseBody
    public ResponseEntity launchSearchQueriesJob() {
        return service.launchSearchQueriesJob();
    }

    @RequestMapping(method = RequestMethod.GET,value = "${spring.data.rest.neighbours}")
    @ResponseBody
    public SearchNeighbours getWordNeighbours(@RequestParam("word") String word) {
        return service.getWordNeighbours(word);
    }
}
