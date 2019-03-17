$(document).ready(function() {
    $("#analyzeButton").click(function() {
        $.ajax({
            url: "/launchSearchQueriesJob",
            type: 'GET'
        });
    });

    $("#searchButton").click(function() {
        var keyWord = document.getElementById('textInput').value;
        if (keyWord && keyWord.length > 0) {
            recompileNeighboursTree(keyWord);
        }
    });
});