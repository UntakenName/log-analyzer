var labelType, useGradients, nativeTextSupport, animate, treeInstance;

(function() {
    var ua = navigator.userAgent,
        iStuff = ua.match(/iPhone/i) || ua.match(/iPad/i),
        typeOfCanvas = typeof HTMLCanvasElement,
        nativeCanvasSupport = (typeOfCanvas == 'object' || typeOfCanvas == 'function'),
        textSupport = nativeCanvasSupport
        && (typeof document.createElement('canvas').getContext('2d').fillText == 'function');
    labelType = (!nativeCanvasSupport || (textSupport && !iStuff))? 'Native' : 'HTML';
    nativeTextSupport = labelType == 'Native';
    useGradients = nativeCanvasSupport;
    animate = !(iStuff || !nativeCanvasSupport);
})();

function recompileNeighboursTree(keyWord, treeInstance) {
    $.ajax({
        url: "/wordNeighbours",
        type: 'GET',
        data: {
            word: keyWord
        },
        success: function (responseData) {
           if (responseData && responseData != "") {
               var convertedObject = convertResponseDataJson(responseData);
               treeInstance.loadJSON(convertedObject);
               treeInstance.compute();
               treeInstance.geom.translate(new $jit.Complex(-200, 0), "current");
               treeInstance.onClick(treeInstance.root);
           }
        }
    });
}

class WordNode {
    constructor(word) {
        this.id = word;
        this.name = word;
        this.data = {};
        this.children = [];
    }
}

function convertResponseDataJson(responseData) {

    var node = new WordNode(responseData.word);

    responseData.neighbours.forEach(function(element) {
        node.children.push(new WordNode(element));
    });

    return node;
}

function initNeighboursTree() {

    treeInstance = new $jit.ST({
        injectInto: 'treeContainer',
        duration: 400,
        transition: $jit.Trans.Quart.easeInOut,
        levelDistance: 50,
        Navigation: {
          enable:true,
          panning:true
        },

        orientation: 'top',

        Node: {
            height: 20,
            width: 60,
            type: 'rectangle',
            color: '#aaa',
            overridable: true
        },
        
        Edge: {
            type: 'bezier',
            overridable: true
        },

        onCreateLabel: function(label, node){
            label.id = node.id;            
            label.innerHTML = node.name;
            label.onclick = function() {
                recompileNeighboursTree(node.id, treeInstance);
            };
            var style = label.style;
            style.width = 60 + 'px';
            style.height = 17 + 'px';            
            style.cursor = 'pointer';
            style.color = '#333';
            style.fontSize = '0.8em';
            style.textAlign= 'center';
            style.paddingTop = '3px';
        },

        onBeforePlotNode: function(node){
            if (node.selected) {
                node.data.$color = "#ff7";
            }
            else {
                delete node.data.$color;
                if(!node.anySubnode("exist")) {
                    var count = 0;
                    node.eachSubnode(function(n) { count++; });
                    node.data.$color = ['#aaa', '#baa', '#caa', '#daa', '#eaa', '#faa'][count];                    
                }
            }
        },

        onBeforePlotLine: function(adj){
            if (adj.nodeFrom.selected && adj.nodeTo.selected) {
                adj.data.$color = "#eed";
                adj.data.$lineWidth = 3;
            }
            else {
                delete adj.data.$color;
                delete adj.data.$lineWidth;
            }
        }
    });
}