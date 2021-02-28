class TopologyDiGraph {
    constructor() {
        this.nodes = []
        this.adjList = new Map()
    }

    addNode(name) {
        this.nodes.push(name)
        this.adjList.set(name, [])
    }

    addEdge(src, dst) {
        this.adjList.get(src).push(dst);
    }
}

module.exports = TopologyDiGraph;
