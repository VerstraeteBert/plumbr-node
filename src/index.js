#!/usr/bin/env node

const fs = require('fs')
const fsp = require('fs').promises
const path = require('path')
const yaml = require('js-yaml')
const getOpts = require('./get-opts')
const StreamTopology = require('./StreamTopology')
const NodeKinds = require('./NodeKinds')
const TopologyDiGraph = require('./TopologyDiGraph')

const SORT_MARKS = {
    NONE: 0,
    TEMP: 1,
    PERM: 2
}

const topoSort = (graph) => {
    const topoSort = []
    const marks = new Map()
    graph.nodes.forEach(el => {
        marks.set(el.name, SORT_MARKS.NONE);
    })
    graph.nodes.forEach(node => {
        topoSortHelper(node, graph, topoSort, marks)
    })
    return topoSort.reverse();
}

const topoSortHelper = (currNode, graph, topoSort, marks) => {
    if (marks[currNode] === SORT_MARKS.PERM) {
        return
    }
    if (marks[currNode] === SORT_MARKS.TEMP) {
        throw new Error("The supplied graph contains a cycle")
    }
    marks[currNode] = SORT_MARKS.TEMP
    graph.adjList.get(currNode).forEach(neighbor => {
        topoSortHelper(neighbor, graph, topoSort, marks)
    })
    marks[currNode] = SORT_MARKS.PERM
    topoSort.push(currNode)
}

const buildAndValidateTopologyDag = (streamTopology) => {
    const topoDiGraph = new TopologyDiGraph()
    // add all components of topology to graph as nodes
    for (const name of [...streamTopology.sources.keys(),...streamTopology.sinks.keys(),...streamTopology.processors.keys()]) {
        topoDiGraph.addNode(name);
    }
    // connect sources to processors with edges
    for (const [sourceName, sourceValues] of streamTopology.sources.entries()) {
        // sources can only connect to processors
        const connectedProcessorName = sourceValues.connectsTo;
        if (!streamTopology.processors.has(connectedProcessorName)) {
            throw new Error(`Connected node described in source ${sourceName} with name ${connectedProcessorName} does not exist or is not a processor`)
        }
        topoDiGraph.addEdge(sourceName, connectedProcessorName)
    }
    // connect processors to other processors or sink
    for (const [processorName, processorValues] of streamTopology.processors.entries()) {
        const connectedComponentName = processorValues.connectsTo;
        if (!streamTopology.processors.has(connectedComponentName) && !streamTopology.sinks.has(connectedComponentName)) {
            throw new Error(`Connected node describe in processor ${processorName} with value ${connectedComponentName} does not exist or is not a processor or sink`)
        }
        topoDiGraph.addEdge(processorName, connectedComponentName)
    }
    // TODO check if all processors and sinks have an incoming connection
    // sort topologically (and detect cycles -> if cycles it cannot be DAG)
    console.log(`Nodes in order given in the input yaml: ${topoDiGraph.nodes}`)
    const topologicalSorting = topoSort(topoDiGraph);
    console.log(`Nodes after topological sort: ${topologicalSorting}`)
    return {
        topoDiGraph: topoDiGraph,
        topoSort: topologicalSorting,
    }
}

const buildYamls = (streamTopo, topoSort) => {
    // loop over all components and initialize their target specific implementation (currently only dapr)
    // TODO abstraction for different targets
    //      projectConfig fetching for project (broker, input, output topics, topic replication, etc)
    // hardwired for now :-)
    const projectConfig = {
        name: streamTopo.name,
        brokers: ['my-cluster-kafka-brokers.kafka:9092'],
        sourceToTopicsMap: {
            'kafka-ingress-0': 'kafka-ingress-0',
            'kafka-ingress-1': 'kafka-ingress-1'
        },
        sinkToTopicsMap: {
            'kafka-egress-0': 'kafka-egress-0',
            'kafka-egress-1': 'kafka-egress-1'
        },
        observability: {
            'tracingEndpoint': 'http://zipkin.observability:9411/api/v2/spans',
            'tracingSamplingRate': '1'
        }
    }

    const targetComponentClasses = require('./daprComponents')
    const targetComponents = new Map()
    const yamlFiles = []
    // initialize components for target
    for (const compName of topoSort) {
        const kind = streamTopo.nameToKindMap.get(compName);
        const currComp = streamTopo.getComp(compName, kind)
        const targetComponent = new targetComponentClasses[kind](currComp, projectConfig)

        if (kind === NodeKinds.PROCESSOR) {
            // plumbing work
            // TODO support fan-out
            const connectedToCompKind = streamTopo.nameToKindMap.get(currComp.connectsTo)
            targetComponent.registerOutput(currComp.connectsTo, connectedToCompKind, projectConfig)
            const sourceCompKind = streamTopo.nameToKindMap.get(currComp.input)
            targetComponent.registerInput(currComp.input, sourceCompKind, projectConfig)
        }
        targetComponents.set(compName, targetComponent)
        yamlFiles.push(...targetComponent.toYaml())
    }

    const monitoringComponent = new targetComponentClasses.monitoring(projectConfig)
    yamlFiles.push(...monitoringComponent.toYaml())
    return yamlFiles;
}

const outputYamlFiles = async (yamlFiles, output) => {
    try {
        for (const yamlFile of yamlFiles) {
            const destFolder = path.join(__dirname, output, yamlFile.dir)
            fs.mkdirSync(destFolder, { recursive: true })
            await fsp.writeFile(path.join(destFolder, yamlFile.name), yamlFile.contents)
        }
    } catch (e) {
        console.log(e)
    }
}

const main = async () => {
    const { file, output, target } = getOpts()
    let doc;
    try {
        doc = yaml.load(fs.readFileSync(file, 'utf8'));
        console.log(doc);
    } catch (e) {
        console.log(e);
        return;
    }

    // todo validation with joi
    const streamTopo = new StreamTopology(doc.name)

    if (doc.steps === undefined) {
        throw new Error("No steps defined.")
    }

    const foundNames = new Set();
    doc.steps.forEach(step => {
        if (foundNames.has(step.name)) {
            throw new Error(`Multiple steps with name ${step.name} defined`);
        }
        switch (step.kind) {
            case NodeKinds.SOURCE:
                streamTopo.addSource(step.name, step.connectsTo)
                break
            case NodeKinds.PROCESSOR:
                streamTopo.addProcessor(step.name, step.connectsTo, step.env)
                break
            case NodeKinds.SINK:
                streamTopo.addSink(step.name)
                break
            default:
                throw new Error(`Invalid step kind (${step.kind}) supplied must be one of source|processor|sink`)
        }
    })


    for (const source of [...streamTopo.sources.values(), ...streamTopo.processors.values()]) {
        const connectsTo = source.connectsTo;
        if (streamTopo.nameToKindMap.get(connectsTo) === undefined) {
            throw new Error(`ConnectsTo invalid for ${source.name} and connected component ${connectsTo}: it does not exist`)
        }
        const connectedType = streamTopo.nameToKindMap.get(connectsTo)
        streamTopo.registerInput(source.name, connectsTo, connectedType)
    }

    const { topoSort, topoDiGraph } = buildAndValidateTopologyDag(streamTopo);
    const yamlFiles = buildYamls(streamTopo, topoSort)
    await outputYamlFiles(yamlFiles, output)
}

(async () => {
    await main()
})()
