const NodeKinds = require('./NodeKinds')

class StreamTopology {
    name = ''
    sources = new Map()
    sinks = new Map()
    processors = new Map()
    nameToKindMap = new Map()

    constructor(name) {
        if (name === undefined) {
            throw new Error('Topology name not present')
        }
        this.name = name
    }

    getComp(name, kind) {
        let currComp;
        switch (kind) {
            case (NodeKinds.SINK):
                currComp = this.sinks.get(name)
                break
            case (NodeKinds.PROCESSOR):
                currComp = this.processors.get(name)
                break
            case (NodeKinds.SOURCE):
                currComp = this.sources.get(name)
                break
            default:
                console.log("Requested invalid component kind or the component with given name and kind does not exist")
        }
        return currComp
    }

    setComp(name, kind, comp) {
        switch (kind) {
            case (NodeKinds.SINK):
                this.sinks.set(name, comp)
                break
            case (NodeKinds.PROCESSOR):
                this.processors.set(name, comp)
                break
            case (NodeKinds.SOURCE):
                this.sources.set(name, ciomp)
                break
            default:
                console.log("Invalid component type supplied")
        }
    }

    registerInput = (srcCompName, destCompName, destKind) => {
        const destComp = this.getComp(destCompName, destKind)
        destComp.input = srcCompName;
        this.setComp(destComp.name, destKind, destComp)
    }

    addSource = (name, connectsTo) => {
        if (name === undefined || connectsTo === undefined) {
            throw new Error('Source name or connectsTo value not present')
        }
        this.sources.set(name, {
            name: name,
            connectsTo: connectsTo,
        })
        this.nameToKindMap.set(name, NodeKinds.SOURCE)
    }

    addProcessor = (name, connectsTo, env) => {
        if (name === undefined || connectsTo === undefined) {
            throw new Error('Source name or connectsTo value undefined')
        }
        // TODO validate env
        const envMap = new Map();
        if (env !== undefined) {
            for (const envObj of env) {
                envMap.set(envObj.name, envObj.value)
            }
        }
        this.processors.set(name, {
            name: name,
            connectsTo: connectsTo,
            env: envMap,
        })
        this.nameToKindMap.set(name, NodeKinds.PROCESSOR)
    }

    addSink = (name) => {
        if (name === undefined) {
            throw new Error('Sink name undefined')
        }
        this.sinks.set(name, {
            name: name
        })
        this.nameToKindMap.set(name, NodeKinds.SINK)
    }
}

module.exports = StreamTopology;
