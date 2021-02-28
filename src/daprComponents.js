const NodeKinds = require('./NodeKinds')
const YamlFile = require('./YamlFile')

// TODO auth
//      brokers (not hardwired)
//      super class to hold project config
// ref: https://docs.dapr.io/reference/api/bindings_api/
// Note: while dapr takes care if at-least-once delivery to each unique processor that is registered,
//       it would be impossible to autoscale granularly if the same consumer group is used for each of the interested processors
//       Thus, a different input binding is needed per processor
class DaprSource {
    namespace = ''
    brokers = []
    name = ''
    topic = ''
    subscribers = []

    constructor(source, projectConfig) {
        this.namespace = projectConfig.name
        this.brokers = projectConfig.brokers
        this.name = source.name
        this.topic = projectConfig.sourceToTopicsMap[source.name]
        this.addSubscriber(source.connectsTo)
    }

    addSubscriber(processorName) {
        this.subscribers.push(processorName);
    }

    getInputUrlForSubscriber(sub) {
        return `/${this.name}-${sub}`
    }

    toYaml() {
       return this.subscribers.map(sub => {
            const inputBindingContent =  `
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: ${this.name}-${sub}-binding
  namespace: ${this.namespace}
spec:
  type: bindings.kafka
  version: v1
  metadata:
    - name: brokers
      value: ${this.brokers.join(',')}
    - name: authRequired
      value: "false"
    - name: topics
      value: ${this.topic}
    - name: consumerGroup
      value: ${this.namespace}-${sub}-grp
`
           return new YamlFile(`${this.name}-${sub}-binding.yaml`, 'bindings', inputBindingContent)
        })
    }
}

// ref: https://docs.dapr.io/reference/api/bindings_api/
// a single kafka egress binding for multiple stream producers is ok
class DaprSink {
    namespace = ''
    brokers = []
    name = ''
    topic = ''

    constructor(sink, projectConfig) {
        this.namespace = projectConfig.name
        this.brokers = projectConfig.brokers
        this.name = sink.name
        this.topic = projectConfig.sinkToTopicsMap[sink.name]
    }

    getOutputUrl() {
        return `http://localhost:3500/v1.0/bindings/${this.#getComponentName()}/`
    }

    #getComponentName() {
        return `${this.name}-binding`
    }

    toYaml() {
        const yamlFiles = []
            const outputBindingContent =  `
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: ${this.#getComponentName()}
  namespace: ${this.namespace}
spec:
  type: bindings.kafka
  version: v1
  metadata:
    - name: brokers
      value: ${this.brokers.join(',')}
    - name: authRequired
      value: "false"
    - name: publishTopic
      value: ${this.topic}
`
        yamlFiles.push(new YamlFile(`${this.#getComponentName()}.yaml`, 'bindings', outputBindingContent))
        return yamlFiles
    }
}

// A single processor needs
// 1. A Kubernetes Deployment and Service
//      - with appropriate input and output url(s)
//      - proper dapr annotations (sidecar injection, app-id, application port, observability config)
// 2. Appropriate topics need be made for each unique output stream (with sensible defaults)
// 3. A PubSub component if connected with other processor (the component is created by the stream producer)
//      - Each PubSub consists of
//          - A Dapr broker:
//              - Has a singular consumer-group
//              - A seperate broker is needed for each
//          - A Dapr subscription:
//              - binds to a broker
//              - Consists of a topic, and route on which to send messages to subscribers
//      - To make it scale, we need a broker and subscription for each different inter-processor edge
// 4. A Keda autoscaling component based on input topic(s)
//      - if connected to a input binding -> topic supplied by engineer
//      - if inter-processor -> topic generated
class DaprProcessor {
    namespace = ''
    name = ''
    brokers = []
    inputTopic = ''
    isInputFromProcessor = false
    outputTopic = ''
    env = new Map()

    constructor(processor, projectConfig) {
        this.namespace = projectConfig.name
        this.name = processor.name
        this.brokers = projectConfig.brokers
        this.env = processor.env
    }

    registerInput(inputName, kind, projectConfig) {
        switch (kind) {
            case NodeKinds.SOURCE:
                this.env.set("IS_SOURCE", "true")
                this.env.set("INPUT_ROUTE", `/${inputName}-${this.name}-binding`)
                this.inputTopic = projectConfig.sourceToTopicsMap[inputName]
                break
            case NodeKinds.PROCESSOR:
                this.env.set("IS_SOURCE", "false")
                this.env.set("INPUT_ROUTE", `/${this.name}`)
                this.inputTopic = `${this.namespace}-${inputName}-stream`
                this.isInputFromProcessor = true
                break
            default:
                throw new Error(`Invalid input stream kind to processor: ${kind}`)
        }
    }

    registerOutput(outputName, kind, projectConfig) {
        switch(kind) {
            case NodeKinds.SINK:
                this.env.set("IS_SINK", "true")
                this.env.set("OUTPUT_ROUTE", `http://localhost:3500/v1.0/bindings/${outputName}-binding`)
                this.outputTopic = projectConfig.sinkToTopicsMap[outputName]
                break
            case NodeKinds.PROCESSOR:
                this.env.set("IS_SINK", "false")
                this.env.set("OUTPUT_ROUTE", `http://localhost:3500/v1.0/publish/kafka-broker-${outputName}/${this.namespace}-${this.name}-stream`)
                this.outputTopic = `${this.namespace}-${this.name}-stream`
                break
            default:
                throw new Error(`Invalid input stream kind to processor: ${kind}`)
        }
    }

    toYaml() {
        const yamlFiles = []
        // TODO app port
        const serviceContents = `
kind: Service
apiVersion: v1
metadata:
  name: ${this.name}
  namespace: ${this.namespace}
  labels:
    app: ${this.name}
spec:
  selector:
    app: ${this.name}
  ports:
  - protocol: TCP
    port: 80
    targetPort: 5000
  type: LoadBalancer
`
        // TODO image name & port
        yamlFiles.push(new YamlFile(`${this.name}-service.yaml`, 'deployments', serviceContents))
        let deployContents = `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${this.name}
  namespace: ${this.namespace}
  labels:
    app: ${this.name}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${this.name}
  template:
    metadata:
      labels:
        app: ${this.name}
      annotations:
        dapr.io/enabled: "true"
        dapr.io/app-id: "${this.name}"
        dapr.io/app-port: "5000"
        dapr.io/config: "tracing-config"
    spec:
      containers:
      - name: ${this.name}
        image: verstraetebert/simple-transformer:1
        ports:
        - containerPort: 5000
        imagePullPolicy: Always
        env:`
        for (const [key, val] of this.env.entries()) {
            deployContents += `
        - name: ${key}
          value: "${val}"`
        }
        yamlFiles.push(new YamlFile(`${this.name}-deploy.yaml`, 'deployments', deployContents))
        // TODO configurable replica #, lagthreshold, pollingInterval, ...
        const kedaFileContents = `
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: ${this.namespace}-${this.name}-scaler
  namespace: ${this.namespace}
spec:
  scaleTargetRef:
    kind: Deployment
    name: ${this.name}
  pollingInterval: 15
  maxReplicaCount: 5
  minReplicaCount: 0
  triggers:
  - type: kafka
    metadata:
      topic: ${this.inputTopic}
      bootstrapServers: ${this.brokers.join(',')}
      consumerGroup: ${this.namespace}-${this.name}-grp
      lagThreshold: "5" 
`
        yamlFiles.push(new YamlFile(`${this.name}-scaler.yaml`, 'scaling', kedaFileContents))

        if (this.isInputFromProcessor) {
            const brokerContents = `
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: kafka-broker-${this.name}
  namespace: ${this.namespace}
spec:
  type: pubsub.kafka
  version: v1
  metadata:
  # connection settings
  - name: brokers
    value: ${this.brokers.join(',')}
  - name: authRequired
    value: "false"
  - name: maxMessageBytes
    value: 1024
  - name: consumerID
    value: ${this.namespace}-${this.name}-grp
`
            yamlFiles.push(new YamlFile(`kafka-broker-${this.name}.yaml`, 'streams', brokerContents))

            const subscriptionContent = `
apiVersion: dapr.io/v1alpha1
kind: Subscription
metadata:
  name: ${this.inputTopic}-${this.name}-subscription
  namespace: ${this.namespace}
spec:
  topic: ${this.inputTopic}
  route: /${this.name}
  pubsubname: kafka-broker-${this.name}
scopes:
  - ${this.name}
`
            yamlFiles.push(new YamlFile(`${this.inputTopic}-${this.name}-subscription.yaml`, 'streams', subscriptionContent))

            const strimziTopicContents = `
apiVersion: kafka.strimzi.io/v1beta1
kind: KafkaTopic
metadata:
  name: ${this.inputTopic}
  namespace: kafka
  labels:
    strimzi.io/cluster: my-cluster
spec:
  partitions: 5
  replicas: 1
  config:
    retention.ms: 7200000
    segment.bytes: 1073741824
`
            yamlFiles.push(new YamlFile(`${this.inputTopic}.yaml`, 'topics', strimziTopicContents))
        }
        return yamlFiles
    }
}

class DaprConfig {
    namespace = ''
    name = ''
    tracingEndpoint = ''

    constructor(projectConfig) {
        this.namespace = projectConfig.name
        this.name = 'tracing-config'
        this.tracingEndpoint = projectConfig.observability.tracingEndpoint
        this.tracingSamplingRate = projectConfig.observability.tracingSamplingRate
    }

    toYaml() {
        const yamlFiles = []
        const monitoringConfigContent =  `
apiVersion: dapr.io/v1alpha1
kind: Configuration
metadata:
  name: ${this.name}
  namespace: ${this.namespace}
spec:
  tracing:
    samplingRate: "${this.tracingSamplingRate}"
    zipkin:
      endpointAddress: ${this.tracingEndpoint}
`
        yamlFiles.push(new YamlFile(`${this.name}.yaml`, 'monitoring', monitoringConfigContent))
        return yamlFiles
    }
}


module.exports = {
    source: DaprSource,
    sink: DaprSink,
    processor: DaprProcessor,
    monitoring: DaprConfig
}
