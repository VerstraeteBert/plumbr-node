const yargs = require('yargs')

const getOpts = () => {
    const argv = yargs
        .option('f', {
            type: 'string',
            alias: 'file',
            description: 'YAML File containing DAG description'
        }).option('t', {
            type: 'string',
            alias: 'target',
            description: 'What deployment target is expected (dapr|knative)'
        }).option('o', {
            type: 'string',
            alias: 'output',
            description: 'To what directory should the Kube objects be written?'
        })
        .strict()
        .parse()

    return {
        file: argv.file,
        target: argv.target,
        output: argv.output
    }
}

module.exports = getOpts;