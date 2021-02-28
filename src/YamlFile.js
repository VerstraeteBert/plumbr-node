class YamlFile {
    name = ''
    contents = ''
    dir = ''

    constructor(name, dir, contents) {
        this.name = name
        this.contents = contents
        this.dir = dir
    }
}

module.exports = YamlFile
