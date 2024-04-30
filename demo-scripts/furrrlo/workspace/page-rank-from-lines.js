const inputFile = engineVars.string("inputFile")
const partitions = engineVars.number("partitions")
const dstDfsFile = engineVars.string("dstDfsFile")
const srcDfsFile = engineVars.string("srcDfsFile")

engine
    .lines({ file: inputFile, partitions, srcDfsFile, dstDfsFile })
    // Links are represented as pairs of page IDs which are separated by space characters. Links
    // are separated by new-line characters.<br>
    // For example <code>"1 2\n2 12\n1 12\n42 63"</code> gives four (directed) links (1)->(2),
    // (2)->(12), (1)->(12), and (42)->(63).
    .flatMap((line) => {
        let links = /** @type {Map<string, string[]>} */ new Map();
        line.split("\n").forEach(link => {
            let src = link.split(" ")[0];
            let dst = link.split(" ")[1];

            links.set(src, (() => {
                let curr = links.get(src) || /** @type string[] */ [];
                curr.push(dst);
                return curr
            })());
        })
        return links
    })
    // A reduce function that takes a sequence of edges and builds
    // the adjacency list for the vertex where the edges originate.
    .reduce((src, dsts) => dsts.reduce((a, b) => a.concat(b)))
    .run('page-rank.js');