// https://github.com/apache/flink/blob/9cc5ab9caf368ef336599e7d48f679c8c9750f49/flink-examples/flink-examples-batch/src/main/java/org/apache/flink/examples/java/graph/PageRank.java
const numOfPages = engineVars.number("numOfPages");
const iterations = engineVars.number("iterations");
const dampeningFactor = engineVars.number("dampeningFactor");

engine
    // Requires a { page: string, adjacencyList: string[] } as input tuples
    .requireInput(e => /** @type {typeof Engine<string, string[]>} */ e)
    // Assign initial rank to pages
    .map((page, neighbors) => {
        return { rank: Number(1.0 / numOfPages), neighbors: neighbors }
    })
    .iterate(iterations, engine => engine
        // Distribute a fraction of a vertex's rank to all neighbors
        .flatMap((page, val) => {
            const rankToDistribute = val.rank / val.neighbors.length;
            const newPages = /** @type {Map<string, { rank: number, neighbors: string[]? }>} */ new Map();
            // Distribute a fraction of a vertex's rank to all neighbors
            val.neighbors.forEach(neighbor => {
                const curr = newPages.get(neighbor) || { rank: 0 };
                curr.rank += rankToDistribute;
                newPages.set(neighbor, curr);
            });
            // Also add the current page with 0 rank so that we can keep the neighbors list
            const curr = newPages.get(page) || { rank: 0 };
            curr.neighbors = val.neighbors;
            newPages.set(page, curr);

            return newPages
        })
        // Collect and sum ranks & restore neighbors adjacency list
        .reduce((page, ranks) => ranks.reduce((
            aVal, bVal
        ) => {
            return { rank: aVal.rank + bVal.rank, neighbors: aVal.neighbors || bVal.neighbors };
        }))
        // Apply the page rank dampening formula &
        .map((page, val) => {
            const randomJump = (1 - dampeningFactor) / numOfPages;
            return {
                rank: Number((val.rank * dampeningFactor) + randomJump),
                neighbors: /** @type string[] */ (val.neighbors || [])
            };
        }))
    // Extract only page&rank
    .map((page, val) => val.rank)

