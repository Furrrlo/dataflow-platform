engine
    .lines("links.txt", 8)
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
    // // Assign initial rank to pages
    .map((page, neighbors) => {
        const numOfPages = 15.0; // TODO
        return { rank: Number(1.0 / numOfPages), neighbors: neighbors }
    })
    .iterate(10, engine => engine
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
            const numOfPages = 15.0; // TODO
            const dampeningFactor = 0.85;
            const randomJump = (1 - dampeningFactor) / numOfPages;
            return {
                rank: Number((val.rank * dampeningFactor) + randomJump),
                neighbors: /** @type string[] */ (val.neighbors || [])
            };
        }))
    // Extract only page&rank
    .map((page, val) => val.rank)

