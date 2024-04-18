engine
    .setup(engine => {
        return engine
            .declareVar("partitions", 8)
            .declareVar("inputFile", "links.txt")
            .declareVar("numOfPages", 15)
            .declareVar("iterations", 10)
            .declareVar("dampeningFactor", 0.85)
    })
    .exec(() => {
        const fileName = "page-rank-from-lines.js";

        engine
            .requireInput()
            .run(fileName);
    })