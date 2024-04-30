engine
    .lines({ file: "kinglear.txt", partitions: 8 })
    .flatMap(function(line, _) {
        let words = /** @type {Map<string, number>} */ new Map();
        line.split(/(\s+)/).forEach(function(word) {
            word = word.trim();
            if(word.length !== 0)
                words.set(word, (words.get(word) || 0) + 1)
        });
        return words
    })
    .changeKey((word, _) => word.toLowerCase())
    .reduce(function(word, counts) {
        return counts.reduce((a, b) => a + b, 0);
    })