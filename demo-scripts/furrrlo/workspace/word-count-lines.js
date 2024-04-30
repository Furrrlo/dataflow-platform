engine
    .lines({ file: "kinglear.txt", partitions: 8 })
    .run('word-count.js')

