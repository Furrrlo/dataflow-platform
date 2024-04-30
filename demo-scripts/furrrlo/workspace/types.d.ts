type FlatMapFn<K, V, K1, V1> = (k: K, v: V) => Map<K1, V1>;
type MapFn<K, V, V1> = (k: K, v: V) => V1
type FilterFn<K, V> = (k: K, v: V) => boolean;
type ChangeKeyFn<K, V, K1> = (k: K, v: V) => K1;
type ReduceFn<K, V, V1> = (k: K, vls: [V]) => V1;
type IterateFn<K, V> = (engine: Engine<K, V>) => Engine<K, V>;

interface EngineSetup {
    setup: (fn: (engine: ConfiguredEngine) => ConfiguredEngine) => ConfiguredEngineRun,
}

interface ConfiguredEngine {
    ___do_not_implement_yourself: void,
    declareVar: (name: string, value: string | number | boolean) => ConfiguredEngine;
}

interface ConfiguredEngineRun {
    exec: (fn: () => void) => void,
}

interface EngineVars {
    number: (name: string) => number,
    string: (name: string) => string,
}

interface EngineSrc {
    lines: (obj: { file: string, partitions: Number }) => Engine<string, void>
    csv: (obj: { file: string, partitions: Number, delimiter?: string }) => Engine<string, string>
    dfs: (obj: { file: string }) => Engine<string, string>
    requireInput: <K, V> (fn?: (engine: Engine<unknown, unknown>) => Engine<K, V>) => Engine<K, V>,
}

interface Engine<K, V> {
    flatMap: <K1, V1> (fn: FlatMapFn<K, V, K1, V1>) => Engine<K1, V1>,
    map: <V1> (fn: MapFn<K, V, V1>) => Engine<K, V1>,
    filter: (fn: FilterFn<K, V>) => Engine<K, V>,
    changeKey: <K1> (fn: ChangeKeyFn<K, V, K1>) => Engine<K1, V>,
    reduce: <V1> (fn: ReduceFn<K, V, V1>) => Engine<K, V1>,
    iterate: (iterations: number, fn: IterateFn<K, V>) => Engine<K, V>,
    run: (file: string) => Engine<any, any>,
}

declare var engineVars: EngineVars;
declare var engine: EngineSrc & EngineSetup;