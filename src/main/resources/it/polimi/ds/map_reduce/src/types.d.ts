// @ts-ignore
type FlatMapFn<K, V, K1, V1> = (k: K, v: V) => Map<K1, V1>;
type MapFn<K, V, V1> = (k: K, v: V) => V1
type FilterFn<K, V> = (k: K, v: V) => boolean;
type ChangeKeyFn<K, V, K1> = (k: K, v: V) => K1;
type ReduceFn<K, V, V1> = (k: K, vls: [V]) => V1;

interface EngineSrc {
    lines: (file: string) => EngineOptions<string, void>
    csv: (file: string, delimeter?: string) => EngineOptions<string, string>
}

interface EngineOptions<K, V> {
    partitions: (n: Number) => Engine<K, V>
}

interface Engine<K, V> {
    flatMap: <K1, V1> (fn: FlatMapFn<K, V, K1, V1>) => Engine<K1, V1>,
    map: <V1> (fn: MapFn<K, V, V1>) => Engine<K, V1>,
    filter: (fn: FilterFn<K, V>) => Engine<K, V>,
    changeKey: <K1> (fn: ChangeKeyFn<K, V, K1>) => Engine<K1, V>,
    reduce: <V1> (fn: ReduceFn<K, V, V1>) => void,
}

declare var engine: EngineSrc;