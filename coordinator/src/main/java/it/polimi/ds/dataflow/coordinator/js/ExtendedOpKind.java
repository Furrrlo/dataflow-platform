package it.polimi.ds.dataflow.coordinator.js;

import it.polimi.ds.dataflow.js.OpKind;
import it.polimi.ds.dataflow.utils.SuppressFBWarnings;
import org.jetbrains.annotations.Unmodifiable;

import java.util.List;
import java.util.stream.Stream;

@SuppressFBWarnings("IC_SUPERCLASS_USES_SUBCLASS_DURING_INITIALIZATION") // See https://github.com/spotbugs/spotbugs/issues/1360
sealed interface ExtendedOpKind permits ExtendedOpKind.Normal, ExtendedOpKind.Ext {

    @Unmodifiable List<ExtendedOpKind> VALUES = Stream.concat(Normal.VALUES.stream(), Ext.VALUES.stream()).toList();

    String getName();

    @SuppressFBWarnings("IMC_IMMATURE_CLASS_WRONG_FIELD_ORDER") // It's a goddamn record, I can't reorder them, see https://github.com/mebigfatguy/fb-contrib/issues/430
    record Normal(OpKind kind) implements ExtendedOpKind {

        public static final @Unmodifiable List<Normal> VALUES = OpKind.VALUES.stream()
                .map(Normal::new)
                .toList();

        @Override
        public String getName() {
            return kind.getName();
        }
    }

    enum Ext implements ExtendedOpKind {
        ITERATE("iterate"), RUN("run");

        public static final @Unmodifiable List<Ext> VALUES = List.of(values());

        private final String name;

        Ext(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
