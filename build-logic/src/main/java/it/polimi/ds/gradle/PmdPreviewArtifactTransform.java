package it.polimi.ds.gradle;

import kotlin.io.FilesKt;
import org.gradle.api.artifacts.transform.*;
import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Classpath;
import org.jetbrains.annotations.Nullable;
import org.objectweb.asm.*;
import org.objectweb.asm.commons.AnalyzerAdapter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.Objects;

@CacheableTransform
public abstract class PmdPreviewArtifactTransform implements TransformAction<TransformParameters.None> {

    private static final String PMD_JAVA_MODULE_PATH = "/net/sourceforge/pmd/lang/java/JavaLanguageModule.class";

    @Classpath
    @InputArtifact
    protected abstract Provider<FileSystemLocation> getInputArtifact();

    @Override
    public void transform(TransformOutputs outputs) {
        var inJar = getInputArtifact().get().getAsFile().toPath();
        System.out.println("[PmdPreviewArtifactTransform] Checking " + inJar.getFileName() + "...");

        try (FileSystem inZipFs = FileSystems.newFileSystem(inJar)) {
            final var inTargetClass  = inZipFs.getPath(PMD_JAVA_MODULE_PATH);
            if(!Files.exists(inTargetClass) || !Files.isRegularFile(inTargetClass)) {
                System.out.println("[PmdPreviewArtifactTransform] " + inJar.getFileName() +
                        " does not require transforming");
                outputs.file(getInputArtifact());
                return;
            }

            final var nameWithoutExtension = FilesKt.getNameWithoutExtension(inJar.getFileName().toFile());
            final var outJar = outputs.file(nameWithoutExtension + "-preview.jar").toPath();
            System.out.println("[PmdPreviewArtifactTransform] Transforming " + inJar.getFileName() +
                    " to " + outJar + "...");
            Files.copy(inJar, outJar, StandardCopyOption.COPY_ATTRIBUTES, LinkOption.NOFOLLOW_LINKS);

            try (FileSystem outZipFs = FileSystems.newFileSystem(outJar)) {
                final var outTargetClass = outZipFs.getPath(PMD_JAVA_MODULE_PATH);
                transform(inTargetClass, outTargetClass);
            }

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void transform(Path srcClass, Path targetClass) throws IOException {
        var cr = new ClassReader(Files.newInputStream(srcClass));
        var cw = new ClassWriter(cr, 0);
        cr.accept(new EnablePreviewClassVisitor(cw), ClassReader.EXPAND_FRAMES);
        Files.write(targetClass, cw.toByteArray());
    }

    private static class EnablePreviewClassVisitor extends ClassVisitor {

        private @Nullable String owner;

        public EnablePreviewClassVisitor(ClassWriter cw) {
            super(Opcodes.ASM9, cw);
        }

        @Override
        public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
            this.owner = name;
            super.visit(version, access, name, signature, superName, interfaces);
        }

        @Override
        public MethodVisitor visitMethod(int access, String name, String descriptor, String signature, String[] exceptions) {
            final var mv = super.visitMethod(access, name, descriptor, signature, exceptions);
            if(!name.equals("<init>"))
                return mv;

            System.out.println("[PmdPreviewArtifactTransform] Found method " + name);
            return new EnablePreviewMethodVisitor(api, Objects.requireNonNull(owner), access, name, descriptor, mv);
        }
    }

    private static class EnablePreviewMethodVisitor extends AnalyzerAdapter {

        private @Nullable Object lastLdc;
        private @Nullable Integer lastLdcStackIndex;
        private @Nullable String defaultVersion;
        private boolean done;

        public EnablePreviewMethodVisitor(int api,
                                          String owner,
                                          int access,
                                          String name,
                                          String descriptor,
                                          MethodVisitor mv) {
            super(api, owner, access, name, descriptor, mv);
        }

        @Override
        public void visitLdcInsn(Object value) {
            super.visitLdcInsn(value);
            lastLdc = value;
            lastLdcStackIndex = stack.size() - 1;
        }

        @Override
        public void visitMethodInsn(int opcode,
                                    String owner,
                                    String name,
                                    String descriptor,
                                    boolean isInterface) {
            var lastLdc = this.lastLdc;
            var lastLdcStackIndex = this.lastLdcStackIndex;
            this.lastLdc = null;
            this.lastLdcStackIndex = null;

            if(done ||
                    (opcode != Opcodes.INVOKEVIRTUAL && opcode != Opcodes.INVOKEINTERFACE) ||
                    !owner.equals("net/sourceforge/pmd/lang/LanguageModuleBase$LanguageMetadata") ||
                    !(lastLdc instanceof String currentVersion) ||
                    !Objects.equals(lastLdcStackIndex, stack.size() - 2) ||
                    !descriptor.startsWith("(Ljava/lang/String;[Ljava/lang/String;)")) {

                super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
                return;
            }

            // addDefaultVersion("21", ...)
            if(defaultVersion == null && name.equals("addDefaultVersion")) {
                defaultVersion = currentVersion;
                super.visitInsn(Opcodes.POP2);
                System.out.println("[PmdPreviewArtifactTransform] Found default version " + defaultVersion);
                return;
            }

            // addVersion("21-preview", ...)
            if(defaultVersion != null &&
                    name.equals("addVersion") &&
                    currentVersion.equals(defaultVersion + "-preview")) {
                super.visitInsn(Opcodes.POP2);

                super.visitLdcInsn(defaultVersion);
                super.visitIntInsn(Opcodes.BIPUSH, 0);
                super.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/String");
                super.visitMethodInsn(opcode, owner, "addVersion", descriptor, isInterface);

                super.visitLdcInsn(currentVersion);
                super.visitIntInsn(Opcodes.BIPUSH, 0);
                super.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/String");
                super.visitMethodInsn(opcode, owner, "addDefaultVersion", descriptor, isInterface);

                System.out.println("[PmdPreviewArtifactTransform] Set PMD default Java version to " + currentVersion);
                done = true;
                return;
            }

            super.visitMethodInsn(opcode, owner, name, descriptor, isInterface);
        }
    }
}
