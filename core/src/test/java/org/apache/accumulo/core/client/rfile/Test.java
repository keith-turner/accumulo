package org.apache.accumulo.core.client.rfile;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.util.FastFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class Test {
    public static void main(String[] args) throws Exception {

        for(int i = 0; i<10;i++) {
            write();
        }
    }

    private static void write() throws IOException {
        try {
            Files.delete(Path.of("/tmp/file1.rf"));
        }catch (NoSuchFileException e){

        }
        long t1 = System.nanoTime();
        var f1 = RFile.newWriter().to("/tmp/file1.rf").build();
        f1.startDefaultLocalityGroup();
        for(int i = 0; i < 1000000; i++) {
            String row = String.format("%09x", i);
            String fam= Integer.toString(i%1000,36);
            String value = (i*31)+"";
            f1.append(new Key(row, fam), value);
        }

        f1.close();
        long t2 = System.nanoTime();

        System.out.printf("%,d ms\n", TimeUnit.NANOSECONDS.toMillis(t2-t1));
    }
}
