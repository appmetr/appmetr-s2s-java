package com.appmetr.s2s;

import com.appmetr.s2s.persister.FileBatchPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RebatcherConsole {

    private static final Logger log = LoggerFactory.getLogger(RebatcherConsole.class);

    public static void main(String[] args) throws IOException {
        for (String arg : args) {
            log.info("Searching for batches in: "+arg);
        }

        final FileBatchPersister fileBatchPersister = new FileBatchPersister(args[0]);
        //fileBatchPersister.rebatch();

        //final AppMetr appMetr = new AppMetr("3aef1916-3964-45cb-a7e4-c53011de6c98", "http://localhost:8081/api", fileBatchPersister);
        final AppMetr appMetr = new AppMetr("773f7b6b-64d4-439b-889b-49c49d4fe146", "http://localhost:8081/api", fileBatchPersister);

        System.in.read();

        appMetr.stop();
    }
}
