package com.appmetr.s2s;

import com.appmetr.s2s.persister.FileBatchPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class RebatcherConsole {

    private static final Logger log = LoggerFactory.getLogger(RebatcherConsole.class);

    public static void main(String[] args) throws IOException {
        if(args.length == 0) System.out.println("Usage: <batch dir> <deploy-id> <appmetr-api-uri> (deploy and uri are optional)");

        final String batchesPath = args[0];
        final FileBatchPersister fileBatchPersister = new FileBatchPersister(batchesPath);


        if(args.length > 1) {
            final String deploy = args[1];
            final String url = args.length >= 3 ? args[2] : "http://localhost:8081/api";

            final AppMetr appMetr = new AppMetr(deploy, url, fileBatchPersister);
            System.out.println("Starting batch uploading to "+url+" with deploy token "+deploy);
            System.in.read();

            appMetr.stop();
        }
    }
}
