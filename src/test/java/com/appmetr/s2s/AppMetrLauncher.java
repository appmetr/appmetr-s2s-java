package com.appmetr.s2s;

import com.appmetr.s2s.events.Action;
import com.appmetr.s2s.events.Event;
import com.appmetr.s2s.persister.FileBatchPersister;

public class AppMetrLauncher {
    public static final String DEPLOY = "070e247a-2040-4d88-b14c-50ec9e58c1fd";
    public static final String URL = "http://localhost:8080/api";

    public static void main(String[] args) {
        final AppMetr appMetr = new AppMetr(DEPLOY, URL, new FileBatchPersister("target"));

        Action action = new Event("Hello");

        appMetr.track(action);

        appMetr.stop();
    }
}
