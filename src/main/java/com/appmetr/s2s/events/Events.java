package com.appmetr.s2s.events;

public class Events {

    public static Action serverInstall() {
        return new Event("server/server_install");
    }
}
