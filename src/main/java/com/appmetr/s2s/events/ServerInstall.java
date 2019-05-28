package com.appmetr.s2s.events;

public class ServerInstall {

    public static Action create() {
        return new Event("server/server_install");
    }
}
