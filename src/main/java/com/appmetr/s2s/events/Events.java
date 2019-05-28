package com.appmetr.s2s.events;

public class Events {

    public static Action serverInstall(String userId) {
        Event event = new Event("server/server_install");
        event.setUserId(userId);
        return event;
    }
}
