package com.appmetr.s2s.events;

import java.util.List;

public class Events {

    public static Action serverInstall(String userId) {
        Event event = new Event("server/server_install");
        event.setUserId(userId);
        return event;
    }

    public static Action trackLevel(String userId, int level) {
        AttachProperties event = new AttachProperties();
        event.setUserId(userId);
        event.getProperties().put("$level", level);
        return event;
    }

    public static Action trackAbGroup(String userId, List<String> groups) {
        AttachProperties event = new AttachProperties();
        event.setUserId(userId);
        event.getProperties().put("$abGroup", groups);
        return event;
    }
}
