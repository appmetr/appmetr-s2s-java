package com.appmetr;

import java.util.Map;

public class Event {
    private String eventId;
    private Map<String, String> properties;

    public Event(String eventId, Map<String, String> properties){
        this.eventId = eventId;
        this.properties = properties;
    }

    public String getEventId() {
        return eventId;
    }

    public Map<String, String> getProperties(){
        return properties;
    };

    public int calcApproximateSize() {
        int propertiesSize = 40 + (40 * properties.size()); //40 - Map size and 40 - each entry overhead
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            propertiesSize += entry.getKey().length() * 2 + 24 + 12;    //24 - String object size, 16 - char[]
            propertiesSize += entry.getValue().length();
        }

        return 8 + propertiesSize + 8; //8 - object header, 8 - double
    }

    @Override public String toString(){
        StringBuilder strBuilder = new StringBuilder(eventId);
        strBuilder.append("\r\n");
        for(Map.Entry property : properties.entrySet()){
            strBuilder.append(property.getKey() + " - " + property.getValue());
        }

        return strBuilder.toString();
    }
}
