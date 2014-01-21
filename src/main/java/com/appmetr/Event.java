package com.appmetr;

import java.util.Map;

public class Event {

    private String event;
    private Map<String, String> properties;

    public Event(String event, Map<String, String> properties){
        this.event = event;
        this.properties = properties;
    }

    public String getEventName() {
        return event;
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
        StringBuilder strBuilder = new StringBuilder(event);
        strBuilder.append("\r\n");
        for(Map.Entry property : properties.entrySet()){
            strBuilder.append(property.getKey() + " - " + property.getValue());
        }

        return strBuilder.toString();
    }
}
