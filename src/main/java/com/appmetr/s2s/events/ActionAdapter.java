package com.appmetr.s2s.events;

import com.google.gson.*;

import java.lang.reflect.Type;

public class ActionAdapter implements JsonSerializer<Action>, JsonDeserializer<Action> {
    private final String CLASS_NAME = "cls";
    private final String INSTANCE = "inst";

    private final Gson gson = new GsonBuilder().create();

    @Override public JsonElement serialize(Action action, Type type, JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        result.addProperty(CLASS_NAME, action.getClass().getSimpleName());
        result.add(INSTANCE, gson.toJsonTree(action));

        return result;
    }

    @Override public Action deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
        JsonObject obj = jsonElement.getAsJsonObject();
        if (!obj.has(CLASS_NAME)) {
            return context.deserialize(jsonElement, type);
        } else {
            try {
                Class cls = Class.forName("com.appmetr.s2s.events." + obj.get(CLASS_NAME).getAsString());

                return gson.<Action>fromJson(obj.get(INSTANCE), cls);
            } catch (ClassNotFoundException e) {
                return context.deserialize(jsonElement, type);
            }
        }
    }
}
