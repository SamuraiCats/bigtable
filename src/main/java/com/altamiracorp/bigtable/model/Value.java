package com.altamiracorp.bigtable.model;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;

import java.nio.ByteBuffer;

public class Value implements JSONString {
    private final byte[] value;

    public Value(Object value) {
        if (value == null) {
            throw new NullPointerException("Value cannot be null");
        }
        this.value = toBytes(value);
    }

    private byte[] toBytes(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return stringToBytes((String) value);
        }

        if (value instanceof Long) {
            return longToBytes((Long) value);
        }

        if (value instanceof Integer) {
            return intToBytes((Integer) value);
        }

        if (value instanceof Double) {
            return doubleToBytes((Double) value);
        }

        if (value instanceof byte[]) {
            return (byte[]) value;
        }

        if (value instanceof Value) {
            return ((Value) value).toBytes();
        }

        if (value instanceof JSONObject) {
            return jsonObjectToBytes((JSONObject) value);
        }

        throw new RuntimeException("Unhandled type to convert: " + value.getClass().getName());
    }

    private byte[] jsonObjectToBytes(JSONObject value) {
        return stringToBytes(value.toString());
    }

    private byte[] doubleToBytes(Double value) {
        return ByteBuffer.allocate(8).putDouble(value).array();
    }

    private byte[] stringToBytes(String value) {
        return value.getBytes();
    }

    private byte[] longToBytes(Long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private byte[] intToBytes(Integer value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public byte[] toBytes() {
        return this.value;
    }

    public Long toLong() {
        if (this.value.length != 8) {
            throw new RuntimeException("toLong failed. Expected 8 bytes found " + this.value.length);
        }
        return ByteBuffer.wrap(this.value).getLong();
    }

    public Double toDouble() {
        if (this.value.length != 8) {
            throw new RuntimeException("toDouble failed. Expected 8 bytes found " + this.value.length);
        }
        return ByteBuffer.wrap(this.value).getDouble();
    }

    public Integer toInteger() {
        if (this.value.length != 4) {
            throw new RuntimeException("toInteger failed. Expected 4 bytes found " + this.value.length);
        }
        return ByteBuffer.wrap(this.value).getInt();
    }

    @Override
    public String toString() {
        return new String(this.value);
    }

    public static byte[] toBytes(Value value) {
        if (value == null) {
            return null;
        }
        return value.toBytes();
    }

    public static String toString(Value value) {
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    public static Long toLong(Value value) {
        if (value == null) {
            return null;
        }
        return value.toLong();
    }

    public static Double toDouble(Value value) {
        if (value == null) {
            return null;
        }
        return value.toDouble();
    }

    public static Integer toInteger(Value value) {
        if (value == null) {
            return null;
        }
        return value.toInteger();
    }

    public static JSONObject toJson(Value value) {
        if (value == null) {
            return null;
        }
        try {
            String str = toString(value);
            if (str.trim().length() == 0) {
                return null;
            }
            return new JSONObject(str);
        } catch (JSONException e) {
            throw new RuntimeException("Could not parse JSON", e);
        }
    }

    @Override
    public String toJSONString() {
        StringBuilder sb = new StringBuilder();
        sb.append('"');
        for (int i = 0; i < this.value.length; i++) {
            byte b = this.value[i];
            if (b == '"') {
                sb.append("\\\"");
            } else if (b == '\\') {
                sb.append("\\\\");
            } else if (b == '/') {
                sb.append("\\/");
            } else if (b == '\b') {
                sb.append("\\b");
            } else if (b == '\f') {
                sb.append("\\f");
            } else if (b == '\n') {
                sb.append("\\n");
            } else if (b == '\r') {
                sb.append("\\r");
            } else if (b == '\t') {
                sb.append("\\t");
            } else if (b >= ' ' && b <= '~') {
                sb.append((char) b);
            } else {
                sb.append("\\u" + String.format("%04X", b));
            }
        }
        sb.append('"');
        return sb.toString();
    }
}
