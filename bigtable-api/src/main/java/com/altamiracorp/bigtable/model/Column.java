package com.altamiracorp.bigtable.model;

public class Column {
    private final String name;
    private final Value value;
    private String visibility;
    private boolean dirty;
    private boolean delete;

    public Column(String name, Object value) {
        this.name = name;
        this.value = new Value(value);
    }

    public Column(String name, Object value, String visibility) {
        this.name = name;
        this.value = new Value(value);
        this.visibility = visibility;
    }

    public String getName() {
        return name;
    }

    public Value getValue() {
        return this.value;
    }

    public String getVisibility () { return this.visibility; }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        toString(result, "");
        return result.toString();
    }

    public void toString(StringBuilder out, String indent) {
        out.append(indent + getName() + ": " + getValue() + "\n");
    }

    public boolean isDelete() {
        return delete;
    }

    public void setDelete(boolean delete) {
        this.delete = delete;
    }
}
