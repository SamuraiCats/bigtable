package com.altamiracorp.bigtable.model;

import com.altamiracorp.bigtable.model.user.ModelUserContext;

import java.util.*;

public abstract class Repository<T extends Row> {
    private ModelSession modelSession;

    public Repository(ModelSession modelSession) {
        this.modelSession = modelSession;
    }

    public abstract T fromRow(Row row);

    public abstract Row toRow(T obj);

    public abstract String getTableName();

    public T findByRowKey(String rowKey, ModelUserContext user) {
        Row row = modelSession.findByRowKey(getTableName(), rowKey, user);
        if (row == null) {
            return null;
        }
        T r = fromRow(row);
        r.setDirtyBits(false);
        return r;
    }

    public T findByRowKey(String rowKey, Map<String, String> columnsToReturn, ModelUserContext user) {
        Row row = modelSession.findByRowKey(getTableName(), rowKey, columnsToReturn, user);
        if (row == null) {
            return null;
        }
        T r = fromRow(row);
        r.setDirtyBits(false);
        return r;
    }

    public Iterable<T> findByRowStartsWith(String rowKeyPrefix, ModelUserContext user) {
        return fromRows(modelSession.findByRowStartsWith(getTableName(), rowKeyPrefix, user));
    }

    public Iterable<T> findAll(ModelUserContext user) {
        return fromRows(modelSession.findAll(getTableName(), user));
    }

    public void save(T obj, ModelUserContext user) {
        save(obj, FlushFlag.DEFAULT, user);
    }

    public void save(T obj, FlushFlag flushFlag, ModelUserContext user) {
        Row r = toRow(obj);
        modelSession.save(r, flushFlag, user);
    }

    public void saveMany(Collection<T> objs, ModelUserContext user) {
        List<Row> rows = new ArrayList<Row>();
        String tableName = null;
        for (T obj : objs) {
            Row row = toRow(obj);
            if (tableName == null) {
                tableName = row.getTableName();
            }
            rows.add(row);
        }
        modelSession.saveMany(tableName, rows, user);
    }

    public Iterable<T> fromRows(final Iterable<Row> rows) {
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                final Iterator<Row> it = rows.iterator();
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public T next() {
                        Row row = it.next();
                        T r = fromRow(row);
                        r.setDirtyBits(false);
                        return r;
                    }

                    @Override
                    public void remove() {
                        it.remove();
                    }
                };
            }
        };
    }

    public void delete(RowKey rowKey, ModelUserContext user) {
        modelSession.deleteRow(getTableName(), rowKey, user);
    }

    protected ModelSession getModelSession() {
        return modelSession;
    }

    public void flush() {
        getModelSession().flush();
    }
}
