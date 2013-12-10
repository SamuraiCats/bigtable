package com.altamiracorp.bigtable.model;

/**
 * Base class to support building Java model objects from a BigTable row
 *
 * @param <TModel> model class that this builder will create from a Row
 */
public abstract class BaseBuilder<TModel> {
    public abstract TModel fromRow(Row<? extends RowKey> row);

    public abstract String getTableName();

}
