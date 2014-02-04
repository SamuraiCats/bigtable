package com.altamiracorp.bigtable.jetty;

import org.eclipse.jetty.server.session.AbstractSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is borrowed from org.eclipse.jetty:jetty-nosql. It is included here to reduce the dependencies.
 * and change the logger to an SLF4j logger which doesn't depend on jetty utils jar
 */
public class NoSqlSession extends AbstractSession {
    private static final Logger __log = LoggerFactory.getLogger(NoSqlSession.class);

    private final NoSqlSessionManager _manager;
    private Set<String> _dirty;
    private final AtomicInteger _active = new AtomicInteger();
    private Object _version;
    private long _lastSync;

    /* ------------------------------------------------------------ */
    public NoSqlSession(NoSqlSessionManager manager, HttpServletRequest request) {
        super(manager, request);
        _manager = manager;
        save(true);
        _active.incrementAndGet();
    }

    /* ------------------------------------------------------------ */
    public NoSqlSession(NoSqlSessionManager manager, long created, long accessed, String clusterId, Object version) {
        super(manager, created, accessed, clusterId);
        _manager = manager;
        _version = version;
    }

    /* ------------------------------------------------------------ */
    @Override
    public Object doPutOrRemove(String name, Object value) {
        synchronized (this) {
            Object old = super.doPutOrRemove(name, value);

            if (_manager.getSavePeriod() == -2) {
                save(true);
            }
            return old;
        }
    }


    @Override
    public void setAttribute(String name, Object value) {
        if (updateAttribute(name, value)) {
            if (_dirty == null) {
                _dirty = new HashSet<String>();
            }

            _dirty.add(name);
        }
    }

    /*
     * a boolean version of the setAttribute method that lets us manage the _dirty set
     */
    protected boolean updateAttribute(String name, Object value) {
        Object old = null;
        synchronized (this) {
            checkValid();
            old = doPutOrRemove(name, value);
        }

        if (value == null || !value.equals(old)) {
            if (old != null)
                unbindValue(name, old);
            if (value != null)
                bindValue(name, value);

            _manager.doSessionAttributeListeners(this, name, old, value);
            return true;
        }
        return false;
    }

    /* ------------------------------------------------------------ */
    @Override
    protected void checkValid() throws IllegalStateException {
        super.checkValid();
    }

    /* ------------------------------------------------------------ */
    @Override
    protected boolean access(long time) {
        __log.debug("NoSqlSession:access:active " + _active);
        if (_active.incrementAndGet() == 1) {
            long period = _manager.getStalePeriod() * 1000L;
            if (period == 0)
                refresh();
            else if (period > 0) {
                long stale = time - _lastSync;
                __log.debug("NoSqlSession:access:stale " + stale);
                if (stale > period)
                    refresh();
            }
        }

        return super.access(time);
    }

    /* ------------------------------------------------------------ */
    @Override
    protected void complete() {
        super.complete();
        if (_active.decrementAndGet() == 0) {
            switch (_manager.getSavePeriod()) {
                case 0:
                    save(isValid());
                    break;
                case 1:
                    if (isDirty())
                        save(isValid());
                    break;

            }
        }
    }

    /* ------------------------------------------------------------ */
    @Override
    protected void doInvalidate() throws IllegalStateException {
        super.doInvalidate();
        save(false);
    }

    /* ------------------------------------------------------------ */
    protected void save(boolean activateAfterSave) {
        synchronized (this) {
            _version = _manager.save(this, _version, activateAfterSave);
            _lastSync = getAccessed();
        }
    }


    /* ------------------------------------------------------------ */
    protected void refresh() {
        synchronized (this) {
            _version = _manager.refresh(this, _version);
        }
    }

    /* ------------------------------------------------------------ */
    public boolean isDirty() {
        synchronized (this) {
            return _dirty != null && !_dirty.isEmpty();
        }
    }

    /* ------------------------------------------------------------ */
    public Set<String> takeDirty() {
        synchronized (this) {
            Set<String> dirty = _dirty;
            if (dirty == null)
                dirty = new HashSet<String>();
            else
                _dirty = null;
            return dirty;
        }
    }

    /* ------------------------------------------------------------ */
    public Object getVersion() {
        return _version;
    }

}
