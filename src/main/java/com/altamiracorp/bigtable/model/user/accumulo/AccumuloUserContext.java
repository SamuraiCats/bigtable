package com.altamiracorp.bigtable.model.user.accumulo;

import com.altamiracorp.bigtable.model.user.ModelUserContext;
import org.apache.accumulo.core.security.Authorizations;

public class AccumuloUserContext implements ModelUserContext {

    private Authorizations accumuloAuthorizations;

    public AccumuloUserContext () {
        accumuloAuthorizations = new Authorizations();
    }

    public AccumuloUserContext (Authorizations accumuloAuthorizations) {
        this.accumuloAuthorizations = accumuloAuthorizations;
    }

    public Authorizations getAuthorizations () {
        return accumuloAuthorizations;
    }

}
