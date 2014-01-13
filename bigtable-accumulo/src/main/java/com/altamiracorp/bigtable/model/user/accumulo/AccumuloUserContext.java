package com.altamiracorp.bigtable.model.user.accumulo;

import com.altamiracorp.bigtable.model.user.ModelUserContext;

import org.apache.accumulo.core.security.Authorizations;

public class AccumuloUserContext implements ModelUserContext {

    @Override
	public String toString() {
		return "AccumuloUserContext [accumuloAuthorizations="
				+ accumuloAuthorizations + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((accumuloAuthorizations == null) ? 0
						: accumuloAuthorizations.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AccumuloUserContext other = (AccumuloUserContext) obj;
		if (accumuloAuthorizations == null) {
			if (other.accumuloAuthorizations != null)
				return false;
		} else if (!accumuloAuthorizations.equals(other.accumuloAuthorizations))
			return false;
		return true;
	}

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
