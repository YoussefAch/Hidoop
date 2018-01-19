package application.montecarlo;

import java.io.Serializable;

import ordo.SortComparator;

public class ComparatorMonteCarlo implements SortComparator, Serializable {

	public ComparatorMonteCarlo() {
		super();
	}

	@Override
	public int compare(String k1, String k2) {
		
		return 1;
		
	}
}
