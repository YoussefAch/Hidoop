package ordo;

import java.util.List;

import application.mymapreduce.Comparator;
import formats.Format;

public class ThreadShuffle extends Thread {

	private int nbReduce;
	private int port;
	Daemon serveur;
	List<Format> readers;
	SortComparator comp;

	public ThreadShuffle(int nbReduce, int port, Daemon serveur, List<Format> readers, SortComparator comp) {
		super();
		this.nbReduce = nbReduce;
		this.port = port;
		this.serveur = serveur;
		this.readers = readers;
		this.comp = comp;
	}



	@Override
	public void run() {
		try {
			this.serveur.runShuffle(this.port, this.readers, this.nbReduce, this.comp);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
