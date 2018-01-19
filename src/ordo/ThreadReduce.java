package ordo;

import java.util.List;
import java.util.Map;

import formats.Format;
import map.MapReduce;

public class ThreadReduce extends Thread {

	Daemon serveur;
	List<String> shufflers;
	Format shuffled;
	Format writer;
	CallBack cb;
	MapReduce mapred;
	Map<String, Serveur> servers;

	public ThreadReduce(Daemon serveur, List<String> shufflers, Format shuffled, Format writer, CallBack cb,
			MapReduce mapred, Map<String, Serveur> servers) {
		super();
		this.serveur = serveur;
		this.shufflers = shufflers;
		this.shuffled = shuffled;
		this.writer = writer;
		this.cb = cb;
		this.mapred = mapred;
		this.servers = servers;
	}

	@Override
	public void run() {

		/* lancement du runReduce sur le noeud */
		try {
			this.serveur.runReduce(this.mapred, this.shufflers, this.shuffled, this.writer, this.servers, cb);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
