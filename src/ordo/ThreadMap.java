package ordo;

import formats.Format;
import map.MapReduce;

public class ThreadMap extends Thread {

	Daemon serveur;
	Format reader;
	Format writer;
	CallBack cb;
	MapReduce mapred;

	public ThreadMap(Daemon serv, MapReduce mr, Format reader, Format writer, CallBack cb) {
		this.serveur = serv;
		this.reader = reader;
		this.writer = writer;
		this.cb = cb;
		this.mapred = mr;
	}

	@Override
	public void run() {

		/* lancement du runMap sur le i eme Map */
		try {
			this.serveur.runMap(this.mapred, this.reader, this.writer, cb);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
