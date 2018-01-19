package hdfs;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;


public class EnregistrementDataNodes implements Runnable {
	
	private ServerSocket server;
	public static  int i;
	
	public EnregistrementDataNodes(ServerSocket ss) {
		this.server = ss;
		this.i = 0;
	}
	
	

	@Override
	public void run() {
		try {
			
			
			while (true) {
				i++;
				Socket s = this.server.accept();
				System.out.println("Une nouvelle machine vient d'être ajoutée avec le port " + (4501 + i));
				// Lancer un Thread pour traiter la requete
				new Thread(new EnregistrementHandler(s, i)).start();
			}			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

}
