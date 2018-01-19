package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class ThreadRegistre extends Thread {

	@Override
	public void run() {
		try {
			ServerSocket ss = new ServerSocket(RegistreServeur.portEcoute);
			while (true) {
				Socket s = ss.accept();
				ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				int port = 3015+RegistreServeur.nextServeur;
				String servinfo = "serveur"+RegistreServeur.nextServeur+"@"+port;
				RegistreServeur.nextServeur++;
				oos.writeObject(servinfo);
				Serveur serv = (Serveur) ois.readObject();
				RegistreServeur.ajouterServeur(serv);
				// Thread d'écoute heartbeat
				Thread listener = new ListenerRegistre(serv.getPortTcp() + 2000);
				listener.start();
				oos.writeObject("Serveur reçu");

				oos.close();
				ois.close();
				s.close();
				System.out.println("La liste des serveurs:");
				for (String serveurname : RegistreServeur.getListeserveurs().keySet()) {
					System.out.println("Le serveur " + serveurname);

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
