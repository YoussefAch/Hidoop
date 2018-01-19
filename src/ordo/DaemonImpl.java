package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;

import formats.Format;
import map.Mapper;
import map.Reducer;

public class DaemonImpl extends UnicastRemoteObject implements Daemon {

	public DaemonImpl() throws RemoteException {
	}

	@Override
	public void runMap(Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException {
		/* Execution du map */
		m.map(reader, writer);
		System.out.println("Execution de map locale terminee avec succes ....");

		/* Fermeture du reader */
		reader.close();

		/* Fermeture du writer */
		writer.close();

		/* envoi du CallBack */
		cb.onFinished();
		System.out.println("CallBack envoyé avec succes");
	}

	@Override
	public void runReduce(Reducer m, List<String> shufflers,Format shuffled, Format writer, Map<String,Serveur> servers, CallBack cb) throws RemoteException {
		/*Réception des shuffles*/
		System.out.println("Récéption du shuffle ...");
		HidoopHelper.createReduceFile(shufflers,shuffled,servers);
		System.out.println("Lancement du reduce ...");
		m.reduce(shuffled, writer);
		/* Fermeture du reader */
		shuffled.close();
		/* Fermeture du writer */
		writer.close();

		/* envoi du CallBack */
		cb.onFinished();
		System.out.println("CallBack envoyé avec succes");

	}

	public void runShuffle(int port, List<Format> readers, int nbReduce, SortComparator comp) throws RemoteException {
		System.out.println("Lancement shuffle ...");
		HidoopHelper.shuffle(port, readers, nbReduce, comp);
	}

	public static void main(String args[]) {

		try {

			
			
			/* Connexion au registre de serveur */
			InetAddress adrRegistre = InetAddress.getByName(RegistreServeur.Registreadresse);

			Socket socket = new Socket(adrRegistre, RegistreServeur.portEcoute);
			/*
			 * Récupération des objets de lecture et d'écriture sur le socket.
			 */
			ObjectOutputStream bw = new ObjectOutputStream(socket.getOutputStream());
			ObjectInputStream br = new ObjectInputStream(socket.getInputStream());
			
			/*Récupération des infos du serveur*/
			String serveurinfo = (String) br.readObject();
			String[] servinf = serveurinfo.split("@");
			int port = Integer.parseInt(servinf[1]);
			String nomServeur = servinf[0];
			
			/* Creation de l'annuaire */
			try {
				Registry registry = LocateRegistry.createRegistry(port);
				System.out.println("Registry created....");
			} catch (RemoteException e) {
				e.printStackTrace();
			}
			
			// Creation d'une instance du serveur
			Daemon serveur = new DaemonImpl();

			// Calcul de l'URL du serveur
			String URL = "//" + InetAddress.getLocalHost().getHostName() + ":" + port + "/" + nomServeur;
			System.out.println(URL);
			/* Enregistrement du serveur aupres du registry */
			Naming.rebind(URL, serveur);

			/* Creer l'instance du serveur pour le registre */
			Serveur s = new Serveur(nomServeur, port, port - 1000, URL);

			/* Envoi du serveur */
			bw.writeObject(s);
			
			/*Confirmation de réception du serveur */
			br.readObject();
			
			/* Lancement du Thread d'émission HeartBeat */
			Thread emetteur = new EmetteurDaemon(port+1000);
			emetteur.start();
			
			br.close();
			bw.close();
			socket.close();

		} catch (Exception exc) {
			exc.printStackTrace();
		}

	}

}
