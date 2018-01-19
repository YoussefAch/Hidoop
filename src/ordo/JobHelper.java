package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

public class JobHelper {
	
	public static void sendReduceLoc(Map<Integer,String> redLoc, String fname) {

		try {
			Socket s = new Socket(InetAddress.getByName(RegistreServeur.Registreadresse), RegistreServeur.portJob);
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("reduce@"+fname);
			oos.writeObject(redLoc);
			ois.close();
			oos.close();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static int getNbBloc (HashMap<String, LinkedList<Integer>> locBloc) {
		int res = 0;
		for (List<Integer> l : locBloc.values()) {
			int resint = 0;
			for (Integer nbloc : l) {
				resint = Math.max(resint, nbloc);
			}
			res = Math.max(res,resint);
		}
		return res;
	}
	/**
	 * Retourne la localisation des Blocs sur les différents noeuds
	 * 
	 * @param fname
	 *            : le nom du fichier à localiser
	 * @return : les différentes localisation
	 */
	public static HashMap<String, LinkedList<Integer>> recInode(String fname) {
		HashMap<String, LinkedList<Integer>> res = new HashMap<String, LinkedList<Integer>>();

		try {
			Socket s = new Socket(InetAddress.getByName(RegistreServeur.Registreadresse), RegistreServeur.portJob);
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("map@"+fname);
			res = (HashMap<String, LinkedList<Integer>>) ois.readObject();
			ois.close();
			oos.close();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}

	/**
	 * Récupère la liste des serveurs auprès du registre des serveurs
	 * 
	 * @return liste des serveurs
	 */
	public static Map<String, Serveur> getServeur() {
		Map<String, Serveur> res = new HashMap<String, Serveur>();
		try {
			Socket s = new Socket(InetAddress.getByName(RegistreServeur.Registreadresse), RegistreServeur.portJob);
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("0");
			res = (Map<String, Serveur>) ois.readObject();

			ois.close();
			oos.close();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;

	}

	/** Vérifier l'état actuel des serveur pour pouvoir continuer l'éxécution
	 * @param servactu : la liste des noms de serveurs utilisés.
	 */
	public static void verifierExecution(List<String> servactu) {
		Map<String, Serveur> listeajour = JobHelper.getServeur();
		if (!listeajour.keySet().containsAll(servactu)) {
			System.out.println("Execution arrêtée : nombre de serveur insuffisant.");
			System.exit(0);
		}
	}

	/**
	 * Methode qui lance les maps sur les différents serveurs
	 * 
	 * @param nbrBloc
	 *            : le nombre de bloc auquel le fichier est divisé
	 * @param inputFname
	 *            : le nom du fichier d'entrée
	 * @param colNode
	 *            : les serveurs où les blocs ont été colocalisés
	 * @param mr
	 *            : le map reduceur
	 * @param executeur
	 *            : l'executeur de thread
	 * @return la liste des callBack à attendre
	 */
	public static List<CallBack> startMaps(int nbrBloc, String inputFname, HashMap<String, LinkedList<Integer>> colNode,
			MapReduce mr, ExecutorService executeur, Map<String, Serveur> servers) {
		List<CallBack> res = new LinkedList<CallBack>();
		int i = 0;
		try {

			/*
			 * Pour chaque serveur lancer les maps sur les blocs qui ont été
			 * colocalisés, sortir quand tous les maps ont été lancé
			 */
			while (i < nbrBloc) {

				for (String serverName : colNode.keySet()) {
					for (Integer j : colNode.get(serverName)) {

						/*
						 * Génération des nom des nameReaders et nameWriters les
						 */
						int numport = 4502+Integer.parseInt(serverName.split("serveur")[1]);
						String nomdeRep = "DataNode"+numport;
						String nameReaderMapi = nomdeRep + "/BLOC" + j + inputFname;
						String nameWriterMapi = nomdeRep + "/Mapped" + "BLOC" + j + inputFname;

						/* Chercher le serveur distant dans l'annuaire */
						Daemon serveurcourant = (Daemon) Naming.lookup(servers.get(serverName).getURL());

						/*
						 * Création des readerMap et writerMap pour le serveur
						 * courant
						 */
						Format readerMapi = new LineFormat(nameReaderMapi);
						Format writerMapi = new KVFormat(nameWriterMapi);

						/* Gestion du CallBack */
						CallBack cb = new CallBackImpl();
						res.add(cb);

						/* On lance les threads pour les serveurs distants */
						executeur.execute(new ThreadMap(serveurcourant, mr, readerMapi, writerMapi, cb));
						/* Un map a été lancé, on incrémente le compteur */
						i++;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}

	/**
	 * Methodes qui lance les reduces sur les differents serveurs
	 * 
	 * @param nbrBloc
	 *            : le nombre de bloc auquel le fichier est divisé
	 * @param inputFname
	 *            : le nom du fichier d'entrée
	 * @param reducers
	 *            : la liste des serveurs qui seront chargés d'effectuer les
	 *            reduces
	 * @param shufflers
	 *            : la liste des serveurs qui seront chargés d'effectuer les
	 *            shuffles
	 * @param mr
	 *            : le map reduceur
	 * @param executeur
	 *            : l'executeur de thread
	 * @return la liste des callBack à attendre
	 */
	public static List<CallBack> startReduces(int nbrBloc, String inputFname,String outputFname, Map<Integer,String> reducers,
			List<String> shufflers, MapReduce mr, ExecutorService executeur, Map<String, Serveur> servers) {
		List<CallBack> res = new LinkedList<CallBack>();
		try {

			/*
			 * Pour chaque serveur lancer la reception du shuffle et le reduce
			 */

			for (Integer numReduce : reducers.keySet()) {
				String serverName = reducers.get(numReduce);
				// Génération des noms du fichiers du Reduce
				int numport = 4502+Integer.parseInt(serverName.split("serveur")[1]);
				String nomdeRep = "DataNode"+numport;
				String nameWriterShuffle = nomdeRep+ "/Shuffled" + inputFname;
				Format writerShuffle = new KVFormat(nameWriterShuffle);
				String nameWriterReduce = nomdeRep +"/BLOC"+ numReduce + outputFname;
				Format writerReduce = new KVFormat(nameWriterReduce);

				/* Chercher le serveur distant dans l'annuaire */

				Daemon serveurcourant = (Daemon) Naming.lookup(servers.get(serverName).getURL());

				/* Gestion du CallBack */
				CallBack cb = new CallBackImpl();
				res.add(cb);

				/* On lance les threads pour les serveurs distants */
				executeur.execute(
						new ThreadReduce(serveurcourant, shufflers, writerShuffle, writerReduce, cb, mr, servers));
				/* Un map a été lancé, on incrémente le compteur */

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}

	/**
	 * Methode qui lance les shuffles sur les différents serveurs
	 * 
	 * @param inputFname
	 *            : le nom du fichier d'entrée
	 * @param colNode
	 *            : les serveurs où les blocs ont été colocalisés
	 * @param executeur
	 *            : l'executeur de thread
	 * @param nbReduce
	 *            : le nombre de reduce à lancer
	 * @param reducers
	 *            : la liste des serveurs chargés de lancer les reduce
	 * @return
	 */
	public static List<String> startShuffles(String inputFname, HashMap<String, LinkedList<Integer>> colNode,
			ExecutorService executeur, int nbReduce, HashMap<Integer, String> reducers, Map<String, Serveur> servers,
			SortComparator comp) {
		List<String> shufflers = new LinkedList<String>();
		for (String servername : colNode.keySet()) {
			shufflers.add(servername);
			List<Format> readers = new LinkedList<Format>();
			/* Creation des formats */
			for (Integer j : colNode.get(servername)) {
				int numport = 4502+Integer.parseInt(servername.split("serveur")[1]);
				String nomdeRep = "DataNode"+numport;
				String nameReaderShuffle = nomdeRep+"/Mapped" + "BLOC" + j + inputFname;
				Format readerShuffle = new KVFormat(nameReaderShuffle);
				readers.add(readerShuffle);
			}
			Serveur servc = servers.get(servername);
			try {
				Daemon serveurcourant = (Daemon) Naming.lookup(servc.getURL());
				executeur.execute(new ThreadShuffle(nbReduce, servc.getPortTcp(), serveurcourant, readers, comp));
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		return shufflers;
	}

	/**
	 * Méthode pour gerer la barriere d'attente sur les call back
	 * 
	 * @param listeCallBack
	 * @param nbCallBack
	 */
	public static void recCallBack(List<CallBack> listeCallBack, int nbCallBack) {
		for (int k = 0; k < nbCallBack; k++) {
			try {
				listeCallBack.get(k).getCalled().acquire();
				System.out.println("Thread numero " + k + " a fini");
			} catch (InterruptedException | RemoteException e) {
				e.printStackTrace();
			}
		}
	}
}
