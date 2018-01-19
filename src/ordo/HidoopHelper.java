/**
 * 
 */
package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import formats.Format;
import formats.Format.OpenMode;
import formats.KV;

/**
 * Classe contenant des méthodes statiques utiles
 */
public class HidoopHelper {


	/**
	 * Retourne les Bloc sur lesquels chaque Noeud va appliquer map, de facon
	 * équitable
	 * 
	 * @param mapnode
	 * @return Liste de bloc par Noeud sur lesquels appliquer map
	 */
	public static HashMap<String, LinkedList<Integer>> locNode(HashMap<String, LinkedList<Integer>> fileMappedNode,
			int nbrBloc) {
		HashMap<String, LinkedList<Integer>> res = new HashMap<String, LinkedList<Integer>>();
		int i = 1;
		String serCourant = null;
		/* Initialisation des champs */
		for (String serv : fileMappedNode.keySet()) {
			res.put(serv, new LinkedList<Integer>());
		}

		/* Colocalisation avec équilibrage des taches */
		while (i <= nbrBloc) {
			serCourant = null;
			for (String serv : fileMappedNode.keySet()) {
				LinkedList<Integer> listebloc = fileMappedNode.get(serv);
				/* Recherche des serveur contenant ce bloc */
				if (listebloc.contains(i)) {

					// Mise à jour de l'identité du serveur contenant ce bloc
					// avec le minimum de tache affectées

					if (serCourant == null) {
						serCourant = serv;
					} else {
						if (res.get(serv).size() < res.get(serCourant).size()) {
							serCourant = serv;
						}
					}
				}
			}
			/* Ajout du numéro de bloc au serveur adéquat */;
			res.get(serCourant).add(i);
			i++;
		}
		return res;

	}

	/**********************************************************************************/

	/** Effectuer le shuffle sur tous les fichiers mappés
	 * @param port : le port du serveur qui lance le shuffle
	 * @param readers : la liste des fichiers mappés
	 * @param nbReduce : le nombre de Reduce
	 * @param comp : Le comparateur pour determiner la cible
	 */
	public static void shuffle(int port, List<Format> readers, int nbReduce, SortComparator comp) {
		try {
			/* Creation du kv indiquant la fin du shuffle */
			KV kvfin = new KV("fini", "fini");
			/* Création du serveur socket */
			ServerSocket ss = new ServerSocket(port);

			/* Création de la collection de writer */
			Map<Integer, ObjectOutputStream> writer = new HashMap<Integer, ObjectOutputStream>();
			List<ObjectInputStream> iob = new LinkedList<ObjectInputStream>();
			List<Socket> sockets = new LinkedList<Socket>();
			/* Ouverture des sockets de communications */
			for (int i = 1; i <= nbReduce; i++) {
				System.out.println("Lancement du serveur shuffle");
				Socket s = ss.accept();
				System.out.println("Connexion reducer reçue");
				sockets.add(s);
				ObjectOutputStream ob = new ObjectOutputStream(s.getOutputStream());
				writer.put(i, ob);
				ObjectInputStream ib = new ObjectInputStream(s.getInputStream());
				iob.add(ib);
			}

			/* Appliquer le shuffle à tous les fichiers mappés. */
			for (Format f : readers) {
				shuffleFile(f, writer, nbReduce, comp);
			}
			/* Indiquer au reducers que le shuffle est fini */
			for (ObjectOutputStream ob : writer.values()) {
				ob.writeObject(kvfin);
			}
			/* Fermeture des sockets */
			for (ObjectInputStream ib : iob) {
				ib.close();
			}
			for (ObjectOutputStream ob : writer.values()) {
				ob.close();
			}
			for (Socket s : sockets) {
				s.close();
			}
			ss.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/** Effectuer le shuffle à partir d'un seul fichier source
	 * @param f : le fichier source
	 * @param writer : la liste des canaux où les données sont envoyés
	 * @param nbReduce : le nombre de reduce
	 * @param comp : Le comparateur pour determiner la cible
	 */
	public static void shuffleFile(Format f, Map<Integer, ObjectOutputStream> writer, int nbReduce,
			SortComparator comp) {
		/* KV courant dans le fichier des résultats du Map */
		KV kv;

		kv = f.read();
		KV kvnull = new KV("null", "null");
		/* tant qu'il existe encore des Key-Value */
		while (kv != null) {

			/* Choix du writer */
			int writeto = comp.compare(kv.k, null);

			/* Ecrire le resultat dans le bon writer */
			for (int i = 1; i <= nbReduce; i++) {
				ObjectOutputStream ob = writer.get(i);
				try {
					if (i == writeto) {
						ob.writeObject(kv);
					} else {
						ob.writeObject(kvnull);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			kv = f.read();
			/*
			 * Ecriture une derniere fois de null pour indiquer la fin du
			 * shuffle
			 */
		}
		System.out.println("Ecriture des kv du fichier finie");

		/* Fermeture du fichier */
		f.close();
	}

	/**********************************************************************************/
	/** Methode qui reçoit les données envoyés par les shuffles et les enregistre pour préparer le reduce
	 * @param shufflers : la liste des serveurs shuffle
	 * @param cible : le fichier où les données seront écrites
	 * @param servers : la liste des serveurs disponibles
	 */
	public static void createReduceFile(List<String> shufflers, Format cible, Map<String, Serveur> servers) {

		cible.open(OpenMode.W);
		/* Création des collections des sockets */
		List<ObjectOutputStream> oob = new LinkedList<ObjectOutputStream>();
		List<ObjectInputStream> iob = new LinkedList<ObjectInputStream>();
		List<Socket> sockets = new LinkedList<Socket>();
		/* Ouverture des connexions avec les shuffle */
		for (String shuffler : shufflers) {
			try {
				Serveur serv = servers.get(shuffler);
				InetAddress adress = InetAddress.getByName(serv.getAdresseIp());
				Socket s = new Socket(adress, serv.getPortTcp());
				sockets.add(s);
				ObjectOutputStream ob = new ObjectOutputStream(s.getOutputStream());
				oob.add(ob);
				ObjectInputStream ib = new ObjectInputStream(s.getInputStream());
				iob.add(ib);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/* Création de la liste d'écoute */
		List<ObjectInputStream> ecoute = new LinkedList<ObjectInputStream>();
		ecoute.addAll(iob);

		try {
			while (ecoute.size() != 0) {
				/* Ecriture du kv dans le fichier */
				for (ObjectInputStream ib : ecoute) {
					KV kvactu = (KV) ib.readObject();
					if (!kvactu.k.equals("null")) {
						if (!kvactu.k.equals("fini")) {
							cible.write(kvactu);
						} else {
							/*
							 * Si un shuffle a fini on le retire de la liste
							 * d'écoute
							 */
							ecoute.remove(ib);
						}
					}

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		/* Fermeture du fichier */
		cible.close();

		/* Fermeture des sockets */
		try {
			for (ObjectInputStream ib : iob) {
				ib.close();
			}
			for (ObjectOutputStream ob : oob) {
				ob.close();
			}
			for (Socket s : sockets) {
				s.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		cible.close();
	}

	/**********************************************************************************/
	/** Renvoie la liste des serveurs qui s'occupera de lancer les serveurs
	 * @param nbReduce : nombre de reduce a lancer
	 * @param serveurdispo
	 * @return liste de serveur Reduce
	 */

	public static HashMap<Integer, String> getReducers(int nbReduce, List<String> serveurdispo) {
		Random rd = new Random();
		/*Methode à compléter*/
		int compteurBloc = 1;
		List<String> serveurtire = new LinkedList<String>();
		HashMap<Integer, String> res = new HashMap<Integer, String>();
		List<String> dejatire = new LinkedList<String>();
		int tire;
		for (int i = 1; i<= nbReduce; i++) {
			do {
			tire = rd.nextInt(serveurdispo.size());
			} while (dejatire.contains(serveurdispo.get(tire)));
		res.put(compteurBloc,serveurdispo.get(tire));
			dejatire.add(serveurdispo.get(tire));
		compteurBloc++;
		}
		return res;
	}

	/**********************************************************************************/
}
