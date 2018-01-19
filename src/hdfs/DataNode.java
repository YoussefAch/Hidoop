package hdfs;

import java.io.File;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import formats.FileHelper;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;

/* Gestion de la connexion avec NameNode et Client - gestion de la création du fichier et des blocs*/
public class DataNode {

	public int port;
	public Map<String, Bloc> listebloc;
	public String NomRep;

	public DataNode(int port) {
		listebloc = new HashMap<String, Bloc>();
		this.port = port;
		this.NomRep = "DataNode" + this.port;
	}

	public static void main(String[] args) {
		try {

			/* Récupération de l'adresse de la machine. */
			InetAddress adresse = InetAddress.getLocalHost();

			/* Connexion au NameNode */
			InetAddress adrnamenome = InetAddress.getByName(NameNode.NameNodeadresse);
			Socket s = new Socket(adrnamenome, NameNode.portNameNodeData);

			/*
			 * Récupération des objets de lecture et d'écriture sur le socket.
			 */
			ObjectOutputStream bw = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream br = new ObjectInputStream(s.getInputStream());

			/* Envoi de l'adresse au NameNode */
			bw.writeObject(adresse.getHostAddress());

			/* Récéption du numero de port à utiliser */
			String portstr = (String) br.readObject();
			int port = Integer.parseInt(portstr);

			/*
			 * ---- PROTOCOLE SYSTOLIQUE Lancement du Thread emetteur de
			 * messages toutes les 15 secondes
			 */
			EmetteurDN emetteur = new EmetteurDN(port + 1000, Integer.toString(15));
			new Thread(emetteur).start();

			/* Fermeture des objets de Lecture et ecriture et du socket */
			br.close();
			bw.close();
			s.close();

			/* Création de l'instance avec le port recu du NameNode */
			DataNode datanode = new DataNode(port);

			try {

				/* Démarrage du serveur de socket */
				ServerSocket ss = new ServerSocket(port);

				/* Récéption et traitement des commandes du client */
				while (true) {

					/* Reception de la connexion du client */
					Socket so = ss.accept();

					/* Récupération des objets de lecture et d'écriture */
					ObjectInputStream ois = new ObjectInputStream(so.getInputStream());
					ObjectOutputStream oos = new ObjectOutputStream(so.getOutputStream());

					/* Lecture de la commande. */
					String cmd = (String) ois.readObject();
					String[] cmdsplit = cmd.split("@");

					/* Filtrage et traitement de la commande. */
					if (cmdsplit[0].equals("CMD_READ")) {

						/* récupération du nom du fichier. */
						String filename = cmdsplit[1];
						/* Récupération du bloc du fichier en question. */
						Bloc blocactu = datanode.listebloc.get("BLOC" + cmdsplit[2] + filename);
						System.out.println("Reception d'une opération read de BLOC" + cmdsplit[2] + filename);
						/* Envoi du format au client */
						Format formatactu=null;
						if (blocactu != null) {

							 formatactu = blocactu.getFile();
							if (blocactu.getType() == Format.Type.KV) {
								oos.writeObject("KV");
							} else {
								oos.writeObject("Line");
							}
						} else {
							 formatactu = new KVFormat(datanode.NomRep+"/BLOC" + cmdsplit[2] + filename);
							oos.writeObject("KV");
						}
						FileHelper.readFile(formatactu, oos, 0);
						formatactu.close();

					} else if (cmdsplit[0].equals("CMD_DELETE")) {
						/* récupération du nom du fichier. */
						String filename = cmdsplit[1];

						System.out.println("Reception d'une opération delete de BLOC" + cmdsplit[2] + filename);

						/* Récupération du bloc du fichier en question. */
						Bloc blocsup = datanode.listebloc.get("BLOC" + cmdsplit[2] + filename);
						/* Suppresion du bloc */
						blocsup.deleteFile();
						datanode.listebloc.remove("BLOC" + cmdsplit[2] + filename);

					} else if (cmdsplit[0].equals("CMD_WRITE")) {

						/* récupération du nom du fichier. */
						String filename = cmdsplit[1];
						/* Récupération du format du fichier */
						String type = (String) ois.readObject();
						/* Création du format du bloc. */
						Format file = null;
						Format.Type t = null;
						new File(datanode.NomRep).mkdir();

						if (type.equals("KV")) {
							file = new KVFormat(datanode.NomRep + "/" + "BLOC" + cmdsplit[2] + filename);
							t = Format.Type.KV;
						} else {
							file = new LineFormat(datanode.NomRep + "/" + "BLOC" + cmdsplit[2] + filename);
							t = Format.Type.LINE;
						}
						/*
						 * Lecture et éprimitives en c fichierscriture des
						 * données.
						 */
						file.open(Format.OpenMode.W);
						FileHelper.writeFile(file, ois);
						file.close();
						/* Création du bloc */
						Bloc newbloc = new Bloc(Integer.parseInt(cmdsplit[2]), file.getFname(), file, t);
						/* Ajout du bloc à la liste */
						System.out.println("Reception d'une opération write de BLOC" + cmdsplit[2] + filename);
						datanode.listebloc.put("BLOC" + cmdsplit[2] + filename, newbloc);

					} else if (cmdsplit[0].equals("CMD_NOTIFICATION")) {
						System.out.println("Reception d'une notification de Remplacement de DataNode en panne ...");

						// Port du nouveau dataNode (dataNode cible)
						int portNewDN = Integer.parseInt(cmdsplit[4]);

						// Adresses du dataNode cible
						String adres = cmdsplit[3];

						// Numéro du bloc à envoyer
						int numBlo = Integer.parseInt(cmdsplit[1]);

						// Nom du fichier associé au bloc a envoyer
						String filena = cmdsplit[2];

						// Récupération du bloc
						Bloc b = datanode.listebloc.get("BLOC" + numBlo + filena);

						// Type du bloc KV/LINE
						String typeBlocA = null;
						if (b.getType() == Format.Type.KV) {
							typeBlocA = "KV";
						} else {
							typeBlocA = "LINE";
						}

						// --------------- Envoi du Bloc au Nouveau DataNode
						// Cible
						System.out.println("Envoi du bloc " + numBlo + " du fichier " + filena
								+ " vers la machine de port " + portNewDN);
						/* Récupération de l'adresse du DataNode */
						InetAddress adrNode = InetAddress.getByName(adres);
						/*
						 * Connexion avec le DataNode et récupération des objets
						 * de lecture et d'écriture.
						 */

						Socket sockRep = new Socket(adrNode, portNewDN);
						ObjectOutputStream oosockRep = new ObjectOutputStream(sockRep.getOutputStream());
						ObjectInputStream oisockRep = new ObjectInputStream(sockRep.getInputStream());
						/* Envoi de la commande */
						oosockRep.writeObject("CMD_WRITE" + "@" + filena + "@" + numBlo);
						/* Envoi du type de Format */

						oosockRep.writeObject(typeBlocA);
						/* Envoi des données du bloc */
						long taille = 100;
						if (b.getType() == Format.Type.LINE) {
							taille = ((LineFormat) b.getFile()).getLength();
						} else {
							taille = ((KVFormat) b.getFile()).getLength();
						}
						FileHelper.readFile(b.getFile(), oosockRep, taille);
						/* Fermeture du socket */
						oosockRep.close();
						oisockRep.close();
						sockRep.close();
						System.out.println("Bloc envoyé avec succès");

					}
					ois.close();
					oos.close();

				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
