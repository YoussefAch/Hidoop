package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import config.Project;
import formats.AbsFormat;
import formats.FileHelper;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import javafx.util.Pair;

public class HdfsHelper {
	/**
	 * Classe contenant les méthodes qui gère les communications en socket entre
	 * Le Client et DataNode ou le Client et NameNode.
	 */

	/**
	 * Methode qui contacte le NameNode et renvoie le INode correspondant au
	 * fichier (en cas de read/delete.
	 * 
	 * @param cmd
	 *            : la commande ( CMD_READ ou CMD_DELETE)
	 * @param filename
	 *            : nom du fichier à lire/supprimer
	 * @return : Inode : noeud du fichier
	 */

	public static Pair<INode,Map<Integer, String>> getInode(String cmd, String filename, int repFactor, int nbBlocs) {
		INode node = null;
		Map<Integer, String> listemachineFonctionnelles = null;
		Pair<INode, Map<Integer, String>>  p = null;
		try {
			/* Récupération de l'adresse du NameNode */
			InetAddress adrNameNode = InetAddress.getByName(NameNode.NameNodeadresse);
			/*
			 * Connexion avec le NameNode et récupération des objets de lecture
			 * et d'écriture.
			 */
			Socket s = new Socket(adrNameNode, NameNode.portNameNodeClient);
			ObjectOutputStream bw = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			/* Envoi de la commande */
			bw.writeObject(cmd + '@' + filename + '@' + repFactor + '@' + nbBlocs);
			/* Récupération du noeud envoyé */
			node = (INode) ois.readObject();
			listemachineFonctionnelles = (Map<Integer, String>) ois.readObject();
			p = new Pair(node, listemachineFonctionnelles);
			bw.close();
			ois.close();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return p;
	}

	/**
	 * Methode qui contacte le NameNode et renvoie la liste des DataNode dispo
	 * * @param cmd : la commande ( CMD_WRITE)
	 * 
	 * @param filename
	 *            : nom du fichier à écrie
	 * @return : String[] ( tableau de adresseDataNode +'@' + portDataNode
	 */

	public static List<ArrayList<String>> getDataNode(String cmd, String filename, int repFactor, int nbBlocs) {

		
		List<ArrayList<String>> listeBlocsMachines = new ArrayList<>();
		ArrayList<String> listeMachines = new ArrayList<String>();
		try {
			/* Récupération de l'adresse du NameNode */
			InetAddress adrNameNode = InetAddress.getByName(NameNode.NameNodeadresse);
			/*
			 * Connexion avec le NameNode et récupération des objets de lecture
			 * et d'écriture.
			 */
			Socket s = new Socket(adrNameNode, NameNode.portNameNodeClient);
			ObjectOutputStream bw = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream br = new ObjectInputStream(s.getInputStream());
			/* Envoi de la commande */
			bw.writeObject(cmd + "@" + filename + "@" + repFactor + "@" + nbBlocs);
			
			
			/* Lecture des données et créations du tableau de DataNode */
			for (int i = 1; i <= nbBlocs; i++) {
				listeMachines = (ArrayList<String>) br.readObject();
				listeBlocsMachines.add(listeMachines);
			}
			bw.close();
			br.close();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return listeBlocsMachines;
	}

	/**
	 * Methode qui contacte les DataNode et récupére le fichier lu
	 * 
	 * @param adresseNode
	 *            : adresses des dataNode à contacter
	 * @param port
	 *            : numéro de port des datanode
	 * @param filename
	 *            : nom du fichier
	 * @return bw : l'ObjectInputStream correspondant
	 */
	public static Format readFileFromDN(int numBloc, String adresseNode, int port, String filename, Format f,
			String systemfilename) {
		try {
			/* Récupération de l'adresse du DataNode */
			InetAddress adrNode = InetAddress.getByName(adresseNode);
			/*
			 * Connexion avec le DataNode et récupération des objets de lecture
			 * et d'écriture.
			 */
			Socket s = new Socket(adrNode, port);
			
			
			ObjectOutputStream bw = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			/* Envoi de la commande */
			bw.writeObject("CMD_READ" + '@' + filename + '@' + numBloc);
			/* Récéption du type de fichier */
			String type = (String) ois.readObject();

			/* Création du fichier si numbloc = 1 */
			if (numBloc == 1) {
				if (type.equals("KV")) {
					f = new KVFormat(systemfilename);
				} else {
					f = new LineFormat(systemfilename);
				}
			}

			/* Ecriture des données dans le fichier. */
			FileHelper.writeFile(f, ois);

			/* Fermeture des sockets */
			ois.close();
			bw.close();
			s.close();
			
		} 
		catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		return f;
	}

	/**
	 * Methode qui contacte le DataNode afin de supprimer un bloc. /**
	 * 
	 * @param numBloc
	 *            : numéro du bloc
	 * @param adresseNode
	 *            : adresse du dataNode à contacter
	 * @param port
	 *            : numéro de port du datanode
	 * @param filename
	 *            : nom du fichier
	 */
	public static void deleteBloc(int numBloc, String adresseNode, int port, String filename) {
		try {
			/* Récupération de l'adresse du DataNode */
			InetAddress adrNode = InetAddress.getByName(adresseNode);
			/*
			 * Connexion avec le DataNode et récupération des objets de lecture
			 * et d'écriture.
			 */
			Socket s = new Socket(adrNode, port);
			ObjectOutputStream bw = new ObjectOutputStream(s.getOutputStream());
			/* Envoi de la commande */
			bw.writeObject("CMD_DELETE" + '@' + filename + '@' + numBloc);
			bw.close();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Methode qui contacte le DataNode et lui envoie un fichier à stocker.
	 * 
	 * @param numBloc
	 *            : numéro du bloc
	 * @param adresseNode
	 *            : adresse du dataNode à contacter
	 * @param port
	 *            : numéro de port du datanode
	 * @param filename
	 *            : nom du fichier
	 * @param blocfile
	 *            : le fichier bloc à stocker
	 */
	public static long writeFileInDN(int numBloc, String adresseNode, int port, String filename, Format blocfile, long nbbyte) {
		try {
			/* Récupération de l'adresse du DataNode */
			InetAddress adrNode = InetAddress.getByName(adresseNode);
			/*
			 * Connexion avec le DataNode et récupération des objets de lecture
			 * et d'écriture.
			 */
			
			Socket s = new Socket(adrNode, port);
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			/* Envoi de la commande */
			oos.writeObject("CMD_WRITE" + '@' + filename + '@' + numBloc);
			/*Envoi du type de Format*/
			oos.writeObject(((AbsFormat) blocfile).getType());
			/* Envoi des données du bloc*/
			nbbyte = FileHelper.readFile(blocfile, oos, nbbyte);
			/*Fermeture du socket*/
			oos.close();
			ois.close();
			s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return nbbyte;
	}
}
