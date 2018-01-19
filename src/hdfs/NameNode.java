package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;




public class NameNode {

	public static final int portNameNodeClient = 4500;
	public static final int portNameNodeData = 10000;
	public static final int portNameNodeReg= 10001;
	public static  String NameNodeadresse;
	
	/* nomFichier => INode associé */
	public static Map<String, INode> catalogue;
	/* port -> IP adress */
	public static Map<Integer, String> listemachines;

	
	

	public synchronized static Map<String, INode> getCatalogue() {
		return catalogue;
	}

	public synchronized static void setCatalogue(Map<String, INode> catalogue) {
		NameNode.catalogue = catalogue;
	}

	

	public static void main(String[] args) {
		
		/*Configurer le NameNode*/
		NameNode.catalogue = new HashMap<String, INode>();
		NameNode.listemachines  = new HashMap<Integer, String>();
		
		
		
		
		
		try {
		/* récupérer l'adresse de la machine où le NameNode est lancé*/
		InetAddress adresse = InetAddress.getLocalHost();
		NameNode.NameNodeadresse = adresse.getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		/*Lancer le thread pour communiquer avec le registre de serveur*/
		ThreadNameNodeReg tNNr = new ThreadNameNodeReg();
		tNNr.start();
		
		try {
			ServerSocket serverEnregis = new ServerSocket(NameNode.portNameNodeData);
			/*1ere etape : Enregistrer les dataNodes connectes*/
			
			/*
			 * LANCEMENT d'UN THREAD QUI ENREGISTRE en permanence les Datanodes qui se connectent
			 */
			new Thread(new EnregistrementDataNodes(serverEnregis)).start();
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		/*2ieme étape : renvoyer les INode correspondant au nom du fichier reçu*/
		try {
			ServerSocket ss = new ServerSocket(NameNode.portNameNodeClient);
			
			
			int k = 0;
			int count = 0;
			while (true) {
				Socket s = ss.accept();
				
				System.out.println("Nombre de machines fonctionnelles : " + NameNode.listemachines.size());
				ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
				ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
				
				
				String cmd = (String) ois.readObject();
				String [] CmdLine = cmd.split("@");
				String filename = CmdLine[1];
				
				int repFactor =  Integer.parseInt(CmdLine[2]);
				int nbBlocs = Integer.parseInt(CmdLine[3]);
				
				if (CmdLine[0].equals("CMD_WRITE")) {
					System.out.println("Reception d'une requete Write");
					/* pour chaque bloc est associ� une liste de machines qui contient ses dups */
					
					HashMap<Integer, ArrayList<String>> mapNode = new HashMap<Integer,ArrayList<String>>();
					
					
					
					if (repFactor > NameNode.listemachines.size()) {
						System.out.println("Desole Votre Facteur de réplication est plus grand que le nombre de machine, veuillez le diminuer pour pouvoir écrire :) !!");
						// le break du NameNode à discuter
					
					} else {
					
						/* choix de repFactor dataNodes pour chaque bloc */
						for (int i = 1; i<= nbBlocs; i++) {

							
							//   "IP@port" des machines associées au Bloc courant
							ArrayList<String> listeMachinesBlocCourant = new ArrayList<String>();
							/* envoi au client les adresses et ports des datanodes */
							
							
							/* On attend d'avoir la liste des machines en exclusion mutuelle */
							synchronized(NameNode.listemachines){
								
								ArrayList<Integer> portsFonctionnels= new ArrayList<Integer>();
								for (Integer portFonctionnel :  NameNode.listemachines.keySet()){
									portsFonctionnels.add(portFonctionnel);
								}
								
								/* liste des ports choisis pour le bloc courant */
								ArrayList<Integer> portsChoisis = new ArrayList<>();
	
								
								/* On choisit les ports parmi ceux disponibles pour un bloc*/
							    while (portsChoisis.size() < repFactor) {
							    	int port = portsFonctionnels.get(k);
							        portsChoisis.add(port);
							        if (k == portsFonctionnels.size() - 1) {
							        	k = 0;
							        } else {
							        	k++;
							        }
							    }
							    
								
							    
								for(int port : portsChoisis) {
									String machine = NameNode.listemachines.get(port) + "@" + Integer.toString(port);
									listeMachinesBlocCourant.add(machine);
								}
							
							}
							
							oos.writeObject(listeMachinesBlocCourant);
							mapNode.put(i,listeMachinesBlocCourant);
							
						}
						/*Ajout du nouveau noeud au catalogue*/
						INode newNode = new INode(filename,mapNode);
						NameNode.catalogue.put(filename, newNode);	
						System.out.println(filename);
						System.out.println("Choix des machines effectué avec equilibre de charge ...");
						System.out.println("Envoi des machines au client ...");
					}
				}
				if (CmdLine[0].equals("CMD_READ")) {
					System.out.println("Reception d'une requete Read ..");
					INode node = NameNode.getCatalogue().get(filename);
					oos.writeObject(node);
					oos.writeObject(NameNode.listemachines);
				}
				if (CmdLine[0].equals("CMD_DELETE")) {
					System.out.println("Reception d'une requete Delete ...");
					/*Récupération du noeud qui décrit le fichier.*/
					INode node = NameNode.catalogue.get(filename);
					oos.writeObject(node);
					oos.writeObject(NameNode.listemachines);
					if (node != null) {
					NameNode.catalogue.remove(filename);
					}
				}
				ois.close();
				oos.close();
				s.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}

