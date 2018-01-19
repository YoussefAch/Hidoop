package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GestionnairePanne {
	
	
	public static void handler(int portPanne) {
		
		
				
				//Récupérer les blocs du DataNode défaillant et Modifier le catalogue(INodes) 
				Map<String,INode> catalogue = NameNode.getCatalogue();  
				List<String> blocsARemplacer= new ArrayList<String>();
				
				
				// Création du catalogue mis à jour après la panne 
				Map<String,INode> catalogueMAJ = new HashMap<String, INode>(); 
				
				
				for (String filename : catalogue.keySet() ){
					INode InodeCourant = catalogue.get(filename);
					Map<Integer,ArrayList<String>> mapNodeCourantMAJ = new HashMap<Integer,ArrayList<String>>(); 
					INode InodeCourantMAJ = new INode(filename, mapNodeCourantMAJ);
					for (Integer bloc : InodeCourant.getMapNode().keySet()){	
						
						ArrayList<String> adressesBlocCourant = InodeCourant.getMapNode().get(bloc);
						ArrayList<String> adressesBlocCourantMAJ = new ArrayList<String>();
						for (String adresse : adressesBlocCourant){
							String [] adresse_port = adresse.split("@");
							
							if (adresse_port[1].equals(Integer.toString(portPanne))){ 
								String []	Nouveauport_adresse; 
								String nouveau_port;
								int indexCourant = adressesBlocCourant.indexOf(adresse);
								if (indexCourant==adressesBlocCourant.size()-1) {
									Nouveauport_adresse = adressesBlocCourant.get(indexCourant-1).split("@");
									nouveau_port = Nouveauport_adresse[1];
								}	
								else {
									Nouveauport_adresse = adressesBlocCourant.get(indexCourant+1).split("@");
									nouveau_port = Nouveauport_adresse[1];
								}	
								String resultat = bloc + "@" + filename + "@" + nouveau_port + "@" + Nouveauport_adresse[0];
								blocsARemplacer.add(resultat);
								int portNewDataNode = 4500 + EnregistrementDataNodes.i;
								String adrNewDataNode = NameNode.listemachines.get(portNewDataNode);
								adressesBlocCourantMAJ.add(adrNewDataNode +"@"+ portNewDataNode);
							} else {
								adressesBlocCourantMAJ.add(adresse);
							}
						}
						mapNodeCourantMAJ.put(bloc, adressesBlocCourantMAJ);
					}
					catalogueMAJ.put(filename, InodeCourantMAJ);
				}
				
				
				// Mise à jour du catalogue après la panne
				NameNode.setCatalogue(catalogueMAJ);
				
				
				// afficher le catalogue courant après modifs
				for (String filename : catalogueMAJ.keySet() ){
					System.out.println(filename);
					INode InodeCourant = catalogueMAJ.get(filename);
					for (Integer bloc : InodeCourant.getMapNode().keySet()){	
						System.out.println("bloc : " + bloc);
						ArrayList<String> adressesBlocCourant = InodeCourant.getMapNode().get(bloc);
						for (String adresse : adressesBlocCourant){
							System.out.println(adresse);
						}
					}
				}
				
				
				System.out.println("-----La liste des blocs à remplacer est de taille : " + blocsARemplacer.size());
				for(String bl : blocsARemplacer) {
					System.out.println(bl);
				}
		
		// notify datanodes
		for (String bl : blocsARemplacer) {
			String[] blocInfo = bl.split("@");
			notifyDataNode(blocInfo[3],Integer.parseInt(blocInfo[2]),Integer.parseInt(blocInfo[0]),blocInfo[1]);
		}		
		
	}
	
	public static void notifyDataNode(String adresseNode, int port, int numBloc, String filename) {
		
		/* Récupération de l'adresse du DataNode */
		InetAddress adrNode;
	
		/*
		 * Connexion avec le DataNode et récupération des objets de lecture
		 * et d'écriture.
		 */
		
		Socket s;
		try {
			adrNode = InetAddress.getByName(adresseNode);

			s = new Socket(adrNode, port);
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			int portNewDataNode = 4500 + EnregistrementDataNodes.i;
			String adrNewDataNode = NameNode.listemachines.get(portNewDataNode);
			/* Envoi de la commande */
			oos.writeObject("CMD_NOTIFICATION" + '@' + numBloc + '@' + filename + '@' + adrNewDataNode + '@' + portNewDataNode);
			oos.close();
			ois.close();
			s.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

}
