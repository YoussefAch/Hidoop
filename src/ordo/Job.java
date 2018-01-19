package ordo;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import formats.Format;
import formats.Format.Type;
import formats.KVFormat;
import formats.LineFormat;
import hdfs.HdfsClient;
import hdfs.PanneDataNodeException;
import map.MapReduce;

public class Job implements JobInterface {

	private List<CallBack> listeCallBacksMap;
	private List<CallBack> listeCallBacksReduce;
	private int numberOfReduces;
	private int numberOfMaps;
	private Type inputFormat;
	private Type outputFormat;
	private String inputFname;
	private String outputFname;
	private SortComparator sortComparator;

	public Job() {
		/* Pour la V0 on a choisi de travailler sur 3 serveurs */
		this.numberOfMaps = 3;
		this.listeCallBacksMap = new ArrayList<CallBack>();
	}

	@Override
	public void setNumberOfReduces(int tasks) {
		this.numberOfReduces = tasks;
	}

	@Override
	public void setNumberOfMaps(int tasks) {
		this.numberOfMaps = tasks;
	}

	@Override
	public void setInputFormat(Type ft) {
		this.inputFormat = ft;
	}

	@Override
	public void setOutputFormat(Type ft) {
		this.outputFormat = ft;
	}

	@Override
	public void setInputFname(String fname) {
		this.inputFname = fname;
	}

	@Override
	public void setOutputFname(String fname) {
		this.outputFname = fname;
	}

	@Override
	public void setSortComparator(SortComparator sc) {
		this.sortComparator = sc;
	}

	@Override
	public int getNumberOfReduces() {
		return this.numberOfReduces;
	}

	@Override
	public int getNumberOfMaps() {
		return this.numberOfMaps;
	}

	@Override
	public Type getInputFormat() {
		return this.inputFormat;
	}

	@Override
	public Type getOutputFormat() {
		return this.outputFormat;
	}

	@Override
	public String getInputFname() {
		return this.inputFname;
	}

	@Override
	public String getOutputFname() {
		return this.outputFname;
	}

	@Override
	public SortComparator getSortComparator() {
		return this.sortComparator;
	}

	/**************************************************************************************************************/
	/**************************************************************************************************************/

	@Override
	public void startJob(MapReduce mr) {

		/* Création de l' Executor service */
		ExecutorService executeur = Executors.newFixedThreadPool(this.numberOfMaps);
		ExecutorService execShuffle = Executors.newFixedThreadPool(this.numberOfMaps);

		/* Création du fichier sur la machine locale */
		File fichierResultat = new File(this.getOutputFname());
		try {
			if (fichierResultat.createNewFile()) {
				System.out.println("Fichier résultat a été crée");
			} else {
				System.out.println("Erreur création fichier");
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		/* Récupérer la liste des serveurs */
		Map<String, Serveur> serveurs = JobHelper.getServeur();
		/*Création d'une liste de serveur*/
		List<String> serveursdispo = new LinkedList<String>();
		serveursdispo.addAll(serveurs.keySet());

		/* appel de hdfsWrite */
		/*Création du callBack du write*/

			HdfsClient.HdfsWrite(Type.LINE, this.getInputFname(), this.getNumberOfMaps(), this.getNumberOfMaps() * 2);
	
		HashMap<String, LinkedList<Integer>> mapnode = JobHelper.recInode(this.getInputFname());
		/* Colocalisation des blocs */
		int nbrBloc = JobHelper.getNbBloc(mapnode);
		HashMap<String, LinkedList<Integer>> colNode = HidoopHelper.locNode(mapnode, nbrBloc);
		/* La liste des serveurs utilisés */
		List<String> servUtil = new LinkedList<String>();
		servUtil.addAll(colNode.keySet());

		/***********************************************************************************************************/

		/* Lancement des maps sur les machines distantes (serveurs) */
		this.listeCallBacksMap = JobHelper.startMaps(nbrBloc, this.inputFname, colNode, mr, executeur, serveurs);

		/* Attendre que tous les callBacks soient reçus */
		JobHelper.recCallBack(this.listeCallBacksMap, nbrBloc);
		/* Tous les map ont terminé */

		/* Vérifier l'état des serveurs */
		 JobHelper.verifierExecution(servUtil);

		/* Création de la liste des reducers */
		HashMap<Integer, String> reducers = HidoopHelper.getReducers(this.numberOfReduces,serveursdispo);

		System.out.println("Lancement de shuffle");
		/* Lancer les shuffles */
		List<String> shufflers = JobHelper.startShuffles(this.inputFname, colNode, execShuffle, this.numberOfReduces,
				reducers, serveurs, this.getSortComparator());
		/* Appliquer le reduce */
		System.out.println("Lancement de reduce");
		List<String> reducersList = new LinkedList<String>();
		reducersList.addAll(reducers.values());
		this.listeCallBacksReduce = JobHelper.startReduces(nbrBloc, this.inputFname, this.outputFname, reducers,
				shufflers, mr, executeur, serveurs);
		
		/*Envoyer la liste des reduce*/
		JobHelper.sendReduceLoc(reducers,this.getOutputFname());

		/* Vérifier l'état des serveurs */
		List<String> servSR = new LinkedList<String>();
		servSR.addAll(reducersList);
		servSR.addAll(shufflers);
		JobHelper.verifierExecution(servSR);

		/* Attendre que tous les callBacks soient reçus */
		JobHelper.recCallBack(this.listeCallBacksReduce, this.numberOfReduces);

		try {
			HdfsClient.HdfsRead(this.getOutputFname(), this.getOutputFname(), this.getNumberOfMaps(),
					this.getNumberOfReduces());
			System.out.println("Fusion des résultats  effectuée avec succès ...");
		} catch (PanneDataNodeException e) {
			System.out.println("Lecture des résultats impossible.");
		}
	}

}
