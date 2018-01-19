package config;

import java.util.HashMap;

public class Project {
	public static String PATH = "/home/ysf/Desktop/hidoop/";
	public static HashMap<String, Integer> listeServeurs = new HashMap<>();
	static {
		listeServeurs.put("serveur0", 3015);
		listeServeurs.put("serveur1", 3016);
		listeServeurs.put("serveur2", 3017);
	}
}
