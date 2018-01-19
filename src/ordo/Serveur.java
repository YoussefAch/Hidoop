package ordo;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import hdfs.NameNode;

public class Serveur implements Serializable {
	private String nomserveur;
	private int portRmi;
	private int portTcp;
	private String URL;
	private String adresseIp;
	
	public Serveur(String nom, int portR, int portT, String Url) {
		nomserveur = nom;
		portRmi = portR;
		portTcp = portT;
		URL = Url;
		try {
			adresseIp  = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		 
		
		
	}

	public String getNomserveur() {
		return nomserveur;
	}

	public void setNomserveur(String nomserveur) {
		this.nomserveur = nomserveur;
	}

	public int getPortRmi() {
		return portRmi;
	}

	public void setPortRmi(int portRmi) {
		this.portRmi = portRmi;
	}

	public int getPortTcp() {
		return portTcp;
	}

	public void setPortTcp(int portTcp) {
		this.portTcp = portTcp;
	}

	public String getURL() {
		return URL;
	}

	public void setURL(String uRL) {
		URL = uRL;
	}

	public String getAdresseIp() {
		return adresseIp;
	}

	public void setAdresseIp(String adresseIp) {
		this.adresseIp = adresseIp;
	}

	

}
