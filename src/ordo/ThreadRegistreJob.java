package ordo;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import hdfs.NameNode;

public class ThreadRegistreJob extends Thread {

	@Override
	public void run() {
		try {
		ServerSocket ss = new ServerSocket(RegistreServeur.portJob);
		while(true) {
			Socket s = ss.accept();
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			String cmd =(String) ois.readObject();
			if (cmd.equals("0")) {
			oos.writeObject(RegistreServeur.getListeserveurs());
			} else {
				String[] cmdtab = cmd.split("@");
				if (cmdtab[0].equals("map")) {
				 Map<String, List<Integer>> tosend = getZoneMap(cmdtab[1]);
				 oos.writeObject(tosend);
				} else {
				Map<Integer,String> tosend = (Map<Integer,String>) ois.readObject();	
				sendReduceLocToNM(tosend,cmdtab[1]);
				}
			}
			oos.close();
			ois.close();
			s.close();
		}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static  Map<String, List<Integer>> getZoneMap(String filename) {
		Map<String, List<Integer>> res = new HashMap<>();
		try {
		Socket s = new Socket(InetAddress.getByName(NameNode.NameNodeadresse),NameNode.portNameNodeReg);
		ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
		ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
		String cmd = "map"+"@"+filename;
		oos.writeObject(cmd);
		res = (Map<String, List<Integer>>)ois.readObject();
		oos.close();
		ois.close();
		s.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return res;
	}
	
	public static void sendReduceLocToNM(Map<Integer,String> redLoc, String filename) {
		try {
			Socket s = new Socket(InetAddress.getByName(NameNode.NameNodeadresse),NameNode.portNameNodeReg);
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			String cmd = "reduce"+"@"+filename;
			oos.writeObject(cmd);
			oos.writeObject(redLoc);
			oos.close();
			ois.close();
			s.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
	}

}
