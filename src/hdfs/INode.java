package hdfs;
import java.io.Serializable;
import java.util.ArrayList;

import java.util.Map;


public class INode implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String filename;
	private int version;

	
	
	/* bloc/listes machines */
	private Map<Integer,ArrayList<String>> mapNode;
	
	public INode(String filename, Map<Integer,ArrayList<String>> mapNode ) {
		this.filename = filename;
		this.version = 1;
		this.mapNode = mapNode;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public int getVersion() {
		return version;
	}

	public Map<Integer, ArrayList<String>> getMapNode() {
		return mapNode;
	}
	
}
