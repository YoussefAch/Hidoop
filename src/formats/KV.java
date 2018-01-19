package formats;

import java.io.Serializable;

public class KV implements Serializable  {


	private static final long serialVersionUID = 1L;

	public static final String SEPARATOR = "<->";
	
	public String k;
	public String v;
	
	public KV() {}
	
	public KV(String k, String v) {
		super();
		this.k = k;
		this.v = v;
	}

	public String toString() {
		return "KV [k=" + k + ", v=" + v + "]";
	}
	
}
