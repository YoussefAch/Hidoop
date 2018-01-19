package formats;



public class KVFormat extends AbsFormat {

	private static final long serialVersionUID = 1L;

	public KVFormat(String name) {
		super(name);
	}


	@Override
	public void write(KV record) {
		try {
			if (!this.openW) {
				this.open(Format.OpenMode.W);
			}
			String newline = record.k + KV.SEPARATOR + record.v;
			bw.write(newline + "\n");
			this.index++;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public KV read() {
		KV record = new KV();
		record = null;
		try {
			if (!this.openR) {
				this.open(Format.OpenMode.R);
			}
			String line = this.br.readLine();
			if (line != null) {
				String[] linekv = line.split(KV.SEPARATOR);
				record = new KV(linekv[0], linekv[1]);
				this.index++;
			} else {
				record = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return record;
	}

	@Override
	public String getType() {
		return "KV";
	}
}
