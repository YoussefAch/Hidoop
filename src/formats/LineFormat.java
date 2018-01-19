package formats;


public class LineFormat extends AbsFormat {

	private static final long serialVersionUID = 1L;

	public LineFormat(String name) {
		super(name);
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
				record = new KV(Integer.toString(this.index), line);
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
	public void write(KV record) {
		try {
			if (!this.openW) {
				this.open(Format.OpenMode.W);
			}
			String newline = record.v;
			bw.write(newline + "\n");
			this.index++;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getType() {
		return "Line";
	}

}
