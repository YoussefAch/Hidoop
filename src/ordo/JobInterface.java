/* une PROPOSITION, SAUF startJob(), setInputFormat(Format.Type ft) et setInputFname(String fname),  qui sont EXIGÉES.
 * tout le reste peut être complété ou adapté
 */

package ordo;

import map.MapReduce;
import formats.Format;

public interface JobInterface {
    public void setNumberOfReduces(int tasks);
    public void setNumberOfMaps(int tasks);
    public void setInputFormat(Format.Type ft);
    public void setOutputFormat(Format.Type ft);
    public void setInputFname(String fname);
    public void setOutputFname(String fname);
    public void setSortComparator(SortComparator sc);
    
    public int getNumberOfReduces();
    public int getNumberOfMaps();
    public Format.Type getInputFormat();
    public Format.Type getOutputFormat();
    public String getInputFname();
    public String getOutputFname();
    public SortComparator getSortComparator();
    
    public void startJob (MapReduce mr);
}