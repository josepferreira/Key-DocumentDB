package nodes;

import org.json.JSONObject;

import java.util.Iterator;
import java.util.Map;

public class ScanIterator implements Iterator{
    public Scan scan;

    private Iterator<Map.Entry<Long, JSONObject>> docs;

    public ScanIterator(Scan scan) {
        this.scan = scan;
    }


    @Override
    public boolean hasNext() {
        if(docs == null || !docs.hasNext()){
            try{
                scan.getMore().get();
                docs = scan.docs.entrySet().iterator();
                return true;
            }
            catch(Exception e){
                System.out.println(e);
                return false;
            }

        }
        return true;
    }

    @Override
    public Map.Entry<Long,JSONObject> next() {
        return docs.next();
    }
}
