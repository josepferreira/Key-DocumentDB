package Operations;

import Operations.Scan;
import org.json.JSONObject;

import java.util.Iterator;
import java.util.Map;

public class ScanIterator implements Iterator{
    public Scan scan;

    private Iterator<Map.Entry<Object, JSONObject>> docs = null;

    public ScanIterator(Scan scan) {
        this.scan = scan;
    }


    @Override
    public boolean hasNext() {
        if(docs == null || !docs.hasNext()){
            try{
                scan.getMore().get();
                docs = scan.docs.entrySet().iterator();
                return docs.hasNext();
            }
            catch(Exception e){
                System.out.println(e);
                return false;
            }

        }
        return true;
    }

    @Override
    public Map.Entry<Object,JSONObject> next() {
        return docs.next();
    }
}
