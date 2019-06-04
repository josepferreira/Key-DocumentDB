package nodes;

import io.atomix.utils.serializer.Serializer;
import messages.*;
import org.json.*;

public class SerializerProtocol {
    public static Serializer newSerializer(){
        return Serializer.builder().withTypes(
                java.util.TreeMap.class,
                java.util.HashMap.class,
                java.util.ArrayList.class,
                java.util.LinkedHashMap.class,
                JSONObject.class,
                KeysUniverse.class,
                GetRequest.class,
                GetReply.class,
                PutRequest.class,
                PutReply.class,
                ReplyMaster.class,
                SlaveIdentifier.class,
                ScanRequest.class,
                ScanReply.class,
                SlaveScanReply.class,
                RemoveRequest.class,
                RemoveReply.class,
                StartRequest.class
        ).build();
    }
}
