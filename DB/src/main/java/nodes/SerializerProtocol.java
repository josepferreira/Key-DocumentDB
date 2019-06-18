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
                java.util.TreeSet.class,
                java.util.HashSet.class,
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
                StartRequest.class,
                StartReply.class,
                EstadoMaster.class,
                PedidoEstado.class,
                StartMessage.class,
                RestartReply.class,
                RestartRequest.class,
                UpdateMessage.class,
                ACKMessage.class,
                Secundario.class
        ).build();
    }
}
