package nodes;

import io.atomix.utils.serializer.Serializer;
import messages.*;
import org.json.*;

public class SerializerProtocol {
    public static Serializer newSerializer(){
        return Serializer.builder().withTypes(
                JSONObject.class,
                KeysUniverse.class,
                GetRequest.class,
                GetReply.class,
                PutRequest.class,
                PutReply.class,
                ReplyMaster.class
        ).build();
    }
}
