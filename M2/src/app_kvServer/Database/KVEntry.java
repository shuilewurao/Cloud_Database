package app_kvServer.Database;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class KVEntry implements Serializable {

    public long start_offset;
    public long end_offset;
    public boolean valid;

    public KVEntry(long start, long end) {
        this.start_offset = start;
        this.end_offset = end;
        this.valid = true;
    }

    public void invalidate() {
        this.valid = false;
    }

    public boolean isValid() {
        return valid;
    }
}
