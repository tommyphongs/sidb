SimpleDB
======

A simple and powerful DB that uses SSDB as storage engine, and provides Java API for manipulating large ammounts of data in the database 



SSDB Github address
-----------------

https://github.com/ideawu/ssdbi

The usage
----------------

```
import org.ptm.SiDB.utils.SSDB;
import org.ptm.SiDB.Utils.SiDBResponse;
import org.pmt.SiDB.SiDBUtils;


SiDB db = SiDBUtilss.create(String host, int port, int timeout)
db.set("name", "tommyphong").check();  

SiDBResponse resp = db.get("name");
if (!resp.ok()) {
    // ...
} else {
    log.error("Error");
}
```
