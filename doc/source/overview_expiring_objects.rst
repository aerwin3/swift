=======================
Expiring Object Support
=======================
Support for object deletion upon an expiration time is built into swift. The Swift client would use the ``X-Delete-At`` or ``X-Delete-After`` headers during an object ``PUT`` or ``POST`` and the cluster would automatically quit serving that object at the specified time and would shortly thereafter remove the object from the system.

The ``X-Delete-At`` header takes a Unix Epoch timestamp, in integer form; for example: ``1317070737`` represents ``Mon Sep 26 20:58:57 2011 UTC``.

The ``X-Delete-After`` header takes an integer number of seconds. The proxy server that receives the request will convert this header into an ``X-Delete-At`` header using its current time plus the value given.

When an object's ``X-Delete-At`` time has arrived a response of ``404 Not Found`` will be returned. The object will be removed from the container listing as soon as the next replication pass for the container database is complete. As for removing the actual object on disk, the object-auditor will place a tombstone for the object upon its next pass.
