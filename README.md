Scan a file or disk image byte-by-byte, outputting documents stored by CouchDB:

    ./grep_couch ~/couchdb-1.0.2/var/lib/couchdb/xxx.couch 
    {"find":"Me","this_is_true":true,"this_is_53":53}
    {"_id":"User/jason"}
    {"find":"Me","this_is_true":true}
    {"_id":"User/jason"}
    {"find":"Me","this_is_true":true,"stuff":{"in stuff":12345}}
    {"_id":"User/jason"}
    {"\u0e44\u0e17\u0e22":true,"this is thai":"\u0e44\u0e17\u0e22"}
