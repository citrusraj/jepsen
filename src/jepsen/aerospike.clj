(ns jepsen.aerospike
  (:use [clojure.set :only [union difference]]
        jepsen.util
        jepsen.set-app
		jepsen.load)
  (:import (com.aerospike.client AerospikeClient))
  (:import (com.aerospike.client Bin))
  (:import (com.aerospike.client Key))
  (:import (com.aerospike.client Value))
  (:import (com.aerospike.client Record)))

(defn aerospike-app
  "Creates a new key/value pair for each element, and uses the automatic transaction retry loop."
  [opts]
    (let 
       [ns     (get opts :ns "test")
        set    (get opts :set "demo")
        bin    (get opts :bin "bin")
        key    (get opts :key "key1")
        host   (get opts :host "127.0.0.1")]
    
    (reify SetApp
      (setup [app]
		; add UDF register code 
		(def client (new AerospikeClient host 3000)))
        (.put client nil (new Key ns set key) (into-array Bin [(Bin/asNull bin)])))

      (add [app element]
		(if (nil? (.execute client 
				nil 
				(new Key ns set key) 
				"list"
				"append"
				(into-array Value [(Value/get bin) (Value/get  element)])))
			error
			ok)

      (results [app]
        (map #(Long. %) (into [] (.getValue (.get client nil (new Key ns set key)) bin))))
 
      (teardown [app]
        (.close client)))))

(defn aerospike-reconnect-app
  "Creates a new key/value pair for each element. Uses fresh connection for every insert"
  [opts]
    (let 
       [ns     (get opts :ns "test")
        set    (get opts :set "demo")
        bin    (get opts :bin "bin")
        key    (get opts :key "key1")
        host   (get opts :host "127.0.0.1")]
    
    (reify SetApp
      (setup [app]
		; add register udf code
		(def client (new AerospikeClient host 3000))
        (.put client nil (new Key ns set key) (into-array Bin [(Bin/asNull bin)])))

      (add [app element]
		(def client (new AerospikeClient host 3000))
		(Thread/sleep 1)
		(if (nil? (.execute client 
				nil 
				(new Key ns set key) 
				"list"
				"append"
				(into-array Value [(Value/get bin) (Value/get  element)])))
			error
			ok)
		(.close client))

      (results [app]
        (map #(Long. %) (into [] (.getValue (.get client nil (new Key ns set key)) bin))))
 
      (teardown [app]
        (.close client)))))
