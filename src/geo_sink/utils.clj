(ns geo-sink.utils
  (:require [clojure.java.io :as io]
            [geo-sink.constants :refer [cache-cursor-file]])
  (:import java.util.Base64))

(defn slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (with-open [in (io/input-stream x)
              out (java.io.ByteArrayOutputStream.)]
    (io/copy in out)
    (.toByteArray out)))

(defn write-file [path input]
  (with-open [o (io/output-stream path)]
    (.write o input)))

(defn write-cursor-cache-file [block-number cursor]
  (write-file cache-cursor-file (str "{\"block_number\": \"" block-number "\", \"cursor\": \"" cursor "\"}")))


(defn ipfs-fetch
  ([cid]
   (when (not (nil? cid))
     (slurp (str "https://ipfs.network.thegraph.com/api/v0/cat?arg=" cid))))
  ([cid max-failures]
   (ipfs-fetch cid 0 max-failures))
  ([cid retry-count max-failures]
   (when (= retry-count max-failures)
     (throw (Exception. "Failed to fetch the cid from ipfs too many times")))
   (try
     (ipfs-fetch cid)
     (catch java.io.IOException e
       (println (str "Failed to fetch data! \n Sleeping for " (* 10 retry-count) "seconds"))
       (Thread/sleep (* 10000 retry-count))
       (ipfs-fetch cid (inc retry-count) max-failures)))))

(defn decode-base64 [to-decode]
  (String. (.decode (Base64/getDecoder) to-decode)))
