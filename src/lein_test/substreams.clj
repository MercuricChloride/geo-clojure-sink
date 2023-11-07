(ns lein-test.substreams
  (:gen-class)
  (:require [sf.substreams.rpc.v2 :as rpc]
            [sf.substreams.rpc.v2.Stream.client :as stream]
            [protojure.grpc.client.providers.http2 :as grpc.http2]
            [protojure.protobuf :as protojure]
            [geo.clojure.sink :as geo]
            [dotenv :refer [env app-env]]
            [clojure.java.io :as io]
            [clojure.core.async :as async]
            [clojure.string :as string]
            [lein-test.db-helpers :refer [update-cursor get-cursor]]
            [sf.substreams.v1 :as v1])
  (:import java.util.Base64))


(def current-block (atom (:cursors/block_number (get-cursor))))
(def cursor (atom (:cursors/cursor (get-cursor))))
(def sink-mode (atom :populate-cache))

(defn file-exists? [filepath]
  (.exists (java.io.File. filepath)))

(defn cursor-watcher
  "Watches the cursor for changes and updates the database"
 [key ref old-state new-state]
 (update-cursor new-state @current-block))
(add-watch cursor :watcher cursor-watcher)

(defn take-all [ch f]
  (loop []
    (let [val (async/<!! ch)]
        (if val
          (do
            (f val)
            (recur))
          (println "Channel done processing")))))


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

(defn empty-output?
  [input]
  (and (empty? (:entries input))
       (empty? (:roles-granted input))
       (empty? (:roles-revoked input))))

(defn format-entry-filename
 [input]
 (let [block (first (string/split (:id input) #"-"))
       index (:index input)
       space (:space input)
       author (:author input)]
  (str block "_" index "_" space "_" author)))

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


(defn uri-type
 [uri]
 (cond
   (string/starts-with? uri "ipfs://") :ipfs
   (string/starts-with? uri "data:application/json;base64,") :base64
   :else :invalid))

(defmulti uri-data uri-type)

(defmethod uri-data :ipfs
 [uri]
 (let [cid (string/replace-first uri "ipfs://" "")]
   (ipfs-fetch cid 3)))

(defmethod uri-data :base64
 [uri]
 (let [data (string/replace-first uri "data:application/json;base64," "")]
   (decode-base64 data)))

(defmulti process-geo-data (fn [_] @sink-mode))

(defmethod process-geo-data :populate-cache
 [geo-output]
 (let [entries (:entries geo-output)
        roles-granted (:roles-granted geo-output)
        roles-revoked (:roles-revoked geo-output)
        entry-path "new-cache/entries-added/"
        granted-path "new-cache/roles-granted/"
        revoked-path "new-cache/roles-revoked/"
        action-path "new-cache/actions/"]
   (doseq [entry entries]
     (let [entry-filename (format-entry-filename entry)]
       (write-file (str entry-path entry-filename) (protojure/->pb entry))
       (when (not (file-exists? (str action-path entry-filename)))
         (spit (str action-path entry-filename) (uri-data (:uri entry))))))
   (doseq [entry roles-granted]
     (when (not (= :null (:role entry)))
       (write-file (str granted-path (:id entry)) (protojure/->pb entry))))
   (doseq [entry roles-revoked]
     (when (not (= :null (:role entry)))
       (write-file (str revoked-path (:id entry)) (protojure/->pb entry))))))

(defmethod process-geo-data :from-cache
 [geo-output]
 (println "should update the db"))

(defn handle-block-scoped-data
  [data]
  (try
   (let [message (:message data)
         block-data (:block-scoped-data message)
         stream-cursor (:cursor block-data)]
     (when block-data
       (let [
             output (:output block-data)
             map-output (:map-output output)
             map-value (:value map-output)
             geo-output (geo/pb->GeoOutput map-value)
             block-number (:number (:clock block-data))]
            (if (empty-output? geo-output)
             (when (= (mod block-number 100) 0)
              (println "Empty block at: " block-number))
             (do
              (println "Got map output at block:" block-number)
              (process-geo-data geo-output)))
            (swap! current-block (fn [_] (str block-number)))
            (swap! cursor (fn [_] stream-cursor)))))
   (catch Exception e
    (println "GOT ERROR: \n\n\n\n\n" e))))


(def spkg (v1/pb->Package (slurp-bytes "geo-substream-v1.0.2.spkg")))

(defn spawn-client [] @(grpc.http2/connect {:uri "https://polygon.substreams.pinax.network:443"
                                            :ssl true
                                            :idle-timeout 60000
                                            :metadata {"authorization" (env "SUBSTREAMS_API_TOKEN")}}))
(defn start-stream
  ([client]
   (start-stream client 36472424 48000000))
  ([client start-block stop-block]
   (let [channel (async/chan (async/buffer 10))]

     (stream/Blocks client (rpc/new-Request {:start-block-num (Integer/parseInt @current-block)
                                             :stop-block-num stop-block
                                             :start-cursor @cursor
                                             :modules (:modules spkg)
                                             :output-module "geo_out"}) channel)
     (take-all channel handle-block-scoped-data))))
