(ns geo-sink.substreams
  (:gen-class)
  (:require [cheshire.core :as ch]
            [clojure.core.async :as async]
            [clojure.string :as string]
            [dotenv :refer [env]]
            [geo-sink.cache :refer [format+filter-actions
                                    write-cursor-cache-file]]
            [geo-sink.constants :refer [cache-action-directory
                                        cache-entry-directory cache-granted-directory
                                        cache-revoked-directory]]
            [geo-sink.db-helpers :refer [get-cursor update-db-cursor]]
            [geo-sink.populate :refer [actions->db role-granted->db
                                       role-revoked->db]]
            [geo-sink.utils :refer [decode-base64 ipfs-fetch slurp-bytes
                                    write-bytes]]
            [geo.clojure.sink :as geo]
            [protojure.grpc.client.providers.http2 :as grpc.http2]
            [protojure.protobuf :as protojure]
            [sf.substreams.rpc.v2 :as rpc]
            [sf.substreams.rpc.v2.Stream.client :as stream]
            [sf.substreams.v1 :as v1]))
(def current-block (atom (:cursors/block_number (get-cursor))))
(def cursor (atom (:cursors/cursor (get-cursor))))
(def sink-mode (atom :populate-cache))

(defn file-exists? [filepath]
  (.exists (java.io.File. filepath)))



(defn handle-cursor-change [new-cursor-string current-block]
  (update-db-cursor new-cursor-string current-block)
  (write-cursor-cache-file new-cursor-string current-block))

(defn cursor-watcher
  "Watches the cursor for changes and updates the database as well as the cache file"
  [key ref old-cursor-string new-cursor-string]
  (handle-cursor-change new-cursor-string @current-block))
(add-watch cursor :watcher cursor-watcher)

(defn take-all [ch f]
  (loop []
    (let [val (async/<!! ch)]
      (if val
        (do
          (f val)
          (recur))
        (println "Channel done processing")))))

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

(defmulti process-geo-data (fn [_ b] b))

(defmethod process-geo-data :populate-cache
  [geo-output _]
  (let [entries (:entries geo-output)
        roles-granted (:roles-granted geo-output)
        roles-revoked (:roles-revoked geo-output)]
    (println (str "Caching -> Entries: " (count entries) ", Roles granted: " (count roles-granted) ", Roles revoked: " (count roles-revoked)))
    (doseq [entry entries]
      (let [entry-filename (format-entry-filename entry)]
        (write-bytes (str cache-entry-directory entry-filename) (protojure/->pb entry))
        (when (not (file-exists? (str cache-action-directory entry-filename)))
          (spit (str cache-action-directory entry-filename) (uri-data (:uri entry))))))
    (doseq [entry roles-granted]
      (when (not (= :null (:role entry)))
        (write-bytes (str cache-granted-directory (:id entry)) (protojure/->pb entry))))
    (doseq [entry roles-revoked]
      (when (not (= :null (:role entry)))
        (write-bytes (str cache-revoked-directory (:id entry)) (protojure/->pb entry))))))

(defmethod process-geo-data :populate-db
  [geo-output _]

  (let [entries (:entries geo-output),
        roles-granted (:roles-granted geo-output)
        roles-revoked (:roles-revoked geo-output)]
    (println (str "Populating -> Entries: " (count entries) ", Roles granted: " (count roles-granted) ", Roles revoked: " (count roles-revoked)))
    (doseq [entry entries]
      (let [block-number (Integer/parseInt @current-block)
            author (:author entry)
            space (:space entry)
            uri (:uri entry)
            uri-data (->> uri
                          uri-data)
            json (ch/parse-string uri-data true)
            actions (:actions json)
            proposal-name (:name json)
            valid-actions (format+filter-actions space author block-number proposal-name actions)]
        (actions->db valid-actions)))

    (doseq [role roles-granted]
      (when (not (= :null (:role role)))
        (println "Shai-Hulud")
        (role-granted->db role)))

    (doseq [role roles-revoked]
      (when (not (= :null (:role role)))
        (print "Harkonnen Forces")
        (role-revoked->db role)))))


(def spkg (v1/pb->Package (slurp-bytes "geo-substream-v1.0.2.spkg")))

(defn spawn-client [] @(grpc.http2/connect {:uri (env "SUBSTREAMS_ENDPOINT")
                                            :ssl true
                                            :idle-timeout 60000
                                            :metadata {"authorization" (env "SUBSTREAMS_API_TOKEN")}}))

(defn handle-block-scoped-data
  [data populate-cache]
  (try
    (let [message (:message data)
          block-data (:block-scoped-data message)
          stream-cursor (:cursor block-data)]
      (when block-data
        (let [output (:output block-data)
              map-output (:map-output output)
              map-value (:value map-output)
              geo-output (geo/pb->GeoOutput map-value)
              block-number (:number (:clock block-data))]
          (if (empty-output? geo-output)
            (when (= (mod block-number 100) 0)
              (println "Empty block at: " block-number))
            (do
              (when populate-cache
                (process-geo-data geo-output :populate-cache))
              (process-geo-data geo-output :populate-db)))
          (swap! current-block (fn [_] (str block-number)))
          (swap! cursor (fn [_] stream-cursor)))))
    (catch Exception e
      (println "GOT ERROR: \n\n\n\n\n" e))))


(defn start-stream
  ([client populate-cache]
   (let [channel (async/chan (async/buffer 10))]
     (stream/Blocks client (rpc/new-Request {:start-block-num (Integer/parseInt @current-block)
                                             :start-cursor @cursor
                                             :modules (:modules spkg)
                                             :output-module "geo_out"}) channel)
     (take-all channel (partial handle-block-scoped-data populate-cache)))))
 