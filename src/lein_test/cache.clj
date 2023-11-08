(ns lein-test.cache
  (:require
   [cheshire.core :as ch]
   [clojure.java.io :as io]
   [clojure.string :as string]
   [geo.clojure.sink :as geo]
   [lein-test.spec.action :as action]
   [lein-test.utils :refer [slurp-bytes]]))

(defn- validate-actions
  ([actions]
   (filter #(action/valid-action? %) actions))
  ([space author block-number actions]
   (->> (filter #(action/valid-action? %) actions)
        (map #(assoc % :space space :author author :block-number block-number)))))

(defn- json->actions
  ([path]
   (let [json (slurp path)]
     (validate-actions ((ch/parse-string json true) :actions))))
  ([prefix path]
   (let [json (slurp (str prefix path))]
     (validate-actions ((ch/parse-string json true) :actions))))
  ([prefix path space author block-number]
   (let [json (slurp (str prefix path))]
     (->> (ch/parse-string json true)
          :actions
          (validate-actions space author block-number)))))

(defn sort-files [files]
  (sort-by (juxt :block :index) files))

(defn- extract-file-meta [filename]
  (let [parts (string/split filename #"_")]
    {:block (Integer/parseInt (get parts 0))
     :index (Integer/parseInt (get parts 1))
     :space (.toLowerCase (get parts 2))
     :author (get parts 3)
     :filename filename}))

(def new-files (->> (io/file "./new-cache/entries-added/")
                    file-seq
                    rest
                    (map #(geo/pb->EntryAdded (slurp-bytes %)))))

(def roles-granted (->> (io/file "./new-cache/roles-granted/")
                    file-seq
                    rest
                    (map #(geo/pb->RoleGranted (slurp-bytes %)))
                    (filter #(not (= (:role %) :null)))))

(def roles-revoked (->> (io/file "./new-cache/roles-revoked/")
                    file-seq
                    rest
                    (map #(geo/pb->RoleRevoked (slurp-bytes %)))
                    (filter #(not (= (:role %) :null)))))

(def cached-log-entries (->> (io/file "./new-cache/actions/")
                             file-seq
                             rest
                             (map #(string/replace % #"./new-cache/actions/" ""))
                             (map extract-file-meta)
                             sort-files
                             (map #(json->actions "./new-cache/actions/" (:filename %) (:space %) (:author %) (:block %)))))
