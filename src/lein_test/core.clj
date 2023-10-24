(ns lein-test.core
  (:gen-class)
  (:require
   [cheshire.core :as ch]
   [lein-test.constants :refer [ENTITIES]]
   [clojure.string :as cstr]
   [clojure.java.io :as io]
   [lein-test.spec.action :as action]
   [honey.sql :as sql]
   [honey.sql.helpers :as h]
   [next.jdbc :as jdbc]
   [lein-test.tables :refer [->action ->triple ->entity ->entity-type ->entity-attribute ->spaces]]
   [lein-test.db-helpers :refer [nuke-db try-execute create-type-tables make-space-schemas]]))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(defn- validate-actions
  ([actions]
   (filter #(action/valid-action? %) actions))
  ([space author actions]
   (->> (filter #(action/valid-action? %) actions)
        (map #(assoc % :space space :author author)))))

(defn- json->actions
  ([path]
   (let [json (slurp path)]
     (validate-actions ((ch/parse-string json true) :actions))))
  ([prefix path]
   (let [json (slurp (str prefix path))]
     (validate-actions ((ch/parse-string json true) :actions))))
  ([prefix path space author]
   (let [json (slurp (str prefix path))]
     (->> (ch/parse-string json true)
          :actions
          (validate-actions space author)))))

(defn- extract-file-meta [filename]
  (let [parts (cstr/split filename #"_")]
    {:block (Integer/parseInt (get parts 0))
     :index (Integer/parseInt (get parts 1))
     :space (.toLowerCase (get parts 2))
     :author (-> (get parts 3)
                 (cstr/replace #"\.json" ""))
     :filename filename}))

(defn sort-files [files]
  (sort-by (juxt :block :index) files))

(def files (->> (io/file "./action_cache/")
                file-seq
                rest
                (map #(cstr/replace % #"./action_cache/" ""))
                (map extract-file-meta)
                sort-files
                (map #(json->actions "./action_cache/" (:filename %) (:space %) (:author %)))))

(defn populate-entities
  "Takes in a seq of actions and populates the `entities` table"
  [actions]
  (-> (h/insert-into :entities)
      (h/values (into [] (map ->entity actions)))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

(defn populate-triples
  "Takes in a seq of actions and populates the `triples` table"
  [actions]
  (-> (h/insert-into :triples)
      (h/values (map ->triple actions))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

(defn populate-actions
  "Takes in a seq of actions and populates the `actions` table"
  [actions]
  (-> (h/insert-into :actions)
      (h/values (into [] (map ->action actions)))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

(defn populate-types
  "Takes in a seq of actions and populates the `entity_types` table"
  [actions]
  (let [filtered (into [] (filter #(= (:attributeId %) "type") actions))]
    (-> (h/insert-into :entity_types)
        (h/values (into [] (map ->entity-type filtered)))
        (h/on-conflict :id (h/do-nothing))
        (sql/format {:pretty true})
        try-execute)))
;; (let [type-creations (filter #((= (:id (:value %)) "type")) filtered)]
;;       (map #(try-execute (sql/format {:create-table (keyword (:space %) (:entityId %)) :with-columns [[:id :text [:not nil]]
;;                                                                                                       [:entity_id :text [:not nil] [:references :public/entities :id]]]})) type-creations))

(defn populate-attributes [actions]
  (let [filtered (into [] (filter #(= (:attributeId %) "01412f83-8189-4ab1-8365-65c7fd358cc1") actions))]
    (-> (h/insert-into :entity_attributes)
        (h/values (into [] (map ->entity-attribute filtered)))
        (h/on-conflict :id (h/do-nothing))
        (sql/format {:pretty true})
        try-execute)))

(defn populate-spaces [actions]
  (let [filtered (into [] (filter #(= (:attributeId %) "space") actions))]
    (-> (h/insert-into :spaces)
        (h/values (into [] (map ->spaces filtered)))
        (h/on-conflict :id (h/do-nothing))
        (sql/format {:pretty true})
        try-execute)))

(defn populate-db [type actions]
  (cond (= type :entities) (populate-entities actions)
        (= type :triples) (populate-triples actions)
        (= type :actions) (populate-actions actions)
        (= type :types) (populate-types actions)
        (= type :attributes) (populate-attributes actions)
        (= type :spaces) (populate-spaces actions)
        :else (throw (ex-info "Invalid type" {:type type}))))

(nuke-db)

(time
 (do
   (doall (map #(populate-db :entities %) files))
   (doall (map #(populate-db :triples %) files))
   (doall (map #(populate-db :spaces %) files))
   (doall (map #(populate-db :types %) files))
   (doall (map #(populate-db :attributes %) files))
   (make-space-schemas)
   (create-type-tables)
   (println "done with everything")))
