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
   [lein-test.db-helpers :refer [nuke-db bootstrap-db try-execute create-type-tables make-space-schemas]]))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

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
                (map #(json->actions "./action_cache/" (:filename %) (:space %) (:author %) (:block %)))))

(defn populate-entities
  "Takes in a seq of actions and populates the `entities` table"
  [actions]
  (let [formatted-sql (-> (h/insert-into :public/entities)
                          (h/values (into [] (map ->entity actions)))
                          (h/on-conflict :id (h/do-nothing))
                          (sql/format {:pretty true})
                          try-execute)]
    (println "Generated SQL:" formatted-sql)
    formatted-sql
    ))


(defn populate-triples
  "Takes in a seq of actions and populates the `triples` table"
  [actions]
  (-> (h/insert-into :public/triples)
      (h/values (map ->triple actions))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

(defn populate-actions
  "Takes in a seq of actions and populates the `actions` table"
  [actions]
  (-> (h/insert-into :public/actions)
      (h/values (into [] (map ->action actions)))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

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
        (= type :spaces) (populate-spaces actions)
        :else (throw (ex-info "Invalid type" {:type type}))))

 ;; (time
 ;;  (do
 ;;    (time (doall (map #(populate-db :entities %) files)))
 ;;    (time (doall (map #(populate-db :triples %) files)))
 ;; ;;    (time (doall (map #(populate-db :spaces %) files)))
 ;; ;;    (time (make-space-schemas))
 ;;    (println "done with everything")))

(def template-function-str
    "CREATE OR REPLACE FUNCTION \"type-$$ENTITY_ID$$\"(ent_id text)
    RETURNS public.entity_types AS $$
    DECLARE
        result_record public.entity_types;
    BEGIN
        SELECT *
        INTO result_record
        FROM entity_types
        WHERE type = '$$ENTITY_ID$$'
        AND entity_id = ent_id;

        RETURN result_record;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END;
    $$ LANGUAGE plpgsql STRICT STABLE;
    comment on function \"type-$$ENTITY_ID$$\"(_entity_id text) is E'@name $$ENTITY_NAME$$';
    ")

(defn type-function
  "Creates a function in the DB to get all entities of a given type"
  [entity-id entity-name]
  (-> template-function-str
    (cstr/replace "$$ENTITY_ID$$" entity-id)
    (cstr/replace "$$ENTITY_NAME$$" entity-name)))

(def all-type-function
    "CREATE OR REPLACE FUNCTION allTypes()
    RETURNS SETOF entities AS $$
    BEGIN
    RETURN QUERY
    SELECT e.*
    FROM entities e
    WHERE e.is_type = true;
    END;
    $$ LANGUAGE plpgsql STRICT STABLE;")

(def all-attribute-function
    "CREATE OR REPLACE FUNCTION entities_attributes(e entities)
    RETURNS SETOF entities AS $$
    BEGIN
    RETURN QUERY
    SELECT e.*
    FROM entities
    JOIN triples t ON entities.id = t.entity_id
    WHERE t.attribute_id = '01412f83-8189-4ab1-8365-65c7fd358cc1' AND e.id = t.value_id;
    END;
    $$ LANGUAGE plpgsql STRICT STABLE;")

(defn entry->actions
  [entry entity-id]
  (filter #(= (:entityId %) entity-id) entry))

;; (try-execute (sql/format [:raw all-type-function]))
;; (try-execute (sql/format [:raw all-attribute-function]))
;; (try-execute (sql/format [:raw (type-function "type" "types")]))

(defn ->proposed-version
  [action-map created-at-block timestamp author proposal-id]
  {:id (str (java.util.UUID/randomUUID))
   :name "update-me" ;TODO UPDATE THIS
   :description "update-me" ;TODO UPDATE THIS
   :created-at timestamp
   :created-at-block created-at-block
   :created-by author
   :entity (:entityId (first action-map))
   :proposal-id proposal-id
   })

(defn new-proposal
  [created-at-block timestamp author proposal-id]
  {:id proposal-id
   :name "update-me"
   :description "update-me"
   :created-at timestamp
   :created-at-block created-at-block
   :created-by author
   :status "APPROVED" ;NOTE THIS IS HARDCODED FOR NOW UNTIL GOVERNANCE FINALIZED
   })

(defn entry->proposal
  [log-entry]
  (let [first-entry (first log-entry)
        author (:author first-entry) ;NOTE the author will be the same for all triples in an action
        block-number (:block-number first-entry)
        proposal-id (str (java.util.UUID/randomUUID))
        entity-ids (into #{} (map :entityId log-entry))
        action-map (map #(entry->actions log-entry %) entity-ids)
        proposal (new-proposal block-number block-number author proposal-id)
        proposals (map #(->proposed-version % block-number block-number author proposal-id) action-map)]
    ; insert proposal into proposals table pointing
        ; insert proposed-versions into proposed-versions table pointing to ^
            ; insert actions into actions table pointing to ^

    [proposal proposals]
    ))

(entry->proposal (nth files 100))


(defn ipfs-fetch
  [cid]
  (slurp (str "https://ipfs.network.thegraph.com/api/v0/cat?arg=" cid)))

(ch/parse-string (ipfs-fetch "QmYxqYRTxGT2VywaH5P9B6gBHs4ZMUKwEtR7tdQFTAonQY"))
