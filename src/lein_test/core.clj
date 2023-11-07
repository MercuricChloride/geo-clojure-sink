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
   [lein-test.substreams :as substreams]
   [lein-test.tables :refer [->action ->triple ->entity ->entity-type ->entity-attribute ->spaces]]
   [lein-test.db-helpers :refer [nuke-db bootstrap-db try-execute create-type-tables make-space-schemas get-cursor]]
   [geo.clojure.sink :as geo]))


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
    (println parts)
    {:block (Integer/parseInt (get parts 0))
     :index (Integer/parseInt (get parts 1))
     :space (.toLowerCase (get parts 2))
     :author (get parts 3)
     :filename filename}))

(defn sort-files [files]
  (sort-by (juxt :block :index) files))

(def new-files (->> (io/file "./new-cache/entries-added/")
                    file-seq
                    rest
                    (map #(geo/pb->EntryAdded (substreams/slurp-bytes %)))))

(def roles-granted (->> (io/file "./new-cache/roles-granted/")
                    file-seq
                    rest
                    (map #(geo/pb->RoleGranted (substreams/slurp-bytes %)))
                    (filter #(not (= (:role %) :null)))))

(def roles-revoked (->> (io/file "./new-cache/roles-revoked/")
                    file-seq
                    rest
                    (map #(geo/pb->RoleRevoked (substreams/slurp-bytes %)))
                    (filter #(not (= (:role %) :null)))))

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
    formatted-sql))
    


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
      (h/values (into [] actions))
      (h/on-conflict :id (h/do-nothing))
      (sql/format {:pretty true})
      try-execute))

(defn populate-spaces [actions]
  (let [filtered (filter #(= (:attributeId %) "space") actions)]
    (when (< 0 (count filtered))
        (-> (h/insert-into :spaces)
            (h/values (into [] (map ->spaces filtered)))
            (h/on-conflict :id (h/do-nothing))
            (sql/format {:pretty true})
            try-execute))))

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

(defn ->proposed-version+actions
  [action-map created-at-block timestamp author proposal-id]
  (let [proposed-version-id (str (java.util.UUID/randomUUID))]
    [{:id proposed-version-id
      :name "update-me" ;TODO UPDATE THIS
      :description "update-me" ;TODO UPDATE THIS
      :created-at timestamp
      :created-at-block created-at-block
      :created-by author
      :entity (:entityId (first action-map))
      :proposal-id proposal-id}
      
     (map #(->action % proposed-version-id) action-map)]))
    
  

(defn new-proposal
  [created-at-block timestamp author proposal-id space]
  {:id proposal-id
   :name "update-me"
   :description "update-me"
   :created-at timestamp
   :created-at-block created-at-block
   :created-by author
   :space space
   :status "APPROVED"}) ;NOTE THIS IS HARDCODED FOR NOW UNTIL GOVERNANCE FINALIZED
   

(defn populate-proposal
  [proposal]
  (-> (h/insert-into :public/proposals)
      (h/values [proposal])
      (h/on-conflict :id (h/do-nothing))
      (sql/format)
      try-execute))

(defn populate-proposed-version
  [proposed-version]
  (-> (h/insert-into :public/proposed-versions)
      (h/values [proposed-version])
      (h/on-conflict :id (h/do-nothing))
      (sql/format)
      try-execute))

(defn entry->proposal
  [log-entry]
  (let [first-entry (first log-entry)
        author (:author first-entry) ;NOTE the author will be the same for all triples in an action
        space (:space first-entry)
        block-number (:block-number first-entry)
        proposal-id (str (java.util.UUID/randomUUID))
        entity-ids (into #{} (map :entityId log-entry))
        action-map (map #(entry->actions log-entry %) entity-ids)
        proposal (new-proposal block-number block-number author proposal-id space)
        proposed-versions+actions (map #(->proposed-version+actions % block-number block-number author proposal-id) action-map)]
    [proposal proposed-versions+actions]))

(defn- populate-proposed-version+actions
  [input]
  (let [[proposed-version actions] input]
    (populate-proposed-version proposed-version)
    (populate-actions actions)))

(defn populate-proposals-from-entry
  [log-entry]
  (let [[proposal proposed-version+actions] (entry->proposal log-entry)]
    (populate-proposal proposal)
    (map populate-proposed-version+actions proposed-version+actions)))

(defn- entry->author
  [entry]
  (->> entry
     first
     :author))

(defn populate-account
  [log-entry]
  (let [account (entry->author log-entry)]
    (when (not (nil? account))
      (-> (h/insert-into :public/accounts)
          (h/values [{:id account}])
          (h/on-conflict :id (h/do-nothing))
          (sql/format)
          try-execute))))

;; (->> files
;;     (map entry->author)
;;     (into #{})
;;     (into [])
;;     (filter #(not (nil? %)))
;;     populate-accounts)

(defn ipfs-fetch
  [cid]
  (slurp (str "https://ipfs.network.thegraph.com/api/v0/cat?arg=" cid)))

;(ch/parse-string (ipfs-fetch "QmYxqYRTxGT2VywaH5P9B6gBHs4ZMUKwEtR7tdQFTAonQY"))

(defn populate-db [type log-entry]
  (cond (= type :entities) (populate-entities log-entry)
        (= type :triples) (populate-triples log-entry)
        (= type :accounts) (populate-account log-entry)
        (= type :spaces) (populate-spaces log-entry)
        (= type :proposals) (populate-proposals-from-entry log-entry)
        :else (throw (ex-info "Invalid type" {:type type}))))

(def start-block 36472424)
(def stop-block 48000000)

(defn handle-args
  [args]
  (let [args (into #{} args)
        from-genesis (get "--from-genesis" args)]
     (when from-genesis
       (swap! substreams/current-block (fn [_] start-block))
       (swap! substreams/cursor (fn [_] "")))))

(defn -main
  "I DO SOMETHING NOW!"
  [& args]
  (handle-args args)

  (while (< (Integer/parseInt @substreams/current-block) stop-block)
    (println "Starting stream at block #" (str @substreams/current-block))
    (let [client (substreams/spawn-client)]
      (substreams/start-stream client start-block stop-block))))
