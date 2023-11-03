(ns lein-test.core
  (:gen-class)
  (:require [cheshire.core :as ch]
            [clojure.java.io :as io]
            [clojure.string :as cstr]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [lein-test.constants :refer [ATTRIBUTES ENTITIES]]
            [lein-test.db-helpers :refer [try-execute]]
            [lein-test.pg-function-helpers :refer [populate-pg-functions]]
            [lein-test.spec.action :as action]
            [lein-test.tables :refer [->action ->entity ->spaces ->triple]]))


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
  (let [filtered-actions (filter #(not (nil? (:entityId %))) actions)]
    (when (< 0 (count filtered-actions))
      (-> (h/insert-into :public/entities)
          (h/values (into [] (map ->entity filtered-actions)))
          (h/on-conflict :id (h/do-nothing))
          (sql/format {:pretty true})
          try-execute))))


(defn populate-triples
  "Takes in a seq of actions and populates the `triples` table"
  [actions]
    (let [create-triple-actions (filter #(= (:type %) "createTriple") actions)
          delete-triple-actions (filter #(= (:type %) "deleteTriple") actions)]
      (when (< 0 (count create-triple-actions))    
      (-> (h/insert-into :public/triples)
          (h/values (map ->triple create-triple-actions))
          (h/on-conflict :id (h/do-nothing))
          (sql/format {:pretty true})
          try-execute)
      )

     (when (< 0 (count delete-triple-actions))     
      (-> (h/update :public/triples)
          (h/values [{:deleted true}])
          (h/where [:in :id (map :id (map ->triple delete-triple-actions))])
          (sql/format {:pretty true})
          try-execute))
    )
)
  

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
   :status "APPROVED" ;NOTE THIS IS HARDCODED FOR NOW UNTIL GOVERNANCE FINALIZED
   })

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



(defn update-entity
  "Updates a specific entity in the table."
  [entityId column value]
  (try-execute (-> (h/update :public/entities)
                   (h/set {column value})
                   (h/where [:= :id entityId])
                   (sql/format {:pretty true}))))

(defn populate-columns
  "Takes actions as arguments, processes them to find blessed columns to update and updates them."
  [actions]
  (let [name-attr-id (:id (:name ATTRIBUTES))
        description-attr-id (:id (:description ATTRIBUTES))
        value-type-attr-id (:id (:value-type ATTRIBUTES))
        type (:id (:type ATTRIBUTES))
        attribute (:id (:attribute ENTITIES))
        schema-type (:id (:schema-type ENTITIES))]
    (doseq [action actions]
      (let [triple (->triple action)
            is-name-update (and (= (:attribute_id triple) name-attr-id)
                                (= (:value_type triple) "string"))
            is-description-update (and (= (:attribute_id triple) description-attr-id)
                                       (= (:value_type triple) "string"))
            is-value-type-update (and (= (:attribute_id triple) value-type-attr-id)
                                      (= (:value_type triple) "entity"))
            is-type-flag-update (and (= (:attribute_id triple) type)
                                     (= (:value_id triple) schema-type))
            is-attribute-flag-update (and (= (:attribute_id triple) type)
                                          (= (:value_id triple) attribute))
            action-type (:type action)
            is-delete-triple (= action-type "deleteTriple")]

        ;; TODO: Add Space and Account Updates
        (when is-name-update
          (update-entity (:entity_id triple) :name (if is-delete-triple nil (:string_value triple))))
        (when is-description-update
          (update-entity (:entity_id triple) :description (if is-delete-triple nil (:string_value triple))))
        (when is-value-type-update
          (update-entity (:entity_id triple) :attribute_value_type_id (if is-delete-triple nil (:value_id triple))))
        (when is-type-flag-update
          (update-entity (:entity_id triple) :is_type (if is-delete-triple nil (boolean (:value_id triple)))))
        (when is-attribute-flag-update
          (update-entity (:entity_id triple) :is_attribute (if is-delete-triple nil (boolean (:value_id triple)))))))))

(defn populate-db [type log-entry]
  (cond (= type :entities) (populate-entities log-entry)
        (= type :triples) (populate-triples log-entry)
        (= type :accounts) (populate-account log-entry)
        (= type :spaces) (populate-spaces log-entry)
        (= type :proposals) (populate-proposals-from-entry log-entry)
        (= type :columns) (populate-columns log-entry)
        :else (throw (ex-info "Invalid type" {:type type}))))

(defn -main
  "I DO SOMETHING NOW!"
  [& args]
    ;; (time (bootstrap-db))
    ;; (time (doall (map #(populate-db :entities %) files)))
    (time (doall (map #(populate-db :triples %) files)))
    (time (doall (map #(populate-db :spaces %) files)))
    (time (doall (map #(populate-db :accounts %) files)))
    (time (doall (map #(populate-db :columns %) files)))
    (time (doall (map #(populate-db :proposals %) files)))
     (populate-pg-functions)
     (println "done with everything"))
