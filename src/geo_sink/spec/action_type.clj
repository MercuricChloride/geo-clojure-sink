(ns geo-sink.spec.action-type
  (:require
   [clojure.spec.alpha :as s]
   [geo-sink.constants :as c]
   [geo-sink.spec.action :as a]))

(defn value-type [triple]
  (:type (:value triple)))

(defn space?
  "A triple that is Creating/Removing a space"
  [triple]
  (if (s/valid? ::a/actionTriple triple)
    (and
     (= (:attributeId triple) "space")
     (= (value-type triple) "string"))
    false))

(defn type?
  "A triple that is Adding/Removing a type from an entity"
  [triple]
  (if (s/valid? ::a/actionTriple triple)
    (and
     (= (:attributeId triple) (:type c/ATTRIBUTES))
     (= (value-type triple) "entity"))
    false))

(defn attribute?
  "A triple that is adding/Removing an attribute from a type(entity)"
  [triple]
  (if (s/valid? ::a/actionTriple triple)
    (and
     (= (:attributeId triple) (:attribute c/ATTRIBUTES))
     (= (value-type triple) "entity"))
    false))

(defn value-type?
  "A triple that is Adding/Removing a value-type from an entity"
  [triple]
  (if (s/valid? ::a/actionTriple triple)
    (and
     (= (:attributeId triple) (:value-type c/ATTRIBUTES))
     (= (value-type triple) "entity"))
    false))

(defn cover?
  "A triple that is Adding/Removing a cover from a space"
  [triple]
  (if (s/valid? ::a/actionTriple triple)
    (and
     (= (:attributeId triple) (:cover c/ATTRIBUTES))
     (= (value-type triple) "entity"))
    false))

(defn subspace?
  "A triple that is Adding/Removing a subspace from an entity"
  [triple]
  (if (s/valid? ::a/actionTriple triple)
    (and
     (= (:attributeId triple) (:subspace c/ATTRIBUTES))
     (= (value-type triple) "entity"))
    false))

(defn triple?
  "A triple that is Adding/Removing a triple"
  [triple]
  (if (s/valid? ::a/actionTriple triple)
    (= (:type triple) "createTriple")
    false))

(defn entity?
  "A triple that is Adding/Removing an entity"
  [triple]
  (if (s/valid? ::a/actionTriple triple)
    (= (:type triple) "createEntity")
    false))

(defn avatar?
  "A triple that is Adding/Removing an avatar to an entity"
  [triple]
  (if (s/valid? ::a/actionTriple triple)
    (and
     (= (:attributeId triple) (:avatar c/ATTRIBUTES))
     (= (value-type triple) "string"))
    false))

(defn name?
  "A triple that is Adding/Removing a name to an entity"
  [triple]
  (if (s/valid? ::a/actionTriple triple)
    (and
     (= (:attributeId triple) (:name c/ATTRIBUTES))
     (= (value-type triple) "string"))
    false))

(defn description?
  "A triple that is Adding/Removing a description to an entity"
  [triple]
  (if (and
     (s/valid? ::a/actionTriple triple)
     (= (:attributeId triple) (:name c/ATTRIBUTES))
     (= (value-type triple) "string"))
    true
    false))

(defn action-type
  "Returns the type of action that the triple is"
[action-triple]
  (let [type (:type action-triple)]
   (cond
     (= type "createTriple") ::create-triple
     (= type "deleteTriple") ::delete-entity
     (= type "createEntity") ::create-entity
     :else (throw (ex-info "Invalid action triple" {:action-triple action-triple})))))

(defn sink-action
  "Returns the type of sink action to take for a triple"
  [triple]
  (cond
    (space? triple) ::space
    (type? triple) ::type
    (attribute? triple) ::attribute
    (value-type? triple) ::value-type
    (cover? triple) ::cover
    (subspace? triple) ::subspace
    (avatar? triple) ::avatar
    (name? triple) ::name
    (description? triple) ::description))

(defmulti execute
  "Returns a map with keys: `:dependencies` and `:handler`.
  `:dependencies` is optional and contains a future that should be executed before the :handler is executed.
  `:handler` should contain a delay that will be executed after the :dependencies future is completed. (if it exists)"
 sink-action)

(defmethod execute ::space [input] {:dependencies (future (Thread/sleep 20) (println "deps handled") true)
                                    :handler (delay (Thread/sleep 20) (println "main action handeld"))})

(defmethod execute ::type [input] (future (Thread/sleep 2000) "TYPE!"))

(defmulti dependencies
  "Returns a vec of dependencies that a particular action requires if any, nil otherwise"
  sink-action)

(defmethod dependencies :default [] nil)
