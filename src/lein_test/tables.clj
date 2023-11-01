(ns lein-test.tables)

(defn value-type
  "Returns the value type as a key for a triple's value"
  [value]
  (let [v (:type value)]
    (cond
      (= v "string") :string_value
      (= v "entity") :entity_value)))

(defn ->action
  "Takes in an action and returns an entry in the actions table"
  [action]
  (let [type (:type action) entityId (:entityId action) attributeId (:attributeId action)]
    {:id (str type "-" entityId "-" attributeId)
     :action_type type
     :entity entityId
     :attribute attributeId}))

(defn ->triple
  "Takes in an action and returns an entry in the triples table"
  [action]
  (let [entity-id (:entityId action) attribute-id (:attributeId action) value-key (value-type (:value action)) value-id (:id (:value action)) value (:value (:value action)) space (:space action)]
    {:id (str (java.util.UUID/randomUUID))
     :entity_id entity-id
     :attribute_id attribute-id
     value-key (if (= value-key :entity_value) value-id value)
     :value_id value-id
     :value_type (:type (:value action))
     :defined_in space
     :is_protected false
     :deleted false}))

(defn ->entity
  "Takes in an action and returns an entry in the entities table"
  [action]
  (let [entityId (:entityId action)]
    {:id entityId}))

(defn ->entity-type
  "Takes in an action and returns an entry in the entity_types table"
  [action]
  (let [entity-id (:entityId action) type-id (:id (:value action))]
    {:id (str entity-id "-" type-id)
     :entity_id entity-id
     :type type-id}))

(defn ->entity-attribute
  "Takes in an action and returns an entry in the entity_attributes table"
  [action]
  (let [entity (:entityId action) attribute (:id (:value action))]
    {:id (str entity "-" attribute)
     :entity_id entity
     :attribute_of attribute}))

(defn ->spaces
  "Takes in an action and returns an entry in the spaces table"
  [action]
  (let [entity (:entityId action) address (:value (:value action))]
    {:id (str entity)
     :address address
     :is_root_space false}))
