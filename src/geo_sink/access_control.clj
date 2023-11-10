(ns geo-sink.access-control
  (:require [geo-sink.db-helpers :refer [try-execute]]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]))
(def ADMIN_ROLE :admin)
(def MEMBER_ROLE :member)
(def MODERATOR_ROLE :moderator)

(defn role-kind
  [role]
  (let [role (:role role)]
    (cond
      (= role ADMIN_ROLE) :admin
      (= role MEMBER_ROLE) :member
      (= role MODERATOR_ROLE) :moderator
      :else :unknown)))

(defmulti role->db role-kind)

(defmethod role->db :admin
  [role-added]
  (-> (h/insert-into :public/space_admins)
      (h/values [{:space (:space role-added)
                 :account (:account role-added)}])
      (sql/format)
      try-execute))

(defmethod role->db :member
  [role-added]
  (-> (h/insert-into :public/space_editors)
      (h/values [{:space (:space role-added)
                 :account (:account role-added)}])
      (sql/format)
      try-execute))

(defmethod role->db :moderator
  [role-added]
  (-> (h/insert-into :public/space_editor_controllers)
      (h/values [{:space (:space role-added)
                 :account (:account role-added)}])
      (sql/format)
      try-execute))

(defmethod role->db :unknown
  [role-added]
  (println "Skipping unknown role added"))

(defmulti remove-role->db role-kind)

(defmethod remove-role->db :admin
  [role-removed]
  (-> (h/delete-from :public/space_admins)
      (h/where {:space (:space role-removed)
                :account (:account role-removed)})
      (sql/format)
      try-execute))

(defmethod remove-role->db :member
  [role-removed]
  (-> (h/delete-from :public/space_editors)
      (h/where {:space (:space role-removed)
                :account (:account role-removed)})
      (sql/format)
      try-execute))

(defmethod remove-role->db :moderator
  [role-removed]
  (-> (h/delete-from :public/space_editor_controllers)
      (h/where {:space (:space role-removed)
                :account (:account role-removed)})
      (sql/format)
      try-execute))

(defmethod remove-role->db :unknown
  [role-removed]
  (println "Skipping unknown role removed"))

(defn add-role [role-added]
  (role->db role-added))

(defn remove-role [role-removed]
  (remove-role->db role-removed))
