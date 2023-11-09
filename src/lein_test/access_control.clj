(ns lein-test.access-control)

(def ADMIN_ROLE :admin)
(def MEMBER_ROLE :member)
(def MODERATOR_ROLE :moderator)

(defn add-role [params]
  (let [account (:account params)
        role (:role params)
        spaceAddress (:space params)]
    (cond
      (= role ADMIN_ROLE) (println "Granted admin role to {} in {}" [account spaceAddress])
      (= role MEMBER_ROLE) (println "Granted member role to {}" [account spaceAddress])
      (= role MODERATOR_ROLE) (println "Granted moderator role to {}" [account spaceAddress])
      :else (println "Received unexpected role value: {}" [role]))))

(defn remove-role [params]
  (let [account (:account params)
        role (:role params)
        spaceAddress (:space params)]
    (cond
      (= role ADMIN_ROLE) (println "Granted admin role to {} in {}" [account spaceAddress])
      (= role MEMBER_ROLE) (println "Granted member role to {}" [account spaceAddress])
      (= role MODERATOR_ROLE) (println "Granted moderator role to {}" [account spaceAddress])
      :else (println "Received unexpected role value: {}" [role]))))
