(ns geo-sink.constants)

(def ATTRIBUTES {:type {:id "type"
                        :name "Types"
                        :value-type :relation}
                 :attribute {:id "01412f83-8189-4ab1-8365-65c7fd358cc1"
                             :name "Attributes"
                             :value-type :relation}
                 :space {:id "space"
                         :name "Space"
                         :value-type :text}
                 :name {:id "name"
                        :name "Name"
                        :value-type :text}
                 :description {:id "Description"
                               :name "Description"
                               :value-type :text}
                 :value-type {:id "ee26ef23-f7f1-4eb6-b742-3b0fa38c1fd8"
                              :name "Value Type"
                              :value-type :relation}
                 :subspace {:id "442e1850-9de7-4002-a065-7bc8fcff2514"
                            :name "Subspace"
                            :value-type :relation}
                 :avatar {:id "235ba0e8-dc7e-4bdd-a1e1-6d0d4497f133"
                          :name "Avatar"
                          :value-type :text}
                 :cover {:id "34f53507-2e6b-42c5-a844-43981a77cfa2"
                         :name "Cover"
                         :value-type :text}
                 :image {:id "457a27af-7b0b-485c-ac07-aa37756adafa"
                         :name "Image"
                         :value-type :text}
                 :indexed-space {:id "30659852-2df5-42f6-9ad7-2921c33ad84b"
                                 :name "Indexed Space"
                                 :value-type :relation}
                 :foreign-types {:id "be745973-05a9-4cd0-a46d-1c5538270faf"
                                 :name "Foreign Types"
                                 :value-type :relation}
                 :blocks {:id "beaba5cb-a677-41a8-b353-77030613fc70"
                          :name "Blocks"
                          :value-type :relation}
                 :parent-entity {:id "dd4999b9-77f0-4c2b-a02b-5a26b233854e"
                                 :name "Parent Entity"
                                 :value-type :relation}
                 :markdown-content {:id "f88047ce-bd8d-4fbf-83f6-58e84ee533e4"
                                    :name "Markdown Content"
                                    :value-type :text}
                 :row-type {:id "577bd9fb-b29e-4e2b-b5f8-f48aedbd26ac"
                            :name "Row Type"
                            :value-type :relation}
                 :filter {:id "b0f2d71a-79ca-4dc4-9218-e3e40dfed103"
                          :name "Filter"
                          :value-type :text}
                 :relation-value-relationship-type {:id "cfa6a2f5-151f-43bf-a684-f7f0228f63ff"
                                                    :name "Relation Value Relationship Type"
                                                    :value-type :relation}})

(def ENTITIES {:schema-type {:id "d7ab4092-0ab5-441e-88c3-5c27952de773"
                             :name "Type"
                             :attributes [:attribute]}
               :image {:id "ba4e4146-0010-499d-a0a3-caaa7f579d0e"
                       :name "Image"}
               :relation {:id "14611456-b466-4cab-920d-2245f59ce828"
                          :name "Relation"}
               :text {:id "9edb6fcc-e454-4aa5-8611-39d7f024c010"
                      :name "Text"}
               :date {:id "167664f6-68f8-40e1-976b-20bd16ed8d47"
                      :name "Date"}
               :web-url {:id "dfc221d9-8cce-4f0b-9353-e437a98387e3"
                         :name "Web URL"}
               :space-configuration {:id "1d5d0c2a-db23-466c-a0b0-9abe879df457"
                                     :name "Space Configuration"
                                     :attributes [:foreign-types]}
               :table-block {:id "88d59252-17ae-4d9a-a367-24710129eb47"
                             :name "Table Block"
                             :attributes [:row-type :parent-entity]}
               :text-block {:id "8426caa1-43d6-47d4-a6f1-00c7c1a9a320"
                            :name "Text Block"
                            :attributes [:markdown-content :parent-entity]}
               :image-block {:id "f0553d4d-4838-425e-bcd7-613bd8f475a5"
                             :name "Image Block"
                             :attributes [:image :parent-entity]}
               :attribute {:id "attribute"
                           :name "Attribute"}})

(def ROOT-SPACE-ADDRESS "0x170b749413328ac9a94762031a7a05b00c1d2e34")

(def geo-genesis-start-block 36472424)

(def cache-entry-directory "cache/entries-added/")
(def cache-granted-directory "cache/roles-granted/")
(def cache-revoked-directory "cache/roles-revoked/")
(def cache-action-directory "cache/actions/")
(def cache-cursor-file "cache/cursor.json")



