#!/usr/bin/env bb
(ns crud 
  (:require [babashka.pods :as pods]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.pprint :as pprint]
            [honey.sql :as hsql]
            [honey.sql.helpers :as hh]
            [clojure.edn :as edn]))                                                                                                                                         

(pods/load-pod 'org.babashka/postgresql "0.1.0")
(require '[pod.babashka.postgresql :as pg])

;; Config
(def conn (edn/read-string (slurp "config.edn")))    

(comment
  (pg/execute! conn ["select * from ops_student;"]))

;; DB access library functions
(defn get-all-students [src-table-name]                                                                                                 
  (let [query-map {:select [:*]
                   :from [(keyword src-table-name)]}
        query (hsql/format query-map)]
   (pg/execute! conn query)))
                                                                                                                                            
(defn create-student!                                                                                                                       
  "Input argument is a list of hashmap `m`, so it denoted as `ms`.                                                                          
                                                                                                                                            
   It does not matter if the hashmap `m` is using namespaced key or not.                                                                    
   However, m has to have a `serial` key.                                                                                                   
   If certain column does not exist in `m`, that column will be filled with NULL"                                                           
  [ms]                                                                                                                                      
  (let [cmd (-> (hh/insert-into :ops_student)                                                                                               
                (hh/values (vec ms))                                                                                                        
                (hsql/format {:pretty true}))]                                                                                               
    (prn "the batch size is: " (count ms))                                                                                                  
    (pg/execute! conn cmd)))                                                                                                              

(defn latest-seq-number!
  "Query the database to get next sequence number
   The sequence number in database will increment by 1"
  [conn]
  (let [cmd-map {:select [[[:nextval "ops_student_serial"]]]}
        cmd (hsql/format cmd-map)]
    (pg/execute! conn cmd)))

;; Pure library functions
(defn class-id->prefix
  "from center_symbol to classroom-type"
  [c-id]
  (cond
    (= 5 (count c-id)) [:classroom/ac "ac"]
    (> (count c-id) 2) [:classroom/franchise (subs c-id 0 2)]
    :else [:classroom/ghost "gh"]))

(defn compact
  "remove all the nil value inside a hashmap"
  [record]
  (into {} (remove (comp nil? second) record)))

;; Assemble functions: The functions which assemble pure functions and DB access functions.
(defn add-columns
  "Handle one student datum:
   - query the sequence number inside database
   - add serial and classroom-type columns"
  [conn src-table datum]
  (let [k (keyword src-table "center_symbol")
        center_symbol (k datum) 
        [tag prefix]  (class-id->prefix center_symbol)
        [{:keys [nextval]}] (latest-seq-number! conn)
        serial (str prefix (pprint/cl-format nil "~8,'0d" (inc nextval)))
        tx* (assoc datum :student/serial serial
                   :student/classroom-type (str tag))
        tx** (compact tx*)]
    tx**))

;; High level API: create-cmd, update-cmd
(defn create-cmd
  [src-table debug?]
  (let [students (get-all-students src-table)
        data (map (partial add-columns conn src-table) students)
        data-segments (partition 1000 1000 [] data)]
    (when debug?
      (prn "show first students: " (first students))
      (prn "show students count: " (count students))
      (prn "show first data: " (first data))
    )
    (dorun
      (map #(create-student! %) data-segments))))

;; Command line arguments processing
(def cli-options
  ;; An option with a required argument
  [["-s" "--src-table SRC_TABLE"
    "Specifiy the `src-table`, if omit, using the default value."
    :default "pre_ops_student_insert"]
   ["-h" "--help" "Showing usage summary"]
   ["-c" "--create" "Inserting new records"]
   ["-u" "--update" "Modifying existing records"]
   ["-d" "--debug" "Printing the intermediate states"]])

(defn main [{:keys [options arguments errors summary]}]
  (cond 
    (:create options) (create-cmd (:src-table options) (:debug options))
;;  (:update args) (update-cmd (:src-table args)) 
    (:help options) (println (str "Usage: \n"summary))
    :else (prn "no arguments recognized")))

(main (parse-opts *command-line-args* cli-options) )

;; crud.clj -d
;; crud.clj --create --src-table pre_ops_student_insert -d
;; crud.clj --update --src-table pre_ops_student_modify
