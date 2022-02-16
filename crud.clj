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

;; API here
(defn all-students-sqlmap
  "default value of src-table-name is pre_ops_student_insert"
  [src-table-name]                                                                                                                    
  {:select [:*]                                                                                                                             
   :from [(keyword src-table-name)]})                                                                                                                       
                                                                                                                                            
(defn get-all-students [src-table-name]                                                                                                 
  (pg/execute! conn 
               (hsql/format
                 (all-students-sqlmap src-table-name))))                                                                                    
                                                                                                                                            
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

(defn latest-seq-number [conn]
  (let [cmd {:select [[[:nextval "ops_student_serial"]]]}]
    (pg/execute! conn 
                 (hsql/format cmd))))

(defn class-id->prefix [c-id]
  (cond
    (= 5 (count c-id)) [:classroom/ac "ac"]
    (> (count c-id) 2) [:classroom/franchise (subs c-id 0 2)]
    :else [:classroom/ghost "gh"]))

(defn compact [record]
  (into {} (remove (comp nil? second) record)))

(defn add-columns
  "Handle one student datum"
  [conn src-table datum]
  (let [k (keyword src-table "center_symbol")
        center_symbol (k datum) 
        [tag prefix]  (class-id->prefix center_symbol)
        [{:keys [nextval]}] (latest-seq-number conn)
        serial (str prefix (pprint/cl-format nil "~8,'0d" (inc nextval)))
        tx* (assoc datum :student/serial serial
                   :student/classroom-type (str tag))
        tx** (compact tx*)]
    tx**))

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

(comment 
  (pg/execute! conn ["select * from ops_student;"]))

(def cli-options
  ;; An option with a required argument
  [(comment
    ["-p" "--port PORT" "Port number"
     :default 80
     :parse-fn #(Integer/parseInt %)
     :validate [#(< 0 % 0x10000) "Must be a number between 0 and 65536"]])
   ["-s" "--src-table SRC_TABLE" "The students to be inserted src table"
    :default "pre_ops_student_insert"]
   ["-h" "--help"]
   ["-c" "--create"]
   ["-u" "--update"]
   ["-d" "--debug"]])

;;  ./bin/postgres/crud.bb.clj -h -d
;; {:src-table "pre_ops_student_insert", :help true, :debug true}
 
(def input 
  (:options (parse-opts *command-line-args* cli-options)))

(defn main [args]
  (cond 
    (:create args) (create-cmd (:src-table args) (:debug args)) 
;;  (:update args) (update-cmd (:src-table args)) 
    :else (prn "no arguments recognized")))

(main input)

(comment 
  (main {:src-table "pre_ops_student_insert" :create true}))

;; crud.clj -d
;; crud.clj --create --src-table pre_ops_student_insert -d
;; crud.clj --update --src-table pre_ops_student_modify
