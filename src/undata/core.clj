(ns undata.core
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv]))

(def data-files
  {"idealpoints.tab" "https://dataverse.harvard.edu/api/access/datafile/2699454?format=tab"
   "rawvotingdata13.tab" "https://dataverse.harvard.edu/api/access/datafile/2699456?format=tab"
   "descriptions.xls" "https://dataverse.harvard.edu/api/access/datafile/2696465"})

(def vote-codes
  {1 :yes
   2 :abstain
   3 :no
   8 :absent
   9 :non-member})

(defn download [uri file]
  (io/make-parents (io/as-file file))
  (with-open [in (io/input-stream (io/as-url uri))
              out (io/output-stream (io/as-file file))]
     (io/copy in out)))

(defn download-data-files
  "Download data files specified in map."
  []  
  (doseq [[filename url] data-files]
    (download url (str "data/" filename))))

(defn read-tld
  [file]
  (let [in-file (io/reader file)]
    (csv/parse-csv in-file :delimiter \tab)))

(defn read-tld-with-header
  [file]
  (let [raw-data (read-tld file)
        header (first raw-data)
        data (rest raw-data)]
    (map #(zipmap header %) data)))

(defn country-codes
  []
  (let [data (read-tld-with-header "data/idealpoints.tab")]
    (into (sorted-map)
          (map #(let [{code "ccode"
                       country-name "CountryName"
                       abbreviation "CountryAbb"} %]
                  [(Integer/parseInt code) [country-name abbreviation]]) data))))

(defn parse-code-number
  [x]
  (int (Double/parseDouble x)))

(defn raw-rc-to-data
  [row ccodes]
  (let [{rcid "rcid"
         session "session"
         vote "vote"
         ccode "ccode"} row]
    {:resolution [(parse-code-number session) (parse-code-number rcid)]
     :country (first (get ccodes (parse-code-number ccode)))
     :vote (get vote-codes (parse-code-number vote))}))

(defn roll-call
  []
  (read-tld-with-header "data/rawvotingdata13.tab"))
