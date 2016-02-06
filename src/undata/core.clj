(ns undata.core
  (:import [org.apache.poi.ss.usermodel Cell])
  (:require [clojure.java.io :as io]
            [clojure.walk :as walk]
            [clojure-csv.core :as csv]
            [dk.ative.docjure.spreadsheet :as spreadsheet]))

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

(defn download
  "Download the resource at uri and save to a file."
  [uri file]
  (io/make-parents (io/as-file file))
  (with-open [in (io/input-stream (io/as-url uri))
              out (io/output-stream (io/as-file file))]
     (io/copy in out)))

(defn download-data-files
  "Download data files specified in map."
  []  
  (doseq [[filename url] data-files]
    (download url (str "data/" filename))))

(defn read-tabbed-data
  "Read tab-delimited data from a file."
  [file]
  (let [in-file (io/reader file)]
    (csv/parse-csv in-file :delimiter \tab)))

(defn read-tabbed-data-with-header
  "Read tab-delimited data from a file,
   returning a sequence of maps, one for each
   row, mapping column name to corresponding
   value in the row."
  [file]
  (let [raw-data (read-tabbed-data file)
        header (first raw-data)
        data (rest raw-data)]
    (map #(zipmap header %) data)))

(defn country-codes
  "Extract the integer to country-code mapping in the data set."
  []
  (let [data (read-tabbed-data-with-header "data/idealpoints.tab")]
    (into (sorted-map)
          (map #(let [{code "ccode"
                       country-name "CountryName"
                       abbreviation "CountryAbb"} %]
                  [(Integer/parseInt code) [country-name abbreviation]]) data))))

(defn fmap
  "Apply a function to each value in a map."
  [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn parse-code-number
  "Parse a code number. The codes in the data
   set are integers, but are inexplicably formatted
   as a float, like '17.0'. Returns an int."
  [x]
  (int (Double/parseDouble x)))

(defn raw-rc-to-data
  "Converts a data map from the raw roll-call data, e.g.,
   {:rcid 22 :session 5 :ccode 2 :vote 1}
   to a map that looks like
   {:resolution [5 22], :country USA, :vote :yes}."
  [row ccodes]
  (let [{:keys [rcid session vote ccode]} row]
    {:resolution [session rcid]
     :country (keyword (or (second (get ccodes ccode)) (str ccode)))
     :vote (get vote-codes vote)}))

(defn raw-roll-call-data
  "Reads raw roll-call data from the file."
  []
  (->>
   (read-tabbed-data-with-header "data/rawvotingdata13.tab")
   (map walk/keywordize-keys)
   (map #(fmap parse-code-number %))))

(defn merge-votes
  "Takes a series of roll call data like
  ({:resolution [5 22], :country USA, :vote :yes}
   {:resolution [5 22], :country UK, :vote :abstain}...)
  and merges to a single map that looks like
   {:USA :yes, :UK :abstain, ...}."
  [vote-maps]
  (into {}
        (for [{:keys [country vote]} vote-maps]
          [country vote])))

(defn roll-call
  "Reads the raw roll-call data from the data file,
   and converts to a sorted sequence of pairs, containing
     [resolution vote-map]
   so the sequence looks somthing like:
     ([[5 22] {:USA :yes, :RUS :abstain, ...}]
      [[5 23] {:USA :yes, :RUS :no, ...}]
      ...)"
  []
  (let [ccodes (country-codes)
        raw-data (raw-roll-call-data)]
    (->> (raw-roll-call-data)
         (map #(raw-rc-to-data % ccodes))
         (group-by :resolution)
         (fmap merge-votes)
         sort)))

(defn country-headers
  "Takes the data produced by (roll-call) and
   works out a unified sorted list of country names
   to be used as headers for the output file."
  [roll-call-data]
  (->> (vals roll-call-data)
       (map keys) (map set) (apply clojure.set/union)
       seq sort))

(defn roll-call-rows
  "Generates rows for the roll-call output file. Takes roll-call-data
   and country-header names and produces rows where the first two
   items are session, rcid, and the rest are vote codes, corresponding
   to countries in the provided country-header-names. Something like
   [5 22 1 9 3 4 ...]"
  [roll-call-data country-header-names]
  (let [vote-to-code (clojure.set/map-invert vote-codes)]
    (for [[[session rcid] vote-map] roll-call-data]
      (map str
           (concat [session rcid]
                   (for [heading country-header-names]
                     (->> heading
                          (get vote-map)
                          (get vote-to-code))))))))

(defn merge-roll-calls
  "Reads roll call data and country-header names and produces
   a file with a header like
   [\"session\", \"rcid\", \"USA\", \"RUS\" ...]
   and rows with integers corresponding to session, rcid, and
   vote codes."
  []
  (let [roll-call-data (roll-call)
        country-header-names (country-headers roll-call-data)
        rows (roll-call-rows roll-call-data country-header-names)
        header (concat ["session" "rcid"] (map name country-header-names))
        full-data (cons header rows)]
    (spit "roll-calls.tab" (csv/write-csv full-data :delimiter \tab))))

(defn cells-in-row [row]
  (take-while identity
              (map #(.getCell row %) (range))))

(defn rows-in-sheet [sheet]
  (take-while identity
              (map #(.getRow sheet %) (range))))

(defn read-excel-sheet [filename sheetname]
  (->> (spreadsheet/load-workbook filename)
       (spreadsheet/select-sheet sheetname)
       (rows-in-sheet)
       (map cells-in-row)
       (map #(map spreadsheet/read-cell %))))
       
