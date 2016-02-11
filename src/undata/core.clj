(ns ^{:author "Arthur Edelstein"
      :doc "Retrieve and merge UNGA Resolution data."}
    undata.core
  (:require [clojure.java.io :as io]
            [clojure.walk :as walk]
            [clojure-csv.core :as csv]
            [dk.ative.docjure.spreadsheet :as spreadsheet]))

(def data-files
  "Data files from Erik Voeten.
   See
   Erik Voeten. \"Data and Analyses of Voting in the UN General Assembly\", Routledge Handbook of International Organization, edited by Bob Reinalda (published May 27, 2013). Available at SSRN: http://ssrn.com/abstract=2111149"
  {"idealpoints.tab" "https://dataverse.harvard.edu/api/access/datafile/2699454?format=tab"
   "rawvotingdata13.tab" "https://dataverse.harvard.edu/api/access/datafile/2699456?format=tab"
   "descriptions.xls" "https://dataverse.harvard.edu/api/access/datafile/2696465"})

(def vote-codes
  "Map from numerical vote codes to
   human-readable keywords."
  {1 :yes
   2 :abstain
   3 :no
   8 :absent
   9 :non-member})

(defn fmap
  "Apply a function to each value in a map."
  ([f m]
   (into {} (for [[k v] m] [k (f v)])))
  ([f m key-pred]
   (into {} (for [[k v] m] [k (if (key-pred k) (f v) v)]))))

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

(defn gridded-data-to-maps
  "Takes a sequence of rows of data,
   returning a sequence of maps, one for each
   row, mapping column name to corresponding
   value in the row."
  [raw-gridded-data]
  (let [header (first raw-gridded-data)
        data (rest raw-gridded-data)]
    (map #(zipmap header %) data)))

(defn cells-in-row
  "Read all cells in a row, from the leftmost column out to
   and including the last occupied column."
  [row]
  (map #(.getCell row %) (range (.getLastCellNum row))))

(defn rows-in-sheet
  "Read all rows in a sheet, from the topmost row down to
   and including the last occupied row."
  [sheet]
  (map #(.getRow sheet %) (range (inc (.getLastRowNum sheet)))))

(defn read-excel-sheet
  "Read in an Excel sheet from a workbook. Convert to a sequence
   of vectors containing the gridded data."
  [filename sheetname]
  (->> (spreadsheet/load-workbook filename)
       (spreadsheet/select-sheet sheetname)
       (rows-in-sheet)
       (map cells-in-row)
       (map (fn [row-cells]
              (map (fn [cell]
                     (when cell
                       (spreadsheet/read-cell cell)))
                   row-cells)))))

(defn read-descriptions
  "Read the descriptions file and convert to a sequence of
   row-maps, using the first row (the header) for keywords."
  []
  (->> (read-excel-sheet "data/descriptions.xls" "descriptions")
       gridded-data-to-maps
       (map walk/keywordize-keys)
       (map #(fmap int % #{:session :rcid :yes :no :abstain}))))

(defn map-descriptions
  "Read description data and produce a map of
   [session rcid] to row-map."
  [description-data]
  (->> description-data
       (group-by #(vector (int (% :session)) (int (% :rcid))))
       (fmap first)))

(defn read-tabbed-data
  "Read tab-delimited data from a file."
  [file]
  (let [in-file (io/reader file)]
    (csv/parse-csv in-file :delimiter \tab)))

(defn country-codes
  "Extract the integer to country-code mapping in the data set."
  []
  (let [data (gridded-data-to-maps (read-tabbed-data "data/idealpoints.tab"))]
    (into (sorted-map)
          (map #(let [{code "ccode"
                       country-name "CountryName"
                       abbreviation "CountryAbb"} %]
                  [(Integer/parseInt code) [country-name abbreviation]]) data))))

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
   (read-tabbed-data "data/rawvotingdata13.tab")
   gridded-data-to-maps
   (map walk/keywordize-keys)
   (map #(fmap parse-code-number %))))

(defn merge-votes
  "Takes a series of roll call data like
  ({:country USA, :vote :yes}
   {:country UK, :vote :abstain}...)
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
         sort
         (map #(zipmap [:resolution :votes] %))
         )))

(defn annotate-roll-call
  "Add description data to roll-call data."
  [roll-call-data]
  (let [description-maps (map-descriptions (read-descriptions))
        vote-to-code (clojure.set/map-invert vote-codes)]
    (for [roll-call-entry roll-call-data]
      (let [description-small (-> roll-call-entry :resolution description-maps
                                  (select-keys [:unres :yes :no :abstain]))]
        (merge roll-call-entry description-small)))))

(defn cross-check-votes
  "Confirms that a roll call entry has consistent data."
  [roll-call-entry]
  (let [raw-tally (merge {:yes 0 :no 0 :abstain 0}
                         (-> roll-call-entry :votes vals frequencies
                             (select-keys [:yes :no :abstain])))
        description-tally (-> roll-call-entry (select-keys [:yes :no :abstain]))]
    ;(println (:resolution roll-call-entry) raw-tally description-tally (merge-with - raw-tally description-tally))
    (= raw-tally description-tally)))

(defn country-headers
  "Takes the data produced by (roll-call) and
   works out a unified sorted list of country names
   to be used as headers for the output file."
  [roll-call-data]
  (->> roll-call-data (map :votes) (map keys) (map set)
       (apply clojure.set/union) seq sort))

(defn roll-call-rows
  "Generates rows for the roll-call output file. Takes roll-call-data
   and country-header names and produces rows where the first two
   items are session, rcid, unres and the rest are vote codes, corresponding
   to countries in the provided country-header-names. Something like
   [5 22 R/44/120 1 9 3 4 ...]"
  [roll-call-data country-header-names]
  (let [vote-to-code (clojure.set/map-invert vote-codes)]
    (for [roll-call-entry roll-call-data]
      (let [{:keys [resolution unres votes]} roll-call-entry
            [session rcid] resolution]
        (map str
             (concat [session rcid unres]
                     (for [heading country-header-names]
                       (->> heading
                            (get votes)
                            (get vote-to-code)))))))))

(defn merge-roll-calls
  "Reads roll call data and country-header names and produces
   a file with a header like
   [\"session\", \"rcid\", \"unres\", \"USA\", \"RUS\" ...]
   and rows with integers corresponding to session, rcid, and
   vote codes."
  []
  (let [roll-call-data (annotate-roll-call (roll-call))
        country-header-names (country-headers roll-call-data)
        rows (roll-call-rows roll-call-data country-header-names)
        header (concat ["session" "rcid" "unres"] (map name country-header-names))
        full-data (cons header rows)]
    (spit "roll-calls.tab" (csv/write-csv full-data :delimiter \tab))))
