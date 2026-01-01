(ns lima.harvest.core.format
  (:require [clojure.string :as str]))

(defn escape-string [s] (str/escape s {\" "\\\"", \\ "\\\\"}))

(defn payee-narration
  "Format payee/narration, including sep prefix if anything to format"
  [sep txn]
  (let [p (:payee txn)
        n (:narration txn)]
    (cond (and p n)
            (format "%s\"%s\" \"%s\"" sep (escape-string p) (escape-string n))
          (and (nil? p) (nil? n)) ""
          p (format "%s\"%s\" \"\"" sep (escape-string p))
          n (format "%s\"%s\"" sep (escape-string n)))))

(defn decimal->anchored-string
  "Convert decimal to string anchored at the units digit, so will align with e.g. integers"
  [d]
  (let [s (str d) dp (or (str/index-of s ".") (count s))] [s (dec dp)]))

(defn post-acc
  "Format a post for the primary acc, with unit digit of units at UNIT-COLUMN"
  [acc units cur]
  (let [indent-acc (format "%s%s" INDENT acc)
        width (count indent-acc)
        [u-str u-anchor] (decimal->anchored-string units)
        n-pad (max 1 (- UNIT-COLUMN (+ width u-anchor 1)))
        pad (apply str (repeat n-pad " "))]
    (format "%s%s%s %s\n" indent-acc pad u-str cur)))

(defn post-acc2
  "Format a post for a secondary acc, with comment if any at COMMENT-COLUMN"
  [acc2]
  (let [indent-acc (format "%s%s" INDENT (:name acc2))
        infer-count (:infer-count acc2)
        infer-category (:infer-category acc2)]
    (if (and infer-count infer-category)
      (let [width (count indent-acc)
            n-pad (max 1 (- COMMENT-COLUMN (+ width 1)))
            pad (apply str (repeat n-pad " "))
            plural (if (> infer-count 1) "s" "")]
        (format "%s%s; inferred from %d %s%s\n"
                indent-acc
                pad
                infer-count
                infer-category
                plural))
      (format "%s\n" indent-acc))))

(defn transaction
  "format a transaction"
  [txn]
  (format "%tF txn%s\n%s%s%s%s%s\n"
          (:date txn)
          (payee-narration " " txn)
          (if-let [txnid (:txnid txn)]
            (format "%s%s: \"%s\"\n" INDENT TXNID_KEY txnid)
            "")
          (if-let [txnid2 (:txnid2 txn)]
            (format "%s%s: \"%s\"\n" INDENT TXNID2_KEY txnid2)
            "")
          (if-let [payee2 (:payee2 txn)]
            (format "%s%s: \"%s\"\n" INDENT PAYEE2_KEY payee2)
            "")
          (post-acc (:acc txn) (:units txn) (:cur txn))
          (str/join (map post-acc2 (:acc2 txn)))))

;; TODO get these from config:
(def INDENT "  ")
(def UNIT-COLUMN 76)
(def COMMENT-COLUMN 41)

;; but not these:
(def TXNID_KEY "txnid")
(def TXNID2_KEY "txnid2")
(def PAYEE2_KEY "payee2")
