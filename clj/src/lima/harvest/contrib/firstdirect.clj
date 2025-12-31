(ns lima.harvest.contrib.firstdirect
  (:require [clojure.string :as str]))

(defn payee-narration
  "Extract payee and narration from txn description and remove that.

  The description field is a composite of payee and narration with spaces between,
  so we attempt to split on two or more spaces, and if we can't just take it as narration.
  "
  [txn]
  (if-let [description (get txn :description)]
    (let [txn (dissoc txn :description)
          [s1 s2] (str/split description #"   *" 2)]
      (if s2 (merge txn {:payee s1, :narration s2}) (assoc txn :narration s1)))
    txn))
