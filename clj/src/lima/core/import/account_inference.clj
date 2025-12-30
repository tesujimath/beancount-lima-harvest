(ns lima.core.import.account-inference)

(defn make-infer-secondary-accounts-from-payees-and-narrations
  [payees narrations]
  (fn [txn]
    (let [amount (amount-number (hash-get txn 'amount))
          primary-account (hash-get txn 'primary-account)
          found-payee (hash-try-get payees (or (hash-try-get txn 'payee) '()))
          found-narration (hash-try-get narrations (or (hash-try-get txn 'narration) '()))
          order-accounts (fn [account-lookup category]
                           (let [all-account-names (hash-keys->list account-lookup)
                                 candidate-account-names (filter (fn [ account-name] (not (equal? account-name primary-account))) all-account-names)
                                 annotated-accounts (map (fn [ account-name]
                                                                 (hash 'name account-name
                                                                       'infer-count (hash-get account-lookup account-name)
                                                                       'infer-category category))
                                                         candidate-account-names)]
                                   (merge-sort annotated-accounts
                                               #:comparator
                                               ;; by infer-count descending, then by name ascending
                                               (fn [ acc0 acc1] (cond [(> (hash-get acc0 'infer-count)
                                                                             (hash-get acc1 'infer-count))
                                                                          #t]
                                                                         [(< (hash-get acc0 'infer-count)
                                                                             (hash-get acc1 'infer-count))
                                                                          #f]
                                                                         [else (string<? (hash-get acc0 'name)
                                                                                         (hash-get acc1 'name))])))))
          secondary-accounts
          (let* (

                 )
            (cond
              [found-payee (order-accounts found-payee "payee")]
              [found-narration (order-accounts found-narration "narration")]
              [(decimal>? amount (decimal-zero)) (list (hash 'name "Income:Unknown"))]
              [(decimal<? amount (decimal-zero)) (list (hash 'name "Expenses:Unknown"))]
              [else '()]))]
      (hash-insert txn 'secondary-accounts secondary-accounts))))
