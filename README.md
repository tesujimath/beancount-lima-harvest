# beancount-lima-harvest

This is a new importer and framework for [Beancount](https://github.com/beancount/beancount) using [Rust](https://rust-lang.org) and [Clojure](https://clojure.org/) and the [Lima parser](https://github.com/tesujimath/beancount-parser-lima).

## Design Principles

The import process is governed by configuration in [EDN](https://github.com/edn-format/edn), as in [this example](test-cases/harvest.edn).
There are two phases.

### Phase 1 - Hulling

Phase 1 import unwraps the container (OFX, CSV, whatever) into generic JSON, using `hull-ofx`, `hull-csv`, et el.

Differences between instituions is handled by Phase 2 configuration, with minimal (but non-zero) use of custom code.

### Phase 2 - Contextual import

Phase 2 uses a digest of a Beancount file for context, which enables:

- mapping of external account ID to Beancount account name via metadata in `open` directives
- secondary account inference from payee/narration of previous transactions
- merging of the same transaction from both ends when importing both accounts together
- detection of duplicate imports from existing transaction IDs

### Caveats

[beancount_reds_importers](https://reds-rants.netlify.app/personal-finance/make-importers-easy-to-write-and-write-lots-of-them/) makes a strong case for code-as-config.  It is possible that the `lima-harvest` approach of mostly declarative configuration with minimal custom code may be insufficient for more complex needs.  In which case, go and enjoy `beancount_reds_importers`. 😊

## Usage

`lima-harvest` provides the following features:
- import from CSV or OFX v1 into Beancount format
- lookup primary account for import from OFX `acctid` field
- infer secondary accounts for postings from payees and narrations in existing Beancount file
- construct transaction ID from OFX `acctid` anf `fitid` fields, and reject import of duplicate transactions
- pair up transactions between accounts where both accounts are imported in the same group

The intention is that OFX v1 import is complete and general purpose.

CSV import, however, requires customising for each financial instituion according to the headers they export in CSV.

### Transaction IDs

A transaction ID is allocated to each transaction by the OFX v1 importer, and this is used to avoid re-importing the same transactions subsequently.  The ID is written to the metadata value `txnid`, (the key is configurable).

The transaction ID comprises `acctid.fitid`.

### Secondary account inference

Where there are matches with payees or narrations in the Beancount file providing context, secondary accounts may be inferred.

If any payees are found to match then narrations are ignored, otherwise narrations are used as a fallback.

In case of multiple matches, all are included, along with a count of match occurences.

### Transaction pairing

When money is moved between accounts, the import file for each account contains a record of the transaction, which leads to a duplicate transaction traditionally requiring manual elimination.

`lima-harvest` has heuristics to pair up transactions which are imported in the same group, removing the need for this manual step.

This requires secondary account inference to have allocated a unique candidate account.  (Multiple matches during secondary account inference precludes transaction pairing.)

Pairing is performed only where the source and destination accounts and the value match, and the date is within some configurable threshold (default 3 days).
The result is a single transaction with both `txnid` and `txnid2` metadata values, or a comment in the case of import files missing transaction IDs. The payee and narration from the second transaction are also preserved as `payee2` and `narration2` metadata fields.  These fields are used for account inference in subsequent imports.

## Diagnostics for mismatched balances

A bother when importing transactions is finding that the asserted balance is different from the sum of the postings.

To facilitate resolving such problems, when a balance mismatch is detected, all posts to that account since the
most recent balance assertion are printed along with the error message.

For example:
```
aya> lima --ledger examples/beancount/balance.beancount
Error: invalid balance
    ╭─[ examples/beancount/balance.beancount:27:1 ]
 27 │ 2020-02-28 balance Assets:Bank:Current   940.00 NZD
    │ ─────────────────────────┬─────
    │                                           ╰───── accumulated 935.00 NZD, error 5.00 NZD
──╯
2020-01-28              1000.00 NZD
2020-02-01  -50.00 NZD   950.00 NZD  Food
2020-02-02  -15.00 NZD   935.00 NZD  Drinks
```

## Alternatives

`lima-harvest` is very new.  For now, or perhaps forever, you may be better served by these alternatives:

- [beancount_reds_importers](https://github.com/redstreet/beancount_reds_importers)
- [beancount-import](https://github.com/jbms/beancount-import)

## License

Licensed under either of

 * Apache License, Version 2.0
   [LICENSE-APACHE](http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   [LICENSE-MIT](http://opensource.org/licenses/MIT)

at your option.
