"""Hot and cold data (be-protocol chapter 15; the assembly project's protocol 11): the owner's unit catalog, the classes
a component declares, candidates and sealing behind a write barrier, the transfer to a route (the warm rows in the same
database, or Parquet with a manifest on the object store) under the claim's generation and epoch, the release of the
online rows, reads by catalog snapshot, holds, destruction and erasure. be-sdk-go's tiering package is the same
protocol; the files below have the same names and duties.

A component declares its classes in its definition (Definition.tiering); the host builds the member's catalog
(member.new_catalog: the deployment's residence, retention and route, the cold store, the columns checked against the
tables at start), runs the SDK's jobs, registers the recovery check before the component's own and the standard routes
after the module's; the module reaches it as rt.tiering, registers its blockers by implementing
tiering(registry), and its readers with history.register. The migrate entry installs the write barrier.

The files:

- classes.py: a class's declaration and its check (P15.1-P15.5); children may nest (Child.parent), a leaf that is no
  one's parent may have a key of several columns (Table.key).
- rows.py: the canonical row of every column type and a unit's digest (P15.8, P15.9); from_canonical turns a canonical
  row back into the driver's values a copy writes.
- plan.py: the plan, a pure function from a unit and the time to its action and what holds it back (P15.37).
- manifest.py: the manifest and the objects' keys (P15.16).
- barrier.py: the write barrier's trigger (P15.13), its search_path pinned to the one it was created under.
- catalog.py: groupings and sealing (P15.6, P15.11, P15.12); the Catalog, its options and its crash-pointed
  background transactions (Catalog.tx); CPU work goes through Catalog.compute, off the event loop.
- member.py: the configuration keys, the catalog's construction, the columns check and the candidates job.
- transfer.py: claim, copy, verify, publish (P15.14-P15.18); the orphans' cleanup and its daily job (P15.20).
  Passthrough: the exceptions a transaction body raises as themselves (StaleClaim, Verification).
- release.py: the release of a published unit's online rows (P15.19), the latest backup read before its transaction.
- reads.py: reads by key, the snapshot and its lease, the cold units of a list, thawing (P15.22-P15.27).
- history.py: the history read with its limit, and the asynchronous export (P15.26, P15.38); a component registers a
  Reader, whose online part decides each row with Subject.decider.
- holds.py: holds and destruction approvals (P15.28, P15.29).
- destroy.py: destruction and its replay from the evidence (P15.29, P15.30).
- erase.py: erasure (P15.31): a sealed unit's online rows and digest in one transaction, a published or released unit
  written again as a new manifest version; every erasure voids the class's exports (void_exports); a unit that moved
  on under it is erased again from its state now.
- reroute.py: writing a unit again: the route change (P15.35), the old versions' release (P15.39), the older
  structures' converters and the readable check (P15.33, P15.34).
- check.py: the recovery check be.tiering (P15.36).
- routes.py: the standard routes of holds, approvals and exports' status (P15.40).
- store.py: the cold store, one bucket and three credentials (write, read, delete); parquet.py: the Parquet files
  (pyarrow, the optional extra be-sdk[tiering]).
"""
