# Account deletion (staging)

## Data-deletion matrix

Account deletion is performed by `delete_user_account` in a database transaction. Unless noted
below, Django's relationship collector removes the records by `CASCADE`.

| Data/model | Result | Explicit work / retention |
| --- | --- | --- |
| `Profile` and onboarding/privacy preferences | Deleted | `CASCADE`; the avatar object is deleted from its configured storage after commit. |
| `Post`, `Rating` and post image | Deleted | `CASCADE`; post images are deleted from storage after commit. |
| `MovieRating` | Deleted | `CASCADE`; current averages/counts/display rating and following statistics are query annotations, so the next query immediately excludes it. |
| `Comment`, directed comment (`target_user`), and `CommentReaction` | Authored UGC and reactions deleted | Authored comments/reactions use `CASCADE`; a deleted directed-message recipient becomes `NULL`. Reaction counters are query annotations. |
| `VideoComment`, its reactions and video object | Deleted | `CASCADE`; video objects are deleted from storage after commit. Reaction counters are query annotations. |
| `Follow`, `Friendship`, visibility blocks | Deleted | Every relationship side uses `CASCADE`; follower/friend counts are queried live. |
| Lists, recommendations, favorites, daily feed pools | Deleted | `CASCADE`, including dependent candidates. |
| `UserTasteProfile` and genre/type/director preferences | Deleted | `CASCADE`; no global identifiable taste aggregate exists. |
| Notifications | Deleted | Recipient and actor relationships use `CASCADE`; contextual notifications also cascade with their UGC. |
| DRF auth token and pending email changes | Deleted | `CASCADE`. |
| Contact/support messages | Deleted | Explicit deletion is required because ordinary deletion is protected by `PROTECT`. |
| Content reports | Deleted when the user is reporter/reported | Current schema uses `CASCADE`; no moderation-retention exception is claimed. Reviews performed by the user retain no identity (`SET_NULL`). |
| Movie catalog entries imported by the user | Retained, anonymized | Shared catalog data is not user UGC; `Movie.author` becomes `NULL` via `SET_NULL`. |
| Weekly recommendation snapshots/items | Retained | Historical, aggregate-only records have no user identifier and are not the source of live displayed ratings. |
| Used public-deletion credential | Token hash, timestamps only | The user link becomes `NULL`; this minimized, non-identifying record prevents reuse. Expired unused credentials likewise become unlinked on deletion. |

The service discovers avatar, post-image and video storage/name pairs before database deletion and
schedules idempotent `storage.delete(name)` calls with `transaction.on_commit`. Thus a database
rollback never removes media, the configured filesystem/R2 abstraction is used, shared catalog
artwork is untouched, and a missing/unavailable object is logged without turning successful account
deletion into an HTTP 500.

## Derived data

Movie rating averages/counts, display ratings, following-rating statistics, comment/video reaction
counts, and follower/friend counts are computed from current rows through query annotations. They
therefore require no cache rebuild and behave as though deleted activity never participated. Taste
profiles and per-user feed/recommendation material are deleted with the user. Weekly recommendation
snapshots are immutable historical aggregates, contain no user identifier, and are not used as the
live movie rating source.

## Public credentials

Public deletion credentials are generated with `secrets.token_urlsafe(32)`, only their SHA-256 hash
is stored, and they expire after 24 hours. Confirmation is a one-use POST. The email link targets
`FRONTEND_BASE_URL/delete-account/confirm/<token>`; the frontend calls the backend confirmation API.
