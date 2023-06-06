# shrinkbolt

### shrink bolt (bbolt) database

entire database is not copied to RAM at once, so this may be suitable for extra large bolt database files.

**untested, use at your own risk.**

### Usage

```go
    // this is experimental
    shrinkbolt.DangerZone = true

    // "some.db" will be processed into a new file named "some.shrunken.db"
    err := shrinkbolt.ShrinkBoltDatabase("some.db","some.shrunken.db")
    if err != nil {
        // do something
    }
```
