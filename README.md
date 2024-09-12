## Commands for Running Files

- **Build the code**: `make`
- **Run test cases**: `./test_assign4_1`
- **Run test cases**: `./test_expr`

## Interface Functions

### Index Manager Functions

1. **initIndexManager(void *mgmtData)**:
   - Initializes the index manager.
   - Command for running: No specific command needed. Called internally during initialization.

2. **shutdownIndexManager()**:
   - Shuts down the index manager, freeing up resources.
   - Command for running: No specific command needed. Called internally during shutdown.

### B+-tree Functions

3. **createBtree(char *idxId, DataType keyType, int n)**:
   - Creates a B+-tree index with the specified ID, key type, and maximum number of elements per node.
   - Command for running: `./test_assign4_1`

4. **openBtree(BTreeHandle **tree, char *idxId)**:
   - Opens an existing B+-tree index with the specified ID.
   - Command for running: `./test_assign4_1`

5. **closeBtree(BTreeHandle *tree)**:
   - Closes the specified B+-tree index, flushing any modified pages to disk.
   - Command for running: No specific command needed. Called internally during closing.

6. **deleteBtree(char *idxId)**:
   - Deletes the B+-tree index with the specified ID, including its corresponding page file.
   - Command for running: `./test_assign4_1`

### Access Information Functions

7. **getNumNodes(BTreeHandle *tree, int *result)**:
   - Retrieves the number of nodes in the specified B-tree.
   - Command for running: No specific command needed. Called internally during testing.

8. **getNumEntries(BTreeHandle *tree, int *result)**:
   - Retrieves the number of entries in the specified B-tree.
   - Command for running: No specific command needed. Called internally during testing.

9. **getKeyType(BTreeHandle *tree, DataType *result)**:
   - Retrieves the data type of keys in the specified B-tree.
   - Command for running: No specific command needed. Called internally during testing.

### Index Access Functions

10. **findKey(BTreeHandle *tree, Value *key, RID *result)**:
    - Finds the record identifier (RID) corresponding to the given key in the B-tree.
    - Command for running: No specific command needed. Called internally during testing.

11. **insertKey(BTreeHandle *tree, Value *key, RID rid)**:
    - Inserts a new key and record identifier pair into the B-tree.
    - Command for running: No specific command needed. Called internally during testing.

12. **deleteKey(BTreeHandle *tree, Value *key)**:
    - Deletes the key and its corresponding record identifier from the B-tree.
    - Command for running: No specific command needed. Called internally during testing.

13. **openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle)**:
    - Opens a scan on the specified B-tree, allowing traversal through its entries.
    - Command for running: No specific command needed. Called internally during testing.

14. **nextEntry(BT_ScanHandle *handle, RID *result)**:
    - Retrieves the next entry in the scan.
    - Command for running: No specific command needed. Called internally during testing.

15. **closeTreeScan(BT_ScanHandle *handle)**:
    - Closes the scan on the B-tree.
    - Command for running: No specific command needed. Called internally during testing.

### Debug and Test Functions

16. **printTree(BTreeHandle *tree)**:
    - Generates a string representation of the B-tree for debugging purposes.
    - Command for running: No specific command needed. Called internally during testing.


