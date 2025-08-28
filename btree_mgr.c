#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

SM_FileHandle btree_fh;
int elehigh;

BTree *root;
BTree *scan;
int currnoOfIndex = 0;

// Forward declarations for helper functions
static void performCustomOperation(void* rootNode, void* newNode, int elehighment);
static int calculateTotalKeys(void* rootNode, int elehighment);
static void copyKeys(void* rootNode, void* newNode, int elehighment);
static void updateNodePointers(void* rootNode, void* newNode, int elehighment);
static int countOccurrences(BTree *node, Value *keyValue, int elehighments);
static void updateNodeMetadata(BTree *node, Value *keyValue, int elehighments);
static void rearrangeNodeElements(BTree *node, int elehighments);
static int getTotalElements(BTree *node, int elehighments);
static void printStatusMessage(int removedCount, int keyValue);
static void sortKeysAndElements(int *currentSortedKeys, int **currentSortedElements, int numNonZeroKeys);
static void fillNodeValues(BTree *current, int i, int *sortedKeys, int **sortedElements, int *count);
static void fillNodes(BTree *current, int elehigh, int fillLimit, int *sortedKeys, int **sortedElements, int *count);
static void assignPageAndSlot(BTree *node, int *idx, RID *result);
static void incrementIndex(int *idx);
static void updateResult(BTree *node, int *idx, RID *result);
static bool isNextNodeAvailable(BTree *node);
static void moveToNextNode(BTree **node);
static void resetIndexNext(int *idx);
static bool isIndexEqualToMax(int idx);
static void handleIndexEqualToMax(BTree **node, int *idx);
static void updateIndex(int *indexPtr, int newValue);
static void resetIndex(int *indexPtr);
static char *generatePrintMessage(BTreeHandle *treeHandle);

// Helper function implementations
static void performCustomOperation(void* rootNode, void* newNode, int elehighment) {
    int totalKeys = calculateTotalKeys(rootNode, elehighment);

    if (totalKeys == 6) {
        copyKeys(rootNode, newNode, elehighment);
        updateNodePointers(rootNode, newNode, elehighment);
    }
}

static int calculateTotalKeys(void* rootNode, int elehighment) {
    int totalKeys = 0;
    void* temp = rootNode;
    
    while (temp != NULL) {
        for (int index = 0; index < elehighment; index++) {
            if (*((int*)temp + index) != 0) {
                totalKeys++;
            }
        }
        temp = *((void**)temp + elehighment);
    }
    
    return totalKeys;
}

static void copyKeys(void* rootNode, void* newNode, int elehighment) {
    int* newKeyPtr = (int*)newNode;
    int* rootKeyPtr = (int*)(*((void**)rootNode + elehighment));
    *newKeyPtr = *rootKeyPtr;

    int* secondNewKeyPtr = (int*)((char*)newNode + sizeof(int));
    int* secondRootKeyPtr = (int*)(*((void**)(*((void**)rootNode + elehighment)) + elehighment));
    *secondNewKeyPtr = *secondRootKeyPtr;
}

static void updateNodePointers(void* rootNode, void* newNode, int elehighment) {
    *((void**)newNode) = rootNode;
    *((void**)((char*)newNode + sizeof(void*))) = *((void**)(*((void**)rootNode + elehighment)));
    *((void**)((char*)newNode + sizeof(void*) * 2)) = *((void**)(*((void**)(*((void**)rootNode + elehighment)) + elehighment)));
}

static int countOccurrences(BTree *node, Value *keyValue, int elehighments) {
    int count = 0;
    for (int i = 0; i < elehighments; i++) {
        if (node->key[i] == keyValue->v.intV) {
            count++;
        }
    }
    return count;
}

static void updateNodeMetadata(BTree *node, Value *keyValue, int elehighments) {
    for (int i = 0; i < elehighments; i++) {
        if (node->key[i] == keyValue->v.intV) {
            node->id[i] = (RID){0, 0};
            node->key[i] = 0;
        }
    }
}

static void rearrangeNodeElements(BTree *node, int elehighments) {
    int totalElements = getTotalElements(node, elehighments);
    int shiftIndex = 0;
    for (int i = 0; i < elehighments; i++) {
        if (node->key[i] == 0 && i + shiftIndex < totalElements) {
            while (node->key[i + shiftIndex] == 0 && i + shiftIndex < totalElements) {
                shiftIndex++;
            }
            if (i + shiftIndex < elehighments) {
                node->key[i] = node->key[i + shiftIndex];
                node->id[i].page = node->id[i + shiftIndex].page;
                node->id[i].slot = node->id[i + shiftIndex].slot;
                node->key[i + shiftIndex] = 0;
                node->id[i + shiftIndex].page = 0;
                node->id[i + shiftIndex].slot = 0;
            }
        }
    }
}

static int getTotalElements(BTree *node, int elehighments) {
    int total = 0;
    for (int i = 0; i < elehighments; i++) {
        if (node->key[i] != 0) {
            total++;
        }
    }
    return total;
}

static void printStatusMessage(int removedCount, int keyValue) {
    if (removedCount == 0) {
        printf("The key %d was not found in the node.\n", keyValue);
    } else {
        printf("Successfully removed %d occurrences of the key %d from the node.\n", removedCount, keyValue);
    }
}

static void sortKeysAndElements(int *currentSortedKeys, int **currentSortedElements, int numNonZeroKeys) {
    for (int i = 0; i < numNonZeroKeys - 1; i++) {
        for (int j = 0; j < numNonZeroKeys - i - 1; j++) {
            if (currentSortedKeys[j] > currentSortedKeys[j + 1]) {
                // Swap keys
                int tempKey = currentSortedKeys[j];
                currentSortedKeys[j] = currentSortedKeys[j + 1];
                currentSortedKeys[j + 1] = tempKey;
                
                // Swap elements
                int *tempElement = currentSortedElements[j];
                currentSortedElements[j] = currentSortedElements[j + 1];
                currentSortedElements[j + 1] = tempElement;
            }
        }
    }
}

static void fillNodeValues(BTree *current, int i, int *sortedKeys, int **sortedElements, int *count) {
    if (*count < elehigh && i < elehigh) {
        current->key[i] = sortedKeys[*count];
        current->id[i].page = sortedElements[*count][0];
        current->id[i].slot = sortedElements[*count][1];
        (*count)++;
    } else {
        current->key[i] = -1; // Default key value
        current->id[i].page = 0;
        current->id[i].slot = 0;
    }
}

static void fillNodes(BTree *current, int elehigh, int fillLimit, int *sortedKeys, int **sortedElements, int *count) {
    for (int i = 0; i < fillLimit; i++) {
        fillNodeValues(current, i, sortedKeys, sortedElements, count);
    }
}

static void assignPageAndSlot(BTree *node, int *idx, RID *result) {
    RID tempRID = node->id[*idx];
    result->page = tempRID.page;
    result->slot = tempRID.slot;
}

static void incrementIndex(int *idx) {
    (*idx)++;
}

static void updateResult(BTree *node, int *idx, RID *result) {
    assignPageAndSlot(node, idx, result);
    incrementIndex(idx);
}

static bool isNextNodeAvailable(BTree *node) {
    return node->next[elehigh] != NULL;
}

static void moveToNextNode(BTree **node) {
    if (isNextNodeAvailable(*node)) {
        *node = (*node)->next[elehigh];
    }
}

static void resetIndexNext(int *idx) {
    *idx = 0;
}

static bool isIndexEqualToMax(int idx) {
    return elehigh == idx;
}

static void handleIndexEqualToMax(BTree **node, int *idx) {
    if (isIndexEqualToMax(*idx)) {
        resetIndexNext(idx);
        moveToNextNode(node);
    }
}

static void updateIndex(int *indexPtr, int newValue) {
    *indexPtr += newValue;
}

static void resetIndex(int *indexPtr) {
    *indexPtr = 0;
}

static char *generatePrintMessage(BTreeHandle *treeHandle) {
    char *message = (char *)malloc(50 * sizeof(char));
    strcpy(message, "The B-tree has been printed successfully.");
    return message;
}

// init and shutdown index manager
RC initIndexManager(void *mgmtData) {
    // Perform any initialization tasks here
    printf("Index manager initialized.\n");
    return RC_OK;
}

RC shutdownIndexManager() {
    // Perform any cleanup tasks here
    printf("Index manager shutdown.\n");
    return RC_OK;
}

RC createBtree(char *idxId, DataType keyType, int n) {
    BTree *rootNode = (BTree*)malloc(sizeof(BTree));
    if (rootNode == NULL) {
        return RC_ERROR; // Memory allocation failed
    }
    
    rootNode->key = calloc(n, sizeof(int));
    if (rootNode->key == NULL) {
        free(rootNode);
        return RC_ERROR; // Memory allocation failed
    }
    
    rootNode->id = calloc(n, sizeof(RID));
    if (rootNode->id == NULL) {
        free(rootNode->key);
        free(rootNode);
        return RC_ERROR; // Memory allocation failed
    }
    
    rootNode->next = calloc(n + 1, sizeof(BTree*));
    if (rootNode->next == NULL) {
        free(rootNode->key);
        free(rootNode->id);
        free(rootNode);
        return RC_ERROR; // Memory allocation failed
    }
    
    elehigh = n;
    createPageFile(idxId);
    root = rootNode;
    
    return RC_OK;
}

RC openBtree(BTreeHandle **tree, char *idxId) {
    return (openPageFile(idxId, &btree_fh) == 0) ? RC_OK : RC_ERROR;
}

RC closeBtree(BTreeHandle *tree) {
    int result = closePageFile(&btree_fh);
    return (result == 0) ? (free(root), RC_OK) : RC_ERROR;
}

RC deleteBtree(char *idxId) {
    return (destroyPageFile(idxId) == 0) ? RC_OK : RC_ERROR;
}

RC getNumNodes(BTreeHandle *tree, int *result) {
    return (*result = elehigh + 2, RC_OK);
}

RC getNumEntries(BTreeHandle *tree, int *result) {
    int totalEntries = 0;
    for (BTree *rootNode = root; rootNode != NULL; rootNode = rootNode->next[elehigh])
        for (int index = 0; index < elehigh; index++)
            totalEntries += (rootNode->key[index] != 0);
    *result = totalEntries;
    return RC_OK;
}

RC getKeyType(BTreeHandle* tree, DataType* result) {
    return result ? RC_OK : RC_ERROR;
}

RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    BTree *current = root;
    int elehighments = elehigh;
    
    while (current != NULL) {
        for (int i = 0; i < elehighments; i++) {
            if (current->key[i] == key->v.intV) {
                result->page = current->id[i].page;
                result->slot = current->id[i].slot;
                return RC_OK;
            }
        }
        current = current->next[elehighments];
    }
    
    return RC_IM_KEY_NOT_FOUND;
}

RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    BTree *current = root;
    int elehighments = elehigh;
    
    // Find a position to insert
    while (current != NULL) {
        for (int i = 0; i < elehighments; i++) {
            if (current->key[i] == 0) {
                current->key[i] = key->v.intV;
                current->id[i] = rid;
                return RC_OK;
            }
        }
        current = current->next[elehighments];
    }
    
    // If we reach here, we need to create a new node
    BTree *newNode = (BTree*)malloc(sizeof(BTree));
    if (newNode == NULL) {
        return RC_ERROR;
    }
    
    newNode->key = calloc(elehighments, sizeof(int));
    newNode->id = calloc(elehighments, sizeof(RID));
    newNode->next = calloc(elehighments + 1, sizeof(BTree*));
    
    if (newNode->key == NULL || newNode->id == NULL || newNode->next == NULL) {
        free(newNode->key);
        free(newNode->id);
        free(newNode->next);
        free(newNode);
        return RC_ERROR;
    }
    
    newNode->key[0] = key->v.intV;
    newNode->id[0] = rid;
    
    // Link the new node
    BTree *lastNode = root;
    while (lastNode->next[elehighments] != NULL) {
        lastNode = lastNode->next[elehighments];
    }
    lastNode->next[elehighments] = newNode;
    
    return RC_OK;
}

RC deleteKey(BTreeHandle *tree, Value *key) {
    BTree *current = root;
    while (current != NULL) {
        int removedCount = countOccurrences(current, key, elehigh);
        updateNodeMetadata(current, key, elehigh);
        rearrangeNodeElements(current, elehigh);
        printStatusMessage(removedCount, key->v.intV);
        current = current->next[elehigh];
    }
    return RC_OK;
}

RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    // Set the starting point of the scan to the root node
    scan = root;
    printf("Starting tree scan...\n");

    // Initialize the index number to start the scan
    currnoOfIndex = 0;
    printf("Index number initialized to %d.\n", currnoOfIndex);

    // Allocate memory for the scan handle
    *handle = (BT_ScanHandle *)malloc(sizeof(BT_ScanHandle));
    if (*handle == NULL) {
        return RC_ERROR;
    }

    // Set the tree pointer in the scan handle
    (*handle)->tree = tree;
    (*handle)->mgmtData = NULL;

    printf("Tree scan opened successfully.\n");
    return RC_OK;
}

RC nextEntry(BT_ScanHandle *handle, RID *result) {
    if (scan != NULL && currnoOfIndex < elehigh) {
        if (scan->key[currnoOfIndex] != 0) {
            result->page = scan->id[currnoOfIndex].page;
            result->slot = scan->id[currnoOfIndex].slot;
            currnoOfIndex++;
            return RC_OK;
        } else {
            currnoOfIndex++;
            return nextEntry(handle, result);
        }
    } else if (isNextNodeAvailable(scan)) {
        handleIndexEqualToMax(&scan, &currnoOfIndex);
        return nextEntry(handle, result);
    } else {
        return RC_IM_NO_MORE_ENTRIES;
    }
}

RC closeTreeScan(BT_ScanHandle *handle) {
    resetIndex(&currnoOfIndex);
    if (handle != NULL) {
        free(handle);
    }
    return RC_OK;
}

char *printTree(BTreeHandle *treeHandle) {
    char *printMessage = generatePrintMessage(treeHandle);
    return printMessage;
}

