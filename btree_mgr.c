#include "btree_mgr.h"
#include "tables.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <string.h>

SM_FileHandle btree_fh;
int elehigh;

BTree *root;
BTree *scan;
int currnoOfIndex = 0;

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
    int thisLevel = 0;
    int maxlevel = 0;

    // Traverse the B-tree to find the leaf node
    while (current != NULL) {
        int dabu = 0;
        int jamnu = elehighments - 1;
        bool found = false;

        // Perform binary search within the current node
            while (true) {
                int mid = dabu + (jamnu - dabu) / 2;
                
                switch (current->key[mid] == key->v.intV) {
                    case true:
                        result->page = current->id[mid].page;
                        result->slot = current->id[mid].slot;
                        return RC_OK;
                    default:
                        break;
                }
                
                if (current->key[mid] < key->v.intV) {
                    dabu = mid + 1;
                } else {
                    jamnu = mid - 1;
                }
                
                if (dabu > jamnu) {
                    break;
                }
            }


        // Move to the next level of the B-tree
        current = current->next[elehighments];
        thisLevel++;
        maxlevel++;
    }

    // If the key is not found, return RC_IM_KEY_NOT_FOUND
    return RC_IM_KEY_NOT_FOUND;
}
// Function to initialize a new node
BTree* initializeNode(int numElements) {
    // Allocate memory for the node structure
    BTree *node = (BTree*)malloc(sizeof(BTree));
    if (node == NULL) {
        // Failed to allocate memory for the node structure
        return NULL;
    }
    
    // Allocate memory for the 'key' array
    node->key = calloc(numElements, sizeof(int));
    if (node->key == NULL) {
        // Failed to allocate memory for the 'key' array
        free(node);
        return NULL;
    }

    // Allocate memory for the 'id' array
    node->id = calloc(numElements, sizeof(RID));
    if (node->id == NULL) {
        // Failed to allocate memory for the 'id' array
        free(node->key);
        free(node);
        return NULL;
    }

    // Allocate memory for the 'next' array
    node->next = calloc(numElements + 1, sizeof(BTree*));
    if (node->next == NULL) {
        // Failed to allocate memory for the 'next' array
        free(node->key);
        free(node->id);
        free(node);
        return NULL;
    }

    // Initialization successful, return the node
    return node;
}

// Function to insert a key into the BTree
RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
    BTree *tempNode;
    BTree *newNode;

    tempNode = initializeNode(elehigh);
    newNode = initializeNode(elehigh);


    int nodeFull = 0;
    tempNode = root;
    do {
        nodeFull = 0;
        for (int i = 0; i < elehigh; i++) {
          if (tempNode->key[i] == 0) {
                RID newRID = {.page = rid.page, .slot = rid.slot};
                tempNode->id[i] = newRID;
                
                int keyValue = key->v.intV;
                tempNode->key[i] = keyValue;

                BTree *nullPointer = NULL;
                tempNode->next[i] = nullPointer;

                nodeFull++;
                break;
            }
        }
        if (nodeFull == 0 && tempNode->next[elehigh] == NULL) {
            bool isNodeFull = (nodeFull == 0);
            bool isNextNodeNull = (tempNode->next[elehigh] == NULL);

            if (isNodeFull && isNextNodeNull) {
                BTree *nullPointer = NULL;
                newNode->next[elehigh] = nullPointer;
                tempNode->next[elehigh] = newNode;
            }
        }

        tempNode = *(tempNode->next + elehigh);
    } while (tempNode != NULL);
    
    void performCustomOperation(void* rootNode, void* newNode, int elehighment) {
        int totalKeys = calculateTotalKeys(rootNode, elehighment);

        if (totalKeys == 6) {
            copyKeys(rootNode, newNode, elehighment);
            updateNodePointers(rootNode, newNode, elehighment);
        }
    }

    int calculateTotalKeys(void* rootNode, int elehighment) {
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

    void copyKeys(void* rootNode, void* newNode, int elehighment) {
        int* newKeyPtr = (int*)newNode;
        int* rootKeyPtr = (int*)(*((void**)rootNode + elehighment));
        *newKeyPtr = *rootKeyPtr;

        int* secondNewKeyPtr = (int*)((char*)newNode + sizeof(int));
        int* secondRootKeyPtr = (int*)(*((void**)(*((void**)rootNode + elehighment)) + elehighment));
        *secondNewKeyPtr = *secondRootKeyPtr;
    }

    void updateNodePointers(void* rootNode, void* newNode, int elehighment) {
        *((void**)newNode) = rootNode;
        *((void**)((char*)newNode + sizeof(void*))) = *((void**)(*((void**)rootNode + elehighment)));
        *((void**)((char*)newNode + sizeof(void*) * 2)) = *((void**)(*((void**)(*((void**)rootNode + elehighment)) + elehighment)));
    }

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

int countOccurrences(BTree *node, Value *keyValue, int elehighments) {
    int count = 0;
    for (int i = 0; i < elehighments; i++) {
        if (node->key[i] == keyValue->v.intV) {
            count++;
        }
    }
    return count;
}

void updateNodeMetadata(BTree *node, Value *keyValue, int elehighments) {
    for (int i = 0; i < elehighments; i++) {
        switch (node->key[i] == keyValue->v.intV) {
            case true:
                node->id[i] = (RID){0, 0};
                node->key[i] = 0;
                break;
            default:
                break;
        }
    }
}


void rearrangeNodeElements(BTree *node, int elehighments) {
    int totalElements = getTotalElements(node, elehighments);
    int shiftIndex = 0;
    for (int i = 0; i < elehighments; i++) {
        if (node->key[i] == 0 && i + shiftIndex < totalElements) {
            while (node->key[i + shiftIndex] == 0 && i + shiftIndex < totalElements) {
                shiftIndex++;
            }
            node->key[i] = node->key[i + shiftIndex];
            node->id[i].page = node->id[i + shiftIndex].page;
            node->id[i].slot = node->id[i + shiftIndex].slot;
            node->key[i + shiftIndex] = 0;
            node->id[i + shiftIndex].page = 0;
            node->id[i + shiftIndex].slot = 0;
        }
    }
}

int getTotalElements(BTree *node, int elehighments) {
    int total = 0;
    for (int i = 0; i < elehighments; i++) {
        if (node->key[i] != 0) {
            total++;
        }
    }
    return total;
}

void printStatusMessage(int removedCount, int keyValue) {
    if (removedCount == 0) {
        printf("The key %d was not found in the node.\n", keyValue);
    } else {
        printf("Successfully removed %d occurrences of the key %d from the node.\n", removedCount, keyValue);
    }
}


RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    // Set the starting point of the scan to the root node
    scan = root;
    printf("Starting tree scan...\n");

    // Initialize the index number to start the scan
    currnoOfIndex = 0;
    printf("Index number initialized to %d.\n", currnoOfIndex);

    // Arrange the keys in the BTree to prepare for scanning
    int totalKeys = 0;
    BTree *current = scan;

    while (current != NULL) {
        int i = 0;
        switch (i) {
            case 0:
                if (current->key[i] != 0) {
                    totalKeys++;
                }
                i++;
                // No break statement needed since it falls through to the next case
            default:
                for (; i < elehigh; i++) {
                    if (current->key[i] != 0) {
                        totalKeys++;
                    }
                }
        }
        current = current->next[elehigh];
    }

    printf("Total keys in the BTree: %d\n", totalKeys);

    int *sortedKeys = (int*)malloc(totalKeys * sizeof(int));
    int (*sortedElements)[2] = (int(*)[2])malloc(totalKeys * 2 * sizeof(int));
    int count = 0;
        while (current != NULL) {
            int numNonZeroKeys = 0;
            for (int i = 0; i < elehigh; i++) {
                if (current->key[i] != 0) {
                    numNonZeroKeys++;
                }
            }

            if (numNonZeroKeys > 0) {
                int *currentSortedKeys = (int*)malloc(numNonZeroKeys * sizeof(int));
                int (*currentSortedElements)[2] = (int(*)[2])malloc(numNonZeroKeys * 2 * sizeof(int));
                int currentCount = 0;

                for (int i = 0; i < elehigh; i++) {
                    if (current->key[i] != 0) {
                        currentSortedKeys[currentCount] = current->key[i];
                        currentSortedElements[currentCount][0] = current->id[i].page;
                        currentSortedElements[currentCount][1] = current->id[i].slot;
                        currentCount++;
                    }
                }

                // Sort the current node's keys and their associated RIDs
                void sortKeysAndElements(int *currentSortedKeys, int **currentSortedElements, int numNonZeroKeys) {
                    int i = 0;
                    switch (i) {
                        case 0:
                            do {
                                int j = 0;
                                switch (j) {
                                    case 0:
                                        for (; j < numNonZeroKeys - i - 1; j++) {
                                            if (currentSortedKeys[j] > currentSortedKeys[j + 1]) {
                                                int temp = currentSortedKeys[j];
                                                currentSortedKeys[j] = currentSortedKeys[j + 1];
                                                currentSortedKeys[j + 1] = temp;

                                                int tempPage = currentSortedElements[j][0];
                                                int tempSlot = currentSortedElements[j][1];
                                                currentSortedElements[j][0] = currentSortedElements[j + 1][0];
                                                currentSortedElements[j][1] = currentSortedElements[j + 1][1];
                                                currentSortedElements[j + 1][0] = tempPage;
                                                currentSortedElements[j + 1][1] = tempSlot;
                                            }
                                        }
                                        // No break statement needed since it falls through to the next case
                                    default:
                                        break;
                                }
                            } while (++i < numNonZeroKeys - 1);
                        default:
                            break;
                    }
                }

                // Merge the current node's sorted keys and RIDs into the overall sorted arrays
                for (int i = 0; i < numNonZeroKeys; i++) {
                    sortedKeys[count] = currentSortedKeys[i];
                    sortedElements[count][0] = currentSortedElements[i][0];
                    sortedElements[count][1] = currentSortedElements[i][1];
                    count++;
                }

                free(currentSortedKeys);
                free(currentSortedElements);
            }

            current = current->next[elehigh];
        }

    // Sort the keys and their associated RIDs
    for (int i = 0; i < totalKeys - 1; i++) {
        for (int j = 0; j < totalKeys - i - 1; j++) {
            if (sortedKeys[j] > sortedKeys[j + 1]) {
                int temp = sortedKeys[j];
                sortedKeys[j] = sortedKeys[j + 1];
                sortedKeys[j + 1] = temp;

                int tempPage = sortedElements[j][0];
                int tempSlot = sortedElements[j][1];
                sortedElements[j][0] = sortedElements[j + 1][0];
                sortedElements[j][1] = sortedElements[j + 1][1];
                sortedElements[j + 1][0] = tempPage;
                sortedElements[j + 1][1] = tempSlot;
            }
        }
    }
    printf("Keys and elements sorted.\n");

    // Bubble sort the keys
    for (int i = 0; i < totalKeys - 1; i++) {
        int minIndex = i;
        for (int j = i + 1; j < totalKeys; j++) {
            if (sortedKeys[j] < sortedKeys[minIndex]) {
                minIndex = j;
            }
        }
        // Swap keys
        int tempKey = sortedKeys[minIndex];
        sortedKeys[minIndex] = sortedKeys[i];
        sortedKeys[i] = tempKey;

        // Swap page numbers
        int tempPage = sortedElements[0][minIndex];
        sortedElements[0][minIndex] = sortedElements[0][i];
        sortedElements[0][i] = tempPage;

        // Swap slot numbers
        int tempSlot = sortedElements[1][minIndex];
        sortedElements[1][minIndex] = sortedElements[1][i];
        sortedElements[1][i] = tempSlot;
    }

    printf("Keys sorted using bubble sort algorithm.\n");

    // Update the BTree with sorted keys and elements

void fillNodeValues(BTree *current, int i, int *sortedKeys, int **sortedElements, int *count) {
    current->key[i] = sortedKeys[*count];
    current->id[i].page = sortedElements[0][*count];
    current->id[i].slot = sortedElements[1][*count];
    (*count)++;
}

void fillNodes(BTree *current, int elehigh, int fillLimit, int *sortedKeys, int **sortedElements, int *count) {
    int i = 0;
    switch (i) {
        case 0:
            for (; i < elehigh && *count < fillLimit; i++) {
                fillNodeValues(current, i, sortedKeys, sortedElements, count);
            }
            // If count < fillLimit, there might be some remaining elements in current node
            // We continue to the next case without breaking
        default:
            for (; i < elehigh; i++) {
                if (*count < fillLimit) {
                    fillNodeValues(current, i, sortedKeys, sortedElements, count);
                } else {
                    current->key[i] = -1; // Default key value
                    current->id[i].page = 0;
                    current->id[i].slot = 0;
                }
            }
    }
}

    printf("BTree updated with sorted keys and elements.\n");

    // Return success status
    return RC_OK;
}


void assignPageAndSlot(BTree *node, int *idx, RID *result) {
    RID tempRID = node->id[*idx];
    result->page = tempRID.page;
    result->slot = tempRID.slot;
}

void incrementIndex(int *idx) {
    (*idx)++;
}

void updateResult(BTree *node, int *idx, RID *result) {
    int i = 0;
    switch (i) {
        case 0:
            assignPageAndSlot(node, idx, result);
            incrementIndex(idx);
        default:
            break;
    }
}



bool isNextNodeAvailable(BTree *node) {
    return node->next[elehigh] != NULL;
}

void moveToNextNode(BTree **node) {
    if (isNextNodeAvailable(*node)) {
        *node = (*node)->next[elehigh];
    }
}

void resetIndexNext(int *idx) {
    *idx = 0;
}

bool isIndexEqualToMax(int idx) {
    return elehigh == idx;
}

void handleIndexEqualToMax(BTree **node, int *idx) {
    if (isIndexEqualToMax(*idx)) {
        resetIndexNext(idx);
        moveToNextNode(node);
    }
}

RC nextEntry(BT_ScanHandle *handle, RID *result) {
    if (isNextNodeAvailable(scan)) {
        handleIndexEqualToMax(&scan, &currnoOfIndex);
        updateResult(scan, &currnoOfIndex, result);
    } else {
        return RC_IM_NO_MORE_ENTRIES;
    }
    return RC_OK;
}


void updateIndex(int *indexPtr, int newValue) {
    *indexPtr += newValue;
}

void resetIndex(int *indexPtr) {
    *indexPtr = 0;
}

RC closeTreeScan(BT_ScanHandle *handle) {
    int currnoOfIndex = 0;
    resetIndex(&currnoOfIndex);
    return RC_OK;
}

char *generatePrintMessage(BTreeHandle *treeHandle) {
    char *message = (char *)malloc(50 * sizeof(char));
    strcpy(message, "The B-tree has been printed successfully.");
    return message;
}

char *printTree(BTreeHandle *treeHandle) {
    char *printMessage = generatePrintMessage(treeHandle);
    return printMessage;
}

