CC := gcc
CFLAGS := -g -Wall
LIBS := -lm

# Executables
EXECUTABLES := test_assign4_1 test_expr

# Object files
OBJ_FILES := storage_mgr.o dberror.o buffer_mgr.o buffer_mgr_stat.o btree_mgr.o record_mgr.o rm_serializer.o expr.o

# Source and header dependencies for tests
TEST_ASSIGN4_1_DEPS := test_assign4_1.c dberror.h storage_mgr.h buffer_mgr.h buffer_mgr_stat.h btree_mgr.h record_mgr.h expr.h
TEST_EXPR_DEPS := test_expr.c dberror.h storage_mgr.h buffer_mgr.h buffer_mgr_stat.h btree_mgr.h record_mgr.h expr.h

.PHONY: default clean run_test_assign4_1 run_test_expr

default: $(EXECUTABLES)

test_assign4_1: test_assign4_1.o $(OBJ_FILES)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

test_expr: test_expr.o $(OBJ_FILES)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS)

test_assign4_1.o: $(TEST_ASSIGN4_1_DEPS)
	$(CC) $(CFLAGS) -c $< $(LIBS)

test_expr.o: $(TEST_EXPR_DEPS)
	$(CC) $(CFLAGS) -c $< $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< $(LIBS)

clean:
	$(RM) $(EXECUTABLES) *.o *~

run_test_assign4_1:
	./test_assign4_1

run_test_expr:
	./test_expr
