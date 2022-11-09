#include <mpi.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "ht.h"

int hash(char* x) {
    if (strcmp(x, "foo") == 0 || strcmp(x, "ba") == 0) return 2;
    if (strcmp(x, "qu") == 0) return 1;
    if (strcmp(x, "baz") == 0) return 0;
    printf("Hash %s - SHOULDN'T REACH HERE!!!!\n", x);
}

int main() {
    MPI_Init(NULL, NULL);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    char m1[19] = "foo\0ba\0baz\0foo\0ba\0";
    char m2[7] = "ba\0qu\0";
    char m3[7] = "qu\0qu\0";
    int total_len = 19 + 14; // Sum of lenths of m1, m2 and m3 (Denotes total file len in HDFS)

    char* buf = malloc(sizeof(char) * total_len * world_size); // Each row r is the data being sent to reducer r
    int* chars_per_reducer = (int*)calloc(world_size, sizeof(int)); // Storing number of chars to be sent to reducer idx

    char* fpointer; // Pointer to array processed by this proc
    int l_of_l; // Number of strings in array

    if (world_rank == 0) fpointer = m1, l_of_l = 5;
    else if (world_rank == 1) fpointer = m2, l_of_l = 2;
    else if (world_rank == 2) fpointer = m3, l_of_l = 2;
    else printf("Rank %d - SHOULDN'T REACH HERE!!!!\n", world_rank);

    int spointer = 0;
    for (int i = 0; i < l_of_l; i++) {
        int which_reducer = hash(&fpointer[spointer]);
        int len = strlen(&fpointer[spointer]);
        memcpy(&buf[which_reducer * total_len] + chars_per_reducer[which_reducer], fpointer + spointer, len + 1);
        spointer += len + 1;
        chars_per_reducer[which_reducer] += len + 1;
    }

    int* M = (int*)malloc(sizeof(int) * world_size * world_size);
    MPI_Allgather(chars_per_reducer, world_size, MPI_INT, M, world_size, MPI_INT, MPI_COMM_WORLD);

    int num_elem_for_red = 0;
    for (int i = 0; i < world_size; i++) {
        num_elem_for_red += M[world_size * i + world_rank];
    }

    char* recv_buf = malloc(num_elem_for_red * sizeof(char));

    for (int i = 0; i < world_size; i++) {
        char* tmp = NULL;
        if (world_rank == i) {
            tmp = recv_buf;
        }
        int* recvcounts = malloc(world_size * sizeof(int));
        for (int j = 0; j < world_size; j++) {
            recvcounts[j] = M[world_size * j + i];
        }
        int* displs = malloc(world_size * sizeof(int));
        displs[0] = 0;
        for (int j = 1; j < world_size; j++) {
            displs[j] = displs[j - 1] + recvcounts[j - 1];
        }
        MPI_Gatherv(&buf[i * total_len], M[world_size * world_rank + i], MPI_CHAR, tmp, recvcounts, displs, MPI_CHAR, i, MPI_COMM_WORLD);

        // if (world_rank == 2 && i == 2) {
        //     for (int j=0; j<num_elem_for_red; j++) {
        //         printf("%d = %c\n", j, tmp[j]);
        //     }
        // }

        if (world_rank == i) {
            ht* counts = ht_create();
            spointer = 0;
            while (spointer < num_elem_for_red) {
                int len = strlen(&tmp[spointer]);
                int value = ht_get(counts, &tmp[spointer]);
                if (value == -1) {
                    value = 1;
                }
                else {
                    value += 1;
                }
                ht_set(counts, &tmp[spointer], value);
                spointer += len + 1;
            }
            hti it = ht_iterator(counts);
            while (ht_next(&it)) {
                printf("%s %d\n", it.key, it.value);
            }
        }
        free(recvcounts);
        free(displs);
    }

    free(recv_buf);
    free(M);
    free(buf);
    free(chars_per_reducer);
    MPI_Finalize();
}
