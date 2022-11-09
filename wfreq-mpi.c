#include <mpi.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <ctype.h>
#include "ht.h"

int hash(char* x, int len) {
    if (strncmp(x, "foo", len) == 0 || strncmp(x, "ba", len) == 0) return 2;
    if (strncmp(x, "qu", len) == 0) return 1;
    if (strncmp(x, "baz", len) == 0) return 0;
    printf("Hash %s - SHOULDN'T REACH HERE!!!!\n", x);

}

int main() {
    MPI_Init(NULL, NULL);
    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    char m1[] = "foo ba baz foo ba";
    int l1 = 18;
    char m2[] = "ba qu";
    int l2 = 6;
    char m3[] = "qu ba";
    int l3 = 6;
    int total_len = (18 + 6 + 6)*2; // Sum of lenths of m1, m2 and m3 (Denotes total file len in HDFS)

    char* buf = malloc(sizeof(char) * total_len * world_size); // Each row r is the data being sent to reducer r
    int* chars_per_reducer = (int*)calloc(world_size, sizeof(int)); // Storing number of chars to be sent to reducer idx

    char* fpointer; // Pointer to array processed by this proc
    int l;

    if (world_rank == 0) fpointer = m1, l = l1;
    else if (world_rank == 1) fpointer = m2, l = l2;
    else if (world_rank == 2) fpointer = m3, l = l3;
    else printf("Rank %d - SHOULDN'T REACH HERE!!!!\n", world_rank);

    int start = 0;
    while (start < l - 1) {
        while (isspace(fpointer[start]) && start < l - 1) start = start + 1;
        int end = start + 1;
        while (!isspace(fpointer[end]) && (end < l)) end = end + 1;
        int len = end - start;
        int which_reducer = hash(&fpointer[start], len);
        memcpy(&buf[which_reducer * total_len] + chars_per_reducer[which_reducer], fpointer + start, len);
        buf[which_reducer * total_len + chars_per_reducer[which_reducer] + len + 1] = '\0';
        if (end == l) {
            chars_per_reducer[which_reducer] += len;
        } else {
            chars_per_reducer[which_reducer] += len + 1;
        }
        start = end;
    }
    // for (int i=0; i<world_size; i++)
    //     printf("%d ", chars_per_reducer[i]);
    // printf("\n");

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

        if (world_rank == i && world_rank == 0) {
            // for (int k=0; k<num_elem_for_red; k++) {
            //     printf("%d = %c\n", k, tmp[k]);
            // }
            ht* counts = ht_create();
            int spointer = 0;
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
