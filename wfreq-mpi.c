#include <mpi.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

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
    int l1[5] = { 3, 2, 3, 3, 2 };
    char m2[7] = "ba\0qu\0";
    int l2[2] = { 2, 2 };
    char m3[7] = "qu\0qu\0";
    int l3[2] = { 2, 2 };
    int total_len = 19 + 14; // Sum of lenths of m1, m2 and m3

    char* buf = malloc(sizeof(char) * total_len * world_size); // Each row r is the data being sent to reducer r
    int starter_for_each = total_len;
    int* chars_per_reducer = (int*)calloc(world_size, sizeof(int)); // Storing number of chars to be sent to reducer idx

    char* fpointer; // Pointer to array processed by this proc
    int* l_of_str; // Pointer to len of strings in array processed by this proc
    int l_of_l; // Length of l_of_str

    if (world_rank == 0) fpointer = m1, l_of_str = l1, l_of_l = 5;
    else if (world_rank == 1) fpointer = m2, l_of_str = l2, l_of_l = 2;
    else if (world_rank == 2) fpointer = m3, l_of_str = l3, l_of_l = 2;
    else printf("Rank %d - SHOULDN'T REACH HERE!!!!\n", world_rank);

    int spointer = 0;
    for (int i = 0; i < l_of_l; i++) {
        
        int which_reducer = hash(&fpointer[spointer]);
        memcpy(&buf[which_reducer * total_len] + chars_per_reducer[which_reducer], fpointer + spointer, l_of_str[i] + 1);
        spointer += l_of_str[i] + 1;
        chars_per_reducer[which_reducer] += l_of_str[i] + 1;
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

        if (world_rank == 2 && i == 2) {
            for (int j=0; j<num_elem_for_red; j++) {
                printf("%d = %c\n", j, tmp[j]);
            }
        }
    }

    free(buf);
    free(chars_per_reducer);
    MPI_Finalize();
}
