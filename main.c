/**
 * Name:    Francesco
 * Surname: Longo
 * ID:      223428
 * Lab:     4
 * Ex:      1
 *
 * Write a C program using Pthreads to sort the content of a binary file including a sequence of
 * random integer numbers, passed as an argument of the command line.
 * Map the file as a vector in memory.
 * Implement a threaded quicksort program where the recursive calls to quicksort are replaced
 * by threads activations, i.e. sorting is done, in parallel, in different regions of the file.
 * If the difference between the right and left indexes is less than a value size, given as an
 * argument of the command line, sorting is performed by the standard quicksort algorithm.
 *
 **/

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <string.h>
#include <stdarg.h>

#define NUMBER_INTEGERS_FILE 1000

// global variables
int n, *array, size;

// prototypes
void swap(int *a, int *b);
void quicksort(int v[], int left, int right);
void create_binary_file(char *filename);
void print_array(int *array);
void *quick_sort_thread (void *arg);

// struct
typedef struct {
    int left;
    int right;
} Region;

// main
int main(int argc, char **argv) {
    pthread_t th;
    Region *region;
    int fdin, len;
    struct stat statbuf;
    char *paddr;

    printf(" > Main started!\n");

    if (argc != 3) {
        printf("Expected 3 argument: %s <file_name> <size>\n", argv[0]);
        exit(-1);
    }

    size = atoi(argv[1]);

    // create binary file containing sequence of random intergers
    create_binary_file(argv[1]);

    // open input file
    // must be RDWR because we will write the modification after editing mapped file
    if ((fdin = open(argv[1], O_RDWR)) < 0) {
        printf("can't open %s for reading", argv[1]);
        exit(-2);
    }

    /* find size of input file */
    if (fstat(fdin, &statbuf) < 0) {
        printf("fstat error");
        exit(-3);
    }

    len = (int) statbuf.st_size;

    // n contains the number of integers
    // divide by sizeof(int) because binary file contains integers
    n = len / sizeof (int);
    printf(" > In this file there are %d integers!\n", n);

    // mmap the input file (does not work on Windows filesystem)
    // PROT_READ | PROT_WRITE are needed because the mapped file will be modified
    // MAP_SHARED is to permit all thread to read modification
    if ((paddr = mmap((caddr_t) 0, (size_t) len, PROT_READ | PROT_WRITE, MAP_SHARED, fdin, 0)) == (caddr_t) -1) {
        perror("mmap");
        //printf("mmap error for input");
        exit(-4);
    }

    // close file
    close(fdin);

    // array now contains the mapped file
    array = (int *) paddr;

    // initial region is [0, array size-1]
    region = (Region *) malloc (sizeof(Region));
    region->left = 0;
    region->right = n-1;

    printf("Array before sort:\n");
    print_array(array);
    printf("------------------\n");

    if(pthread_create(&th, NULL, quick_sort_thread, (void *) region) != 0) {
        printf("There was an error with the thread creation\n");
        exit(-5);
    }
    pthread_join(th, NULL);

    printf("Array after sort:\n");
    print_array(array);
    printf("------------------\n");

    printf(" > End!\n");

    free(region);
    free(array);

    return 0;
}

void create_binary_file(char *filename) {
    int i, *tmp;
    FILE *fp;
    tmp = malloc(NUMBER_INTEGERS_FILE * sizeof(int));
    time_t random_time;

    srand((unsigned) time(&random_time));

    //fill array v
    for(i = 0; i < NUMBER_INTEGERS_FILE; i++) {
        tmp[i] = rand() % 1000; // random number from 0 to 999
    }

    // save to text file in binary
    fp = fopen(filename, "wb");
    fwrite(tmp, sizeof(int), NUMBER_INTEGERS_FILE, fp);
    fclose(fp);
}

void print_array(int *array) {
    int i;

    for(i=0; i<n; i++) {
        printf("%d\n", array[i]);
    }
}

void *quick_sort_thread(void *arg) {
    Region *region = (Region *) arg;
    int left = region->left;
    int right = region->right;
    int i, j, x;
    pthread_t th1, th2;

    if (left >= right) {
        return NULL;
    }

    //arrays of length smaller than <size> are sorted by standard qsort
    if (right-left <= size) {
        quicksort(array, left, right);
        pthread_exit(NULL);
    }

    x = array[left];
    i = left - 1;
    j = right + 1;

    //quicksort performed using threads
    while (i < j) {
        while (array[--j] > x);

        while (array[++i] < x);

        if (i < j) {
            swap(&array[i], &array[j]);
        }
    }

    // create new threads for remaining regions
    region = (Region *) malloc (sizeof(Region));
    region->left = left;
    region->right = j;

    if (pthread_create(&th1, NULL, quick_sort_thread, (void *) region) != 0) {
        printf("There was an error with the thread creation\n");
        exit(-6);
    }

    region = (Region *) malloc (sizeof(Region));
    region->left = j+1;
    region->right = right;

    if(pthread_create(&th2, NULL, quick_sort_thread, (void *) region) != 0) {
        printf("There was an error with the thread creation\n");
        exit(-7);
    }

    pthread_join (th1, NULL);
    pthread_join (th2, NULL);

    free(region);

    pthread_exit(NULL);
}

void swap(int *a, int *b) {
    int t = *a;

    *a = *b;
    *b = t;
}

void quicksort(int v[], int left, int right) {
    int i, j, x;

    if (left >= right) {
        return;
    }

    x = v[left];
    i = left - 1;
    j = right + 1;

    while (i < j) {
        while (v[--j] > x);

        while (v[++i] < x);

        if (i < j)
            swap(&(v[i]), &(v[j]));
    }

    quicksort(v, left, j);
    quicksort(v, j + 1, right);
}
