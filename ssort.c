/* Parallel sample sort
 */
#include <stdio.h>
#include <unistd.h>
#include <mpi.h>
#include <stdlib.h>


static int compare(const void *a, const void *b)
{
  int *da = (int *)a;
  int *db = (int *)b;

  if (*da > *db)
    return 1;
  else if (*da < *db)
    return -1;
  else
    return 0;
}

int main( int argc, char *argv[])
{
  int rank, size, root = 0;
  int i, N, j;
  int *vec;
  int *split, *g_split; // local splitter and globally all the splitter
  int *bucket_offset, *bucket_size;
  int *bucket, g_bucket_size; // bucket to each processor
  int *recv_offset, *recv_size;
  int *rank_list, *ones;
  double T1, T2;
  
  if ( argc != 2 ) {
    fprintf(stderr, "Number of elements per processor must be given!\n");
    abort();
  }

  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  rank_list = calloc(size,sizeof(int));
  ones = calloc(size,sizeof(int));
  for (i=0; i<size; i++) {
      rank_list[i] = i;
      ones[i] = 1;
  }

  /* Number of random numbers per processor (this should be increased
   * for actual tests or could be passed in through the command line */
  N = atol(argv[1]);

  vec = calloc(N, sizeof(int));
  /* seed random number generator differently on every core */
  srand((unsigned int) (rank + 393919));

  /* fill vector with random integers */
  for (i = 0; i < N; ++i) {
    vec[i] = rand();
  }
  printf("rank: %d, first entry: %d\n", rank, vec[0]);

  MPI_Barrier(MPI_COMM_WORLD);
  T1 = MPI_Wtime();
  /* sort locally */
  qsort(vec, N, sizeof(int), compare);

  /* randomly sample s entries from vector or select local splitters,
   * i.e., every N/P-th entry of the sorted vector */
  split = calloc(size-1, sizeof(int));
  for (i=0; i < size-1; i++)
    split[i] = vec[N/size*(i+1)];

  /* every processor communicates the selected entries
   * to the root processor; use for instance an MPI_Gather */
  g_split = calloc(size*(size-1), sizeof(int));
  MPI_Gather(split, size-1, MPI_INT, g_split, size-1, MPI_INT, root, MPI_COMM_WORLD);

  /* root processor does a sort, determinates splitters that
   * split the data into P buckets of approximately the same size */
  if (rank == root) {
    qsort(g_split, size*(size-1), sizeof(int), compare);
    for (i=0; i < size-1;i++)
      split[i] = g_split[(size-1)*(i+1)];
  }

  /* root process broadcasts splitters */
  MPI_Bcast(split, size-1, MPI_INT, root, MPI_COMM_WORLD);

  /* every processor uses the obtained splitters to decide
   * which integers need to be sent to which other processor (local bins) */

  bucket_offset = calloc(size, sizeof(int));
  bucket_size = calloc(size, sizeof(int));

  j = 0;
  bucket_offset[0] = 0;
  for (i=0; i < N; i++) {
    if (vec[i] >= split[j]) {
	bucket_offset[j+1] = i;
	bucket_size[j] = bucket_offset[j+1]-bucket_offset[j];
	j++;
    }
    if (j == size-1)
	break;
  }
  bucket_size[j] = N - bucket_offset[j];
  for (i=j+1; i<size; i++) {
      bucket_size[i] = 0;
      bucket_offset[i] = N;
  }

  /* send and receive: either you use MPI_AlltoallV, or
   * (and that might be easier), use an MPI_Alltoall to share
   * with every processor how many integers it should expect,
   * and then use MPI_Send and MPI_Recv to exchange the data */
  recv_size = calloc(size, sizeof(int));
  MPI_Alltoallv( bucket_size, ones, rank_list, MPI_INT, recv_size, ones, rank_list, MPI_INT, MPI_COMM_WORLD);
  recv_offset = calloc(size, sizeof(int));
  recv_offset[0] = 0; 
  for (i = 1; i < size; i++)
      recv_offset[i] = recv_offset[i-1]+recv_size[i-1];
  g_bucket_size = recv_offset[size-1] + recv_size[size-1];
  bucket = calloc(g_bucket_size,sizeof(int));

  MPI_Alltoallv( vec, bucket_size, bucket_offset, MPI_INT, bucket, recv_size, recv_offset, MPI_INT, MPI_COMM_WORLD);

  /* do a local sort */
  qsort(bucket, g_bucket_size, sizeof(int), compare);
  MPI_Barrier(MPI_COMM_WORLD);
  T2 = MPI_Wtime();
  if (rank == 0)
    printf("Time for sorting %d elements per processor: %fs.\n", N, T2-T1);
  /* every processor writes its result to a file */
  {
    FILE* fd = NULL;
    char filename[256];
    snprintf(filename, 256, "output%02d.txt", rank);
    fd = fopen(filename,"w+");

    if(NULL == fd) {
      printf("Error opening file \n");
      return 1;
    }

    for(i = 0; i < g_bucket_size; ++i)
      fprintf(fd, "%d\n", bucket[i]);

    fclose(fd);
  }

  free(vec);
  free(split);
  free(g_split);
  free(bucket);
  free(bucket_size);
  free(bucket_offset);
  free(recv_size);
  free(recv_offset);
  free(rank_list);
  free(ones);
  MPI_Finalize();
  return 0;
}
