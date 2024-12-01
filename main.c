#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#define DIE(assertion, call_description)				\
	do {								\
		if (assertion) {					\
			fprintf(stderr, "(%s, %d): ",			\
					__FILE__, __LINE__);		\
			perror(call_description);			\
			exit(errno);				        \
		}							\
	} while (0)
#define NUM_LETTERS 26
#define HASH_SIZE 991
// Structure to store each word and associated data
typedef struct Node {
    char *word;
    // Array of file IDs where the word is present    
    int *file_ids;
    // Counting the number of files where the word appears
    int count;
    int total_count;
    // For the array
    int capacity;
    struct Node *next;
} Node;
// Hash map structure containing the hash table and mutexes
typedef struct {
    Node **table;
    // one for every bucket
    pthread_mutex_t *locks;
} HashMap;
// Manage the list of files
typedef struct {
    // File names
    char **files;
    int total_files;
    int current_index;
    pthread_mutex_t lock;
} FileList;
// Used for the mapper threads
typedef struct {
    FileList *file_list;
    // Syncronize mappers and reducers
    pthread_barrier_t *barrier;
    HashMap *hash_map;
} MapperArgs;
// Used for the reducer threads
typedef struct {
    HashMap *hash_map;
    pthread_barrier_t *barrier;
    int reducer_id;
    int num_reducers;
    // Array of mutexes for letters
    pthread_mutex_t *letter_mutexes;
} ReducerArgs;

// Start working with files
FileList *initialize_file_list(const char *input_file) {
    FILE *file = fopen(input_file, "r");
    // Error opening file
    if (!file) {
        return NULL;
    }
    // Allocate memory for the FileList structure
    FileList *file_list = malloc(sizeof(FileList));
    DIE(file_list == NULL, "malloc() failed!\n");
    if (!file_list) {
        fclose(file);
        return NULL;
    }

    // Read the total number of files
    if (fscanf(file, "%d", &file_list->total_files) != 1) {
        // Error reading number of files
        fclose(file);
        free(file_list);
        return NULL;
    }

    // Allocate memory for the array of file names
    file_list->files = malloc(file_list->total_files * sizeof(char *));
    DIE(file_list->files == NULL, "malloc() failed!\n");
    if (!file_list->files) {
        // Error allocating memory for files
        fclose(file);
        free(file_list);
        return NULL;
    }

    // Read each file name and allocate memory for it
    for (int i = 0; i < file_list->total_files; i++) {
        file_list->files[i] = malloc(256 * sizeof(char));
        if (!file_list->files[i]) {
            fclose(file);
            // Free previously allocated file names
            for (int j = 0; j < i; j++) {
                free(file_list->files[j]);
            }
            free(file_list->files);
            free(file_list);
            exit(EXIT_FAILURE);
        }
        fscanf(file, "%s", file_list->files[i]);
    }
    // Initialize the current index and mutex
    file_list->current_index = 0;                 
    pthread_mutex_init(&file_list->lock, NULL);
    fclose(file);                              
    return file_list;
}

char *get_next_file(FileList *file_list, int *file_id) {

    pthread_mutex_lock(&file_list->lock);
    if (file_list->current_index >= file_list->total_files) {
        pthread_mutex_unlock(&file_list->lock);
        return NULL;
    }
    *file_id = file_list->current_index + 1;
    char *file_name = file_list->files[file_list->current_index++];
    pthread_mutex_unlock(&file_list->lock);
    return file_name;
}
// Map a string to an index in the hash table
unsigned long hash_function(const char *str) {
    unsigned long hash = 5381;
    int c;
    while ((c = (unsigned char)*str++)) {
        hash = ((hash << 5) + hash) + c;
    }
    return hash % HASH_SIZE;
}

HashMap *initialize_hash_map() {
    HashMap *hash_map = malloc(sizeof(HashMap));
    DIE(hash_map == NULL, "malloc() failed!\n");
     // Error allocating memory for HashMap
    if (!hash_map) {
        return NULL;
    }

    // Allocate memory for the hash table
    hash_map->table = malloc(HASH_SIZE * sizeof(Node *));
    if (!hash_map->table) {
        // Error allocating memory for hash table
        free(hash_map);
        return NULL;
    }

    // Allocate memory for the mutexes
    hash_map->locks = malloc(HASH_SIZE * sizeof(pthread_mutex_t));
    if (!hash_map->locks) {
        // Error allocating memory for mutexes
        free(hash_map->table);
        free(hash_map);
        return NULL;
    }

    // initialize mutexes
    for (int i = 0; i < HASH_SIZE; i++) {
        hash_map->table[i] = NULL;
        pthread_mutex_init(&hash_map->locks[i], NULL);
    }

    return hash_map;
}

void add_to_hash_map(HashMap *hash_map, const char *word, int file_id) {
    unsigned long index = hash_function(word);
    // Lock the bucket
    pthread_mutex_lock(&hash_map->locks[index]);

    Node *current = hash_map->table[index];
    while (current) {
        if (strcmp(current->word, word) == 0) {
            current->total_count++;
            // Check if file_id already exits
            for (int i = 0; i < current->count; i++) {
                // file_id already exists
                if (current->file_ids[i] == file_id) {
                    pthread_mutex_unlock(&hash_map->locks[index]);
                    return;
                }
            }
            // Add new fileid to the array
            if (current->count >= current->capacity) {
                current->capacity *= 2;
                int *new_file_ids = realloc(current->file_ids, current->capacity * sizeof(int));
                if (!new_file_ids) {
                    // Error reallocating memory for file_ids
                    pthread_mutex_unlock(&hash_map->locks[index]);
                    exit(EXIT_FAILURE);
                }
                current->file_ids = new_file_ids;
            }
            current->file_ids[current->count++] = file_id;
            pthread_mutex_unlock(&hash_map->locks[index]);
            return;
        }
        current = current->next;
    }

    // Word not found in the hash map, create a new Node
    Node *new_node = malloc(sizeof(Node));
    // Error allocating memory for new node
    DIE(new_node == NULL, "malloc() failed!\n");
    if (!new_node) {
        pthread_mutex_unlock(&hash_map->locks[index]);
        exit(EXIT_FAILURE);
    }

    new_node->word = strdup(word);
    new_node->file_ids = malloc(2 * sizeof(int));
    // Error allocating memory for new node
    if (!new_node->word || !new_node->file_ids) {
        pthread_mutex_unlock(&hash_map->locks[index]);
        exit(EXIT_FAILURE);
    }

    // Initialize the new node and insert at the beggining of the bucket
    new_node->file_ids[0] = file_id;
    new_node->count = 1;
    new_node->total_count = 1;
    new_node->capacity = 2;
    new_node->next = hash_map->table[index];
    hash_map->table[index] = new_node;

    pthread_mutex_unlock(&hash_map->locks[index]);
}
 
int is_alpha(char c) {
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
        return 1;
    }
    return 0; 
}
char to_lower(char c) {
    if (c >= 'A' && c <= 'Z') {
        return c + ('a' - 'A');
    }
    return c;
}
//Function executed by mapper threads.
//Each mapper processes files and adds words to the hash map.
void *mapper_function(void *arg) {
    MapperArgs *args = (MapperArgs *)arg;
    FileList *file_list = args->file_list;
    HashMap *hash_map = args->hash_map;

    char *file_name;
    int file_id;
    // Get the next file to process
    while ((file_name = get_next_file(file_list, &file_id)) != NULL) {
        FILE *file = fopen(file_name, "r");
        if (!file) {
            // Cannot open file, skip to next
            continue;
        }

        char word[256];
        // Read words from the file
        while (fscanf(file, "%255s", word) != EOF) {
            char new_word[256];
            int idx = 0;
            // Clean the word by removing non-alphabetic characters and converting to lowercase
            for (int i = 0; word[i]; i++) {
                if (is_alpha((unsigned char)word[i])) {
                    new_word[idx++] = to_lower((unsigned char)word[i]);
                }
            }
            new_word[idx] = '\0';
            if (strlen(new_word) > 0) {
                add_to_hash_map(hash_map, new_word, file_id);
            }
        }
        fclose(file);
    }

    pthread_barrier_wait(args->barrier);
    pthread_exit(NULL);
}

//Comparison function for sorting Nodes in the reducer.
int compare_nodes(const void *a, const void *b) {
    Node *nodeA = *(Node **)a;
    Node *nodeB = *(Node **)b;

    // Compare descending by number of files
    if (nodeB->count != nodeA->count) {
        return nodeB->count - nodeA->count;
    }
    //compare alphabetically
    return strcmp(nodeA->word, nodeB->word);
}

//  Used to sort the file_ids array within each Node.
int compare_ints(const void *a, const void *b) {
    int int_a = *(const int *)a;
    int int_b = *(const int *)b;
    return int_a - int_b;
}

void *reducer_function(void *arg) {
    ReducerArgs *args = (ReducerArgs *)arg;
    HashMap *hash_map = args->hash_map;
    int reducer_id = args->reducer_id;
    int num_reducers = args->num_reducers;
    pthread_mutex_t *letter_mutexes = args->letter_mutexes;

    // Wait for all mappers to finish
    pthread_barrier_wait(args->barrier);

    // Determine the letters that are going to be proccessed
    int base_letters = NUM_LETTERS / num_reducers;
    int extra_letters = NUM_LETTERS % num_reducers;

    int start_letter;
    int letters_to_process;

    if (reducer_id < extra_letters) {
        letters_to_process = base_letters + 1;
        start_letter = reducer_id * letters_to_process;
    } else {
        letters_to_process = base_letters;
        start_letter = reducer_id * letters_to_process + extra_letters;
    }
    int end_letter = start_letter + letters_to_process - 1;

    //total number of nodes
    int total_nodes = 0;
    for (int i = 0; i < HASH_SIZE; i++) {
        Node *current = hash_map->table[i];
        while (current) {
            char first_letter = to_lower((unsigned char)current->word[0]);
            int letter_index = first_letter - 'a';
            if (letter_index >= start_letter && letter_index <= end_letter) {
                total_nodes++;
            }
            current = current->next;
        }
    }

    // hold the pointers to the nodes
    Node **nodes = malloc(total_nodes * sizeof(Node *));
    if (!nodes) {
        exit(EXIT_FAILURE);
    }

    // Collect the nodes that start with the letters assigned
    int node_count = 0;
    for (int i = 0; i < HASH_SIZE; i++) {
        Node *current = hash_map->table[i];
        while (current != NULL) {
            char first_letter = to_lower((unsigned char)current->word[0]);
            int letter_index = first_letter - 'a';
            if (letter_index >= start_letter && letter_index <= end_letter) {
                nodes[node_count++] = current;
            }
            current = current->next;
        }
    }
    // Sort the nodes
    qsort(nodes, node_count, sizeof(Node *), compare_nodes);

    // Write the results to the output files corresponding to the letters
    for (int i = start_letter; i <= end_letter; i++) {
        // Lock the mutex for this letter
        pthread_mutex_lock(&letter_mutexes[i]);

        char filename[10];
        sprintf(filename, "%c.txt", 'a' + i);
        FILE *file = fopen(filename, "w");
        if (!file) {
            // Error opening output file
            pthread_mutex_unlock(&letter_mutexes[i]);
            continue;
        }

        // sort file_ids and write to the file
        for (int j = 0; j < node_count; j++) {
            Node *current = nodes[j];
            char first_letter = to_lower((unsigned char)current->word[0]);
            if (first_letter - 'a' == i) {
                // Sort the file_ids array before printing
                qsort(current->file_ids, current->count, sizeof(int), compare_ints);

                fprintf(file, "%s:[", current->word);
                for (int k = 0; k < current->count; k++) {
                    fprintf(file, "%d%s",
                            current->file_ids[k],
                            (k == current->count - 1) ? "]\n" : " ");
                }
            }
        }

        fclose(file);
        // Unlock the mutex for this letter
        pthread_mutex_unlock(&letter_mutexes[i]);
    }
    free(nodes);

    pthread_exit(NULL);
}

// Free functions
void free_hash_map(HashMap *hash_map) {
    for (int i = 0; i < HASH_SIZE; i++) {
        pthread_mutex_lock(&hash_map->locks[i]);

        Node *current = hash_map->table[i];
        while (current) {
            Node *temp = current;
            // free the string, the array and the node
            free(temp->word);
            free(temp->file_ids);
            current = current->next;
            free(temp);
        }
        pthread_mutex_unlock(&hash_map->locks[i]);
        pthread_mutex_destroy(&hash_map->locks[i]);
    }
    
    free(hash_map->table);   
    free(hash_map->locks);   
    free(hash_map);
}

void free_file_list(FileList *file_list) {
    if (file_list == NULL) 
        return;
    for (int i = 0; i < file_list->total_files; i++) {
        free(file_list->files[i]);
    }

    free(file_list->files);
    pthread_mutex_destroy(&file_list->lock);
    free(file_list);           
}

int main(int argc, char *argv[]) {
    // Command line arguments
    int num_mappers = atoi(argv[1]);
    int num_reducers = atoi(argv[2]);
    const char *input_file = argv[3];

    // Initialize the file list and hash map
    FileList *file_list = initialize_file_list(input_file);
    HashMap *hash_map = initialize_hash_map();
    if (file_list == NULL || hash_map == NULL)
        return EXIT_FAILURE;

    // Initialize the barrier to synchronize mappers and reducers
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, num_mappers + num_reducers);

    // Initialize letter mutexes for reducers
    pthread_mutex_t letter_mutexes[NUM_LETTERS];
    for (int i = 0; i < NUM_LETTERS; i++) {
        pthread_mutex_init(&letter_mutexes[i], NULL);
    }

    // Create mapper threads
    pthread_t *mappers = malloc(num_mappers * sizeof(pthread_t));
    MapperArgs mapper_args = {file_list, &barrier, hash_map};
    // Create reducer threads
    pthread_t *reducers = malloc(num_reducers * sizeof(pthread_t));
    ReducerArgs *reducer_args = malloc(num_reducers * sizeof(ReducerArgs));
    for (int i = 0 ; i < num_mappers + num_reducers; i++) {
        if (i < num_mappers) {
            pthread_create(&mappers[i], NULL, mapper_function, &mapper_args);
        } else {
            reducer_args[i - num_mappers].hash_map = hash_map;
            reducer_args[i - num_mappers].barrier = &barrier;
            reducer_args[i - num_mappers].reducer_id = i - num_mappers;
            reducer_args[i - num_mappers].num_reducers = num_reducers;
            reducer_args[i - num_mappers].letter_mutexes = letter_mutexes; // Pass the mutex array
            pthread_create(&reducers[i - num_mappers], NULL, reducer_function, &reducer_args[i - num_mappers]);
        }

    }
    for (int i = 0; i < num_mappers + num_reducers; i++) {
        if (i < num_mappers)
            pthread_join(mappers[i], NULL);
        else  
            pthread_join(reducers[i - num_mappers], NULL); 
    }

    // Destroy the barrier
    pthread_barrier_destroy(&barrier);

    // Destroy letter mutexes
    for (int i = 0; i < NUM_LETTERS; i++) {
        pthread_mutex_destroy(&letter_mutexes[i]);
    }

    // Free allocated memory
    free(mappers);
    free(reducers);
    free(reducer_args);
    free_hash_map(hash_map);
    free_file_list(file_list);

    return 0;
}
