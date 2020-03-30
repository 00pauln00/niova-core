#include <stdio.h>
#include <uuid/uuid.h>
#include <string.h>
#include <stdlib.h>

typedef unsigned long long uint64_t;

static inline void
niova_uuid_2_uint64(const uuid_t uuid_in, uint64_t *high, uint64_t *low)
{
    if (high)
        *high = *(const unsigned long long *)((const char *)&uuid_in[0]);

    if (low)
        *low = *(const unsigned long long *)((const char *)&uuid_in[8]);
}

int
main(void)
{
    uuid_t uuid;
    uuid_generate(uuid);

    char uuid_str[UUID_STR_LEN];
    uuid_unparse(uuid, uuid_str);

    unsigned long long uuid_num = strtoull((const char *)uuid, NULL, 16);
    unsigned long long uuid_num1 =
        *(const unsigned long long *)((const char *)&uuid[0]);

    unsigned long long uuid_num2 =
        *(const unsigned long long *)((const char *)&uuid[8]);

    fprintf(stdout, "%s %llx %llx %llx\n",
            uuid_str, uuid_num, uuid_num1, uuid_num2);

    niova_uuid_2_uint64(uuid, &uuid_num1, &uuid_num2);

    fprintf(stdout, "%s %llx %llx %llx\n",
            uuid_str, uuid_num, uuid_num1, uuid_num2);


    return 0;
}
