#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "log.h"
#include "alloc.h"
#include "dlmalloc.h"

REGISTRY_ENTRY_FILE_GENERATE;

int
main(void)
{

#if defined(dlmalloc)
    FATAL_MSG("dlmalloc is defined when only mspaces should be");
#endif

    mspace m = create_mspace_with_base(NULL, 0, 0);
    NIOVA_ASSERT(m == NULL);

    void *p = niova_malloc(10240UL);
    NIOVA_ASSERT(p != NULL);

    m = create_mspace_with_base(p, 10240, 0);
    NIOVA_ASSERT(m != NULL);

    void *x = mspace_malloc(m, 10240);
    NIOVA_ASSERT(x == NULL);

    x = mspace_malloc(m, 1024);
    NIOVA_ASSERT(x);

    mspace_free(m, x);
    destroy_mspace(m);
    niova_free(p);

    return 0;
}
