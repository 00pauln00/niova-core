#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "niova/log.h"
#include "niova/alloc.h"
#include "dlmalloc.h"

REGISTRY_ENTRY_FILE_GENERATE;

static void
fragmentation_check(mspace m)
{
    // Perform a simple fragmentation test
    void *y[8];

    for (int i = 0; i < 8; i++)
    {
        y[i] = mspace_malloc(m, 1000);
        NIOVA_ASSERT(y[i]);

//        SIMPLE_LOG_MSG(LL_WARN, "y[%d]=%p", i, y[i]);
    }

    NIOVA_ASSERT(mspace_malloc(m, 2000) == NULL);

    mspace_free(m, y[0]);
    mspace_free(m, y[6]);
    y[0] = NULL;
    y[6] = NULL;

    NIOVA_ASSERT(mspace_malloc(m, 2000) == NULL);

    mspace_free(m, y[1]);  // Free a slot adjacent to one already freed
    y[1] = NULL;

    void *x = mspace_malloc(m, 2000);
    NIOVA_ASSERT(x);
    mspace_free(m, x);

    for (int i = 0; i < 8; i++)
    {
        if (y[i])
            mspace_free(m, y[i]);
    }
}

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

    void *x = mspace_malloc(m, 10200);
    NIOVA_ASSERT(x == NULL);

    x = mspace_malloc(m, 1024);
    NIOVA_ASSERT(x);

    mspace_free(m, x);

    fragmentation_check(m);

    destroy_mspace(m);
    niova_free(p);

    return 0;
}
