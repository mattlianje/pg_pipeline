#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pipeline_hello);

/*
 * Hello world function that takes a name and returns a greeting
 */
Datum
pipeline_hello(PG_FUNCTION_ARGS)
{
    text *name_text;
    text *result;
    char *name_string;
    char *result_string;
    int name_len;
    
    /* Get input text or use default if NULL */
    if (PG_ARGISNULL(0)) {
        name_string = pstrdup("world");
    } else {
        name_text = PG_GETARG_TEXT_PP(0);
        name_string = text_to_cstring(name_text);
    }
    
    name_len = strlen(name_string);
    
    /* Allocate and build result string */
    result_string = palloc(name_len + 14);
    sprintf(result_string, "Hello, %s!", name_string);
    
    /* Convert to text datum */
    result = cstring_to_text(result_string);
    
    /* Free allocated memory */
    if (!PG_ARGISNULL(0)) {
        pfree(name_string);
    }
    pfree(result_string);
    
    PG_RETURN_TEXT_P(result);
}
