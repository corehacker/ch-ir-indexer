/*******************************************************************************
 * Copyright (c) 2014, Sandeep Prakash <sxp121331@utdallas.edu>
 *
 * \file   ch-ir-indexer.c
 *
 * \author sandeepprakash
 *
 * \date   Feb 19, 2014
 *
 * \brief
 *
 * General Information
 * ===================
 * This package contains source code for the application that parses set of files
 * in a given directory and tokenizes them. It will keep track of the number of
 * occurances of each token and prints out statistics like their frequency of
 * occurance, total unique tokens, tokens occuring only once, etc.
 *
 * The source code is POSIX compliant and uses the standard C99 data types.
 *
 * The application is named "ch-ir-indexer".
 *
 * The program uses libraries developed by me which have been made open source
 * earlier in my Masters. The code is found on github here:
 *    https://github.com/corehacker/c-modules
 * The libraries that are being used are ch-pal, a platform abstraction layer and
 * ch-utils, general utilities like lists, queues and hashmap.
 *
 * Building The Sourcecode
 * =======================
 * 1. Unzip the tar ball in the current directory.
 * 2. Navigate to code/indexer. This is where Makefile is located.
 *    % cd code/indexer
 * 3. Issue make command after issuing make clean.
 *    % make -f Makefile_independent clean
 *    % make -f Makefile_independent
 *    After successful execution of the above commands, the executable
 *    "ch-ir-indexer" will be created in the current directory.
 *
 * Execution
 * =========
 * 1. Requirements:
 *    a. Export the LD_LIBRARY_PATH environment variable. A utility script is
 *       provided for ease.
 *       % chmod 755 export.sh
 *       % source export.sh
 *
 * 2. Application Usage:
 *    Usage:
 *    ./ch-ir-indexer <Directory To Parse> [<Hashmap Table Size>]
 *       Directory To Parse - Absolute or relative directory path to parse files.
 *       Hashmap Table Size - Table size of the hashmap. Smaller the table size
 *                            slower is the run time. [Optional]
 *
 ******************************************************************************/

#include <ctype.h>
#include <ch-pal/exp_pal.h>
#include <ch-utils/exp_list.h>
#include <ch-utils/exp_hashmap.h>
#include "porter.h"
#include "ch-ir-common.h"
#include "ch-ir-dir-parser.h"
#include "ch-ir-tokenizer.h"
#include "ch-ir-indexer.h"
#include "ch-ir-indexer-compress.h"

#define DEFAULT_DIRECTORY_TO_PARSE                 "./Cranfield"
#define DEFAULT_STOPWORDS_FILEPATH                 "./stopwords"
#define DEFAULT_MAX_TOKEN_SIZE                     (2048)
#define MAX_FILENAME_LEN                           (16384)
#define DEFAULT_TOP_LEVEL_TOKEN_HASHMAP_TABLE_SIZE (1000)
#define DEFAULT_POSTINGS_HASHMAP_TABLE_SIZE        (100)
#define DEFAULT_UNCOMPRESSED_SERIALIZE_FILEPATH    "./uncompressed_index.bin"
#define DEFAULT_COMPRESSED_SERIALIZE_FILEPATH      "./compressed_index.bin"

static void print_usage(
   int i_argc,
   char **ppc_argv)
{
   printf (
      "\n Usage:"
         "\n \t%s <Directory To Parse> [<Stopwords filepath (Default: %s)>] [<Hashmap Table Size (Default: %d)>]"
         "\n \t\tDirectory To Parse - Absolute or relative directory path to parse files."
         "\n \t\tStopwords filepath - [Optional: Default: %s]."
         "\n \t\tHashmap Table Size - Table size of the hashmap. Smaller the table "
         "size slower is the run time. [Optional: Default: %d]", ppc_argv [0],
      DEFAULT_STOPWORDS_FILEPATH, DEFAULT_TOP_LEVEL_TOKEN_HASHMAP_TABLE_SIZE,
      DEFAULT_STOPWORDS_FILEPATH, DEFAULT_TOP_LEVEL_TOKEN_HASHMAP_TABLE_SIZE);
   printf ("\n");
}

int main(
   int i_argc,
   char **ppc_argv)
{
   int32_t i_ret_val = -1;
   CH_IR_RET_E e_ret_val = eCH_IR_RET_FAILURE;
   CH_IR_INDEXER_INIT_PARAMS_X x_indexer_init_params = {0};
   uint8_t *puc_stopwords_filepath = NULL;
   uint8_t *puc_dir_path = NULL;
   CH_IR_INDEXER_CTXT_X *px_indexer_ctxt = NULL;
   PAL_LOGGER_INIT_PARAMS_X x_init_params = {false};
   PAL_RET_E e_pal_ret = ePAL_RET_FAILURE;
   CH_IR_INDEX_SERIALIZE_OPTIONS_X x_serialize_options = {false};

   uint8_t ucaa_sample_terms [] [250] = { "Reynolds", "NASA", "Prandtl", "flow",
      "pressure", "boundary", "shock" };
   uint32_t ui_sample_token_len = 0;
   uint32_t ui_i = 0;

   if (i_argc < 2 || i_argc > 4)
   {
      print_usage (i_argc, ppc_argv);
      i_ret_val = -1;
      goto CLEAN_RETURN;
   }

   pal_env_init ();

   x_init_params.e_level = eLOG_LEVEL_LOW;
   x_init_params.b_enable_console_logging = true;
   pal_logger_env_init(&x_init_params);

   if (NULL != ppc_argv [2])
   {
      puc_stopwords_filepath = ppc_argv [2];
   }
   else
   {
      puc_stopwords_filepath = DEFAULT_STOPWORDS_FILEPATH;
   }

   if (NULL != ppc_argv[1])
   {
      puc_dir_path = ppc_argv[1];
   }
   else
   {
      puc_dir_path = DEFAULT_DIRECTORY_TO_PARSE;
   }

   if (NULL != ppc_argv [3])
   {
      e_pal_ret = pal_atoi ((uint8_t *) ppc_argv [3],
         (int32_t *) &(x_indexer_init_params.ui_token_hm_table_size));
      if ((ePAL_RET_SUCCESS != e_pal_ret)
         || (0 == x_indexer_init_params.ui_token_hm_table_size))
      {
         x_indexer_init_params.ui_token_hm_table_size =
            DEFAULT_TOP_LEVEL_TOKEN_HASHMAP_TABLE_SIZE;
      }
   }
   else
   {
      x_indexer_init_params.ui_token_hm_table_size =
         DEFAULT_TOP_LEVEL_TOKEN_HASHMAP_TABLE_SIZE;
   }
   x_indexer_init_params.ui_postings_hm_table_size =
      DEFAULT_POSTINGS_HASHMAP_TABLE_SIZE;
   x_indexer_init_params.ui_max_token_len = DEFAULT_MAX_TOKEN_SIZE;
   x_indexer_init_params.ui_max_filepath_len = MAX_FILENAME_LEN;
   x_indexer_init_params.b_ignore_stopwords = true;
   pal_strncpy (x_indexer_init_params.uca_stopwords_filepath,
      puc_stopwords_filepath,
      sizeof(x_indexer_init_params.uca_stopwords_filepath));
   e_ret_val = ch_ir_indexer_init (&x_indexer_init_params, &px_indexer_ctxt);

   e_ret_val = ch_ir_indexer_build_index (px_indexer_ctxt, puc_dir_path);

   //(void) pal_memset(&x_serialize_options, 0x00, sizeof(x_serialize_options));
   x_serialize_options.b_serialize_uncompressed = true;
   x_serialize_options.puc_uncompressed_filepath = DEFAULT_UNCOMPRESSED_SERIALIZE_FILEPATH;
   e_ret_val = ch_ir_indexer_serialize(px_indexer_ctxt, &x_serialize_options);

   e_ret_val = ch_ir_indexer_compress_index(px_indexer_ctxt);

   //(void) pal_memset(&x_serialize_options, 0x00, sizeof(x_serialize_options));
   x_serialize_options.b_serialize_compressed = true;
   x_serialize_options.puc_compressed_filepath = DEFAULT_COMPRESSED_SERIALIZE_FILEPATH;
   e_ret_val = ch_ir_indexer_serialize(px_indexer_ctxt, &x_serialize_options);

   ui_i = 0;
   while (1)
   {
      ui_sample_token_len = pal_strnlen (ucaa_sample_terms [ui_i],
         sizeof(ucaa_sample_terms [ui_i]));
      if (ui_sample_token_len > 0)
      {
         e_ret_val = ch_ir_indexer_print_stats (px_indexer_ctxt,
            ucaa_sample_terms [ui_i]);
         ui_i++;
      }
      else
      {
         break;
      }
   }

   e_ret_val = ch_ir_indexer_print_stats(px_indexer_ctxt, NULL);

   e_ret_val = ch_ir_indexer_deinit (px_indexer_ctxt);

   i_ret_val = 0;
CLEAN_RETURN:
   return i_ret_val;
}
